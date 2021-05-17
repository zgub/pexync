package workers

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"

	"github.com/davecgh/go-spew/spew"
	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
	"github.com/zgub/pexync/core"
	"github.com/zgub/pexync/lfs"
	"golang.org/x/sync/errgroup"
)

type senderState int

// if I want to reset the receiver to initial state I need a state :-/
const (
	RST senderState = iota // initialized state
	RCV                    // file list received
	WRT                    // spawned writers
)

type receiver struct {
	ctx         context.Context
	state       senderState // not sure if needed, or if I ever implement this
	senderUUID  uuid.UUID   // same here
	srcList     map[int64]*lfs.FileDesc
	writersMap  map[int64]FileWriter
	fileWriters *errgroup.Group // writters error group
}

// parseSenderList parses a file list from sender and updates it with the information from destiantion
func (rc receiver) parseSenderList(msg *core.Message) error {
	rc.senderUUID = msg.GetUuid()
	log.Trace().
		Str("sender uuid", rc.senderUUID.String()).
		Msgf("receiver list parser - src file list, length: %d", len(msg.GetList()))

	// store source filelist ina a map for future!!!
	for i, fd := range msg.GetList() {
		rc.srcList[fd.Idx] = fd
		if int64(i) != fd.Idx {
			// TODO: remove index, redundant information
			log.Warn().
				Int("slice index", i).
				Int64("file index", fd.Idx).
				Msg("WHOA!!!")
		}
	}

	// now compare sender and remote directories
	diffMap, err := rc.compare()
	if err != nil {
		return errors.Wrap(err, "file comparator failed")
	}

	// starting checksums workers
	hashReaders := new(errgroup.Group)
	ccIo := viper.GetInt("io_concurrency")
	hashChan := make(chan *core.Message)
	for i := 0; i < ccIo; i++ {
		dCtx := context.Context(rc.ctx)
		w := NewHashreader(dCtx, hashChan)
		hashReaders.Go(w.Start)
	}

	// send data to checksum workers
	for dstFd := range diffMap {
		log.Trace().
			Str("state", dstFd.State.String()).
			Str("file name", dstFd.Prefix+"/"+dstFd.FileName).
			Msg("receiver list parser - sending to hash reader")
		hashChan <- core.NewHashRequest(dstFd)
	}
	for i := 0; i < ccIo; i++ {
		hashChan <- core.NewFIN(msg.GetUuid())
	}
	err = hashReaders.Wait()
	if err != nil {
		return errors.Wrap(err, "error caclulation initial check sums")
	}

	// make sure that we copy the block hashes!!
	for dstFd, srcFd := range diffMap {
		srcFd.Weak = dstFd.Weak
		srcFd.Sha1 = dstFd.Sha1
	}

	return nil
}

// compare is the main function comparing sender dir listing with destiantion directory listing
func (rc *receiver) compare() (map[*lfs.FileDesc]*lfs.FileDesc, error) {

	// pull from config
	dstDir := viper.GetString("destination")

	// check if the destination dir exists
	if _, err := os.Stat(dstDir); os.IsNotExist(err) {
		// create one
		os.Mkdir(dstDir, os.ModeDir)
	} else if err != nil {
		return nil, err
	}

	// well if it ODES exist, let's list it
	dstList, err := lfs.ParseDir(dstDir)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to list directory %s", dstDir)
	}

	// build a map of local entries for faster lookup
	dstMap := make(map[string]*lfs.FileDesc, len(dstList))
	for _, dstFd := range dstList {
		dstMap[dstFd.RelPath] = dstFd
	}

	// prepare the result = a map of differences
	diffMap := make(map[*lfs.FileDesc]*lfs.FileDesc)
	for _, srcFd := range rc.srcList {

		p := filepath.Join(dstDir, srcFd.RelPath)
		log.Trace().
			Str("source path", srcFd.RelPath).
			Str("destination path", p).
			Msg("receiver - lookup and compare")

		if dstFd, ok := dstMap[srcFd.RelPath]; ok {
			// it does exist on destination
			if srcFd.FileSize == dstFd.FileSize && srcFd.Modified == dstFd.Modified {
				// same size, same modification date, we're adding SHA1 so if only mod time was changed, we can skip it anyway on source
				// check permission, modtime and ownership and update if needed
				err = fixMeta(dstDir, srcFd, dstFd)
				if err != nil {
					return nil, errors.Wrap(err, "unable to fix metadata")
				}
				srcFd.State = lfs.Skip
				log.Debug().
					Str("path", p).
					Msg("receiver comparing -  updating metadata")
			} else {
				log.Debug().
					Str("sender path", srcFd.RelPath).
					Int64("source file size", srcFd.FileSize).
					Int64("destination file size", dstFd.FileSize).
					Time("source file modified", srcFd.Modified.UTC()).
					Time("receiver file modified", dstFd.Modified.UTC()).
					Msg("receiver DIFF")

				// sync the states in both structs
				srcFd.State = lfs.Diff
				dstFd.State = lfs.Diff
				// important for block checksum calculation
				dstFd.BlockSize = srcFd.BlockSize
				// remote index is not important, this is required for file writer
				dstFd.Idx = srcFd.Idx
				// map both here for block checksum calculation later
				diffMap[dstFd] = srcFd
				// store!
				rc.srcList[srcFd.Idx] = srcFd

				if !srcFd.IsDir {
					// a file that exists and is not dir

					// treat "remote" files smaller than block sizes as missing
					if srcFd.BlockSize >= dstFd.FileSize {
						srcFd.State = lfs.Missing
						continue
					}
					// check for zero sized files
					if srcFd.FileSize == 0 {
						log.Trace().
							Str("path", p).
							Msg("receiver comparing - empty file")

						// file creted, modify meta if required and set as done
						err = fixMeta(dstDir, srcFd, dstFd)
						if err != nil {
							return nil, errors.Wrap(err, "error changing metadata")
						}
						srcFd.State = lfs.Skip
					}
					// determine what has changed, if permission and/or modtime only, do not set it to diff
					if srcFd.FileSize == dstFd.FileSize {
						// possibly the same file by contents
						srcFd.State = lfs.Meta
						dstFd.State = lfs.Meta
						err = fixMeta(dstDir, srcFd, dstFd)
						if err != nil {
							return nil, errors.Wrap(err, "error changing metadata")
						}
					}
				} else {
					log.Trace().
						Str("path", p).
						Msg("receiver comparing - fixing dir meta")
					// directory that exists, check meta only
					err = fixMeta(dstDir, srcFd, dstFd)
					if err != nil {
						return nil, errors.Wrap(err, "error changing metadata")
					}
					srcFd.State = lfs.Skip
				}
			}
			continue
		} else {
			// it does not exist on destination, check if it's a directory
			if srcFd.IsDir {
				// create directory
				log.Debug().
					Str("path", p).
					Msg("receiver comparing - creating directory")
				if _, err := os.Stat(p); os.IsNotExist(err) {
					// create one
					os.Mkdir(p, os.ModeDir)
				} else if err != nil {
					return nil, errors.Wrapf(err, "%s - unable to create directory", p)
				}
			} else {
				// set it as missing
				// check for zero sized files
				if srcFd.FileSize == 0 {
					log.Trace().
						Str("path", p).
						Msg("receiver comparing - empty file")
					file, err := os.Create(p)
					if err != nil {
						return nil, errors.Wrapf(err, "%s - unable to create file", p)
					}
					file.Close()

					// TODO fix metadata on new empty file
					srcFd.State = lfs.Skip
					continue
				}
				srcFd.State = lfs.Missing
				// store!
				rc.srcList[srcFd.Idx] = srcFd
				log.Debug().
					Str("sender path", srcFd.RelPath).
					Int64("source file size", srcFd.FileSize).
					Time("source file modified", srcFd.Modified.UTC()).
					Msg("receiver MISS")
			}
		}
	}
	return diffMap, nil
}

func (rc *receiver) processList(w http.ResponseWriter, r *http.Request) {

	buf, err := decompress(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Error().
			Err(err).
			Caller().
			Msg("internal server error")
		return
	}

	spew.Dump(buf)

	msg := &core.Message{}
	err = json.NewDecoder(buf).
		Decode(&msg)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Error().
			Err(err).
			Caller().
			Msg("internal server error")
		return
	}

	spew.Dump(msg)

	switch msg.GetFlag() {
	case core.INI:
		// update msg with local state(s)
		err := rc.parseSenderList(msg)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			log.Error().
				Err(err).
				Caller().
				Msg("internal server error")
			return
		}
		msg.SetFlag(core.SUM)
		err = respondWithJSON(w, http.StatusOK, msg)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			log.Error().
				Err(err).
				Caller().
				Msg("internal server error")
			return
		}
	default:
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Fatal().
			Err(err).
			Caller().
			Msg("internal server error - unknown message")
	}
}

func (rc receiver) processRemoteData(w http.ResponseWriter, r *http.Request) {
	buf, err := decompress(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Error().
			Err(err).
			Caller().
			Msg("internal server error - decompression failed")
		return
	}

	log.Info().
		Msgf("http  receiver - received %d bytes of data", buf.Len())

	// write
	dd, err := lfs.Deserialize(buf.Bytes())

	fi := dd.FileIndex()

	if fileWriter, ok := rc.writersMap[fi]; ok {
		fileWriter.inbox <- core.NewWSQ(dd)
	} else {
		// new file, new writer
		log.Debug().
			Str("filename", rc.srcList[dd.FileIndex()].FileName).
			Msg("receiver - starting new writter")
		inbox := make(chan *core.Message)
		// create new file writer worker
		fr := NewFileWriter(rc.ctx, rc.senderUUID, dd.GetStreamCount(), rc.srcList[dd.FileIndex()], inbox)
		// add it to the lookup map
		rc.writersMap[fi] = fr
		// send a new message
		rc.fileWriters.Go(fr.Start)
		fr.inbox <- core.NewWSQ(dd)
	}

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Error().
			Err(err).
			Caller().
			Msg("internal server error - write data failed")
		return
	}

	resp := core.NewACK()

	err = respondWithJSON(w, http.StatusOK, resp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Error().
			Err(err).
			Caller().
			Msg("internal server error")
		return
	}
}

// LocalSender represents blah balh
type LocalReceiver struct {
	inbox  <-chan *core.Message
	sender chan<- *core.Message
	receiver
}

func NewLocalReceiver(ctx context.Context, in <-chan *core.Message, sender chan<- *core.Message) *LocalReceiver {
	return &LocalReceiver{
		receiver: receiver{
			ctx:         ctx,
			state:       RST,
			srcList:     make(map[int64]*lfs.FileDesc),
			writersMap:  make(map[int64]FileWriter),
			fileWriters: new(errgroup.Group),
		},
		inbox:  in,
		sender: sender,
	}
}

func (w *LocalReceiver) Start() error {

	finSent := false

	writtersDone := func() bool {
		if !finSent {
			return false
		}
		for _, fw := range w.writersMap {
			if !fw.IsAlive() {
				return false
			}
		}
		return true
	}

	for !writtersDone() {
		select {
		case <-w.ctx.Done():
			log.Debug().
				Msg("receiver - closing, context done")
			// send fin to all readers
			return errors.New("context done")
		case msg := <-w.inbox:
			// received a message that is not a FIN
			switch msg.GetFlag() {
			// initialization
			case core.INI:
				// update msg with local directory state(s)
				//spew.Dump(msg)
				err := w.parseSenderList(msg)
				w.senderUUID = msg.GetUuid()
				if err != nil {
					return errors.Wrap(err, "failed during sync init")
				}
				msg.SetFlag(core.SUM)
				//spew.Dump(msg)

				err = sendWithTimeout(msg, w.sender)
				if err != nil {
					return errors.Wrap(err, "failed to respond to sender")
				}
			case core.WSQ:
				/********************************************************************************************************
				 * ignore this, local was developed to test the concept, that's why the serialize / deserialize detour  *
				 ********************************************************************************************************/
				data, err := msg.GetDataDesc().Serialize()
				if err != nil {
					return errors.Wrap(err, "error serializing data")
				}
				dd, err := lfs.Deserialize(data)
				if err != nil {
					return errors.Wrap(err, "error serializing data")
				}
				//spew.Dump(msg)
				//fmt.Println("receiver - WSQ")
				//spew.Dump(dd)
				/***********************
				 * end of detour       *
				 ***********************/
				log.Trace().
					Str("filename", w.srcList[dd.FileIndex()].FileName).
					Int64("data sequence", dd.Seq()).
					Msg("receiver - data received")
				fi := dd.FileIndex()
				if fileWritter, ok := w.writersMap[fi]; ok {
					// new message, already existing writer
					fileWritter.inbox <- core.NewWSQ(dd)
				} else {
					// new file, new writer
					log.Debug().
						Str("filename", w.srcList[dd.FileIndex()].FileName).
						Msg("receiver - starting new writter")
					streams := dd.GetStreamCount()

					// sanity check
					if streams == 0 {
						panic("zero stream count")
					}

					inbox := make(chan *core.Message, streams)
					// create new file writer worker
					fr := NewFileWriter(w.ctx, w.senderUUID, streams, w.srcList[dd.FileIndex()], inbox)
					// add it to the lookup map
					w.writersMap[fi] = fr
					// send a new message
					w.fileWriters.Go(fr.Start)
					fmt.Println("sending first packet")
					//spew.Dump(dd)
					fr.inbox <- core.NewWSQ(dd)
				}
			case core.FIN:
				log.Debug().
					Msg("receiver received FIN")
				finSent = true
				//break Loop
			default:
				//spew.Dump(msg)
				return errors.New("unknown message received")
			}
		}
	}
	/*
		for _, wr := range w.writersMap {
			wr.inbox <- &core.Message{
				Flag: core.FIN,
			}
		}
	*/

	fmt.Println("waiting for writers to return")

	err := w.fileWriters.Wait()
	if err != nil {
		return errors.Wrap(err, "error writing files")
	}
	log.Debug().
		Msg("writers done")

	return nil
}

type HttpReceiver struct {
	receiver
}

func NewHttpReceiver(ctx context.Context) *HttpReceiver {
	return &HttpReceiver{
		receiver: receiver{
			ctx:         ctx,
			state:       RST,
			srcList:     make(map[int64]*lfs.FileDesc),
			writersMap:  make(map[int64]FileWriter),
			fileWriters: new(errgroup.Group),
		},
	}
}

func (w *HttpReceiver) Start() error {
	r := chi.NewRouter()
	timeoutValue := viper.GetDuration("timeout")

	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Compress(gzip.DefaultCompression))
	r.Use(middleware.Timeout(timeoutValue))

	// routes
	r.Route("/list", func(r chi.Router) {
		r.Post("/", w.processList)
	})

	r.Route("/data", func(r chi.Router) {
		r.Post("/", w.processRemoteData)
	})

	address := viper.GetString("bind_address")
	if net.ParseIP(address) == nil {
		return errors.New("invalid bind address: " + address)
	}
	port := strconv.Itoa(viper.GetInt("port"))
	address = address + ":" + port
	dstDir := viper.GetString("destination")
	log.Info().
		Str("destination directory", dstDir).
		Str("listening", address).
		Msg("RDY")
	err := http.ListenAndServe(address, r)
	if err != nil {
		return errors.Wrapf(err, "unable to listen on %s", address)
	}

	err = w.fileWriters.Wait()
	return err
}
