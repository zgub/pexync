package workers

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"strconv"

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

// LocalSender represents blah balh
type LocalReceiver struct {
	ctx        context.Context
	inbox      <-chan *core.Message
	sender     chan<- *core.Message
	state      senderState // not sure if needed, or if I ever implement this
	senderUUID uuid.UUID   // same here
	srcList    map[int64]*lfs.FileDesc
	writersMap map[int64]FileWriter
}

func NewLocalReceiver(ctx context.Context, in <-chan *core.Message, sender chan<- *core.Message) *LocalReceiver {
	return &LocalReceiver{
		ctx:        ctx,
		inbox:      in,
		sender:     sender,
		state:      RST,
		srcList:    make(map[int64]*lfs.FileDesc),
		writersMap: make(map[int64]FileWriter),
	}
}

func (w *LocalReceiver) Start() error {

	g := new(errgroup.Group)
	for {
		select {
		case <-w.ctx.Done():
			log.Debug().Msg("local receiver closing, context done")
			// send fin to all readers
			for _, wr := range w.writersMap {
				wr.inbox <- &core.Message{
					Flag: core.FIN,
				}
			}
		case msg := <-w.inbox:
			switch msg.Flag {
			case core.INI:
				err := w.handleIni(msg)
				if err != nil {
					return errors.Wrap(err, "failed during sync init")
				}
			case core.FIN:
				log.Trace().
					Msg("receiver received FIN")
				// send fin to all readers
				for _, writer := range w.writersMap {
					writer.inbox <- &core.Message{
						Flag: core.FIN,
					}
				}
				err := g.Wait()
				return err
			case core.WSQ:
				log.Trace().
					Str("filename", msg.FileDesc.FileName).
					Msg("receiver - data received")
				data, err := msg.DataDesc.Serialize()
				if err != nil {
					return errors.Wrap(err, "error serializing data")
				}

				// spawn filewriters

				// wait for the transfer to finish

				// validate ???

				// end
				// dd is new DataDesc created from serialized msg.DataDesc
				dd, err := lfs.Deserialize(data)
				if err != nil {
					return errors.Wrap(err, "error deserializing data")
				}
				fi := dd.FileIndex()
				if fr, ok := w.writersMap[fi]; ok {
					// new message
					fr.inbox <- &core.Message{
						Flag:     core.WSQ,
						FileDesc: w.srcList[fi],
						DataDesc: dd,
					}
				} else {
					log.Debug().
						Str("filename", w.srcList[fi].FileName).
						Msg("starting new writter")
					inbox := make(chan *core.Message)
					fr := NewFileWriter(w.ctx, w.senderUUID, w.srcList[fi], inbox)
					w.writersMap[fi] = fr
					// send a new message
					g.Go(func() error { return fr.Start() })
					fr.inbox <- &core.Message{
						Flag:     core.WSQ,
						FileDesc: w.srcList[fi],
						DataDesc: dd,
					}
				}

			default:
				return errors.New("unknown message received")
			}
		}
	}
}

func (w *LocalReceiver) handleIni(msg *core.Message) error {
	w.senderUUID = msg.UUID
	log.Debug().
		Str("sender uuid", w.senderUUID.String()).
		Msgf("receiver handling src file list, length: %d", len(msg.FileList))
	// stop all writers if any, this is a reset!

	// get local (destination file list)
	dstDir := viper.GetString("local_destination")

	// check if the destination dir exists
	if _, err := os.Stat(dstDir); os.IsNotExist(err) {
		// create one
		os.Mkdir(dstDir, os.ModeDir)
	} else if err != nil {
		return err
	}

	dstList, err := lfs.ParseDir(dstDir)
	if err != nil {
		return errors.Wrap(err, "unable to list directory")
	}

	// build a map of local entries for faster lookup
	dstMap := make(map[string]*lfs.FileDesc, len(dstList))
	for _, dstFd := range dstList {
		dstMap[dstFd.RelPath] = dstFd
	}
	diffMap := make(map[*lfs.FileDesc]*lfs.FileDesc)
	for _, srcFd := range msg.FileList {
		path := dstDir + srcFd.RelPath
		if dstFd, ok := dstMap[srcFd.RelPath]; ok {
			// it does exist on destination
			if srcFd.FileSize == dstFd.FileSize && srcFd.Modified == dstFd.Modified {
				// check permission, modtime and ownership and aupdate if needed
				err = fixMeta(dstDir, srcFd, dstFd)
				if err != nil {
					return nil
				}
				srcFd.State = lfs.Skip
				log.Debug().
					Str("path", path).
					Msg("receiver updating metadata")
			} else {
				log.Debug().
					Str("sender path", srcFd.RelPath).
					Uint64("source file size", srcFd.FileSize).
					Uint64("destination file size", dstFd.FileSize).
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
				w.srcList[srcFd.Idx] = srcFd

				// determine what has changed, if permission and/or modtime only, do not set it to diff

				if !srcFd.IsDir {
					// treat "remote" files smaller than block sizes as missing
					if uint64(srcFd.BlockSize) > dstFd.FileSize {
						srcFd.State = lfs.Missing
						continue
					}
					// check for zero sized files
					if srcFd.FileSize == 0 {
						log.Trace().
							Str("path", path).
							Msg("empty file")

						// file creted, modify meta if required and set as done
						err = fixMeta(dstDir, srcFd, dstFd)
						if err != nil {
							return nil
						}
						srcFd.State = lfs.Skip
					}
				} else {
					log.Trace().
						Str("path", path).
						Msg("receiver fixing dir meta")
					// directory that exists, check meta only
					err = fixMeta(dstDir, srcFd, dstFd)
					if err != nil {
						return nil
					}
					srcFd.State = lfs.Skip
				}
			}
			continue
		} else {
			// it does not exist on destination, check if it's a ditrectory
			if srcFd.IsDir {
				// create directory
				log.Debug().
					Str("path", path).
					Msg("creating directory")
				if _, err := os.Stat(path); os.IsNotExist(err) {
					// create one
					os.Mkdir(path, os.ModeDir)
				} else if err != nil {
					return errors.Wrapf(err, "%s - unable to create directory", path)
				}
			} else {
				// set it as missing
				// check for zero sized files
				if srcFd.FileSize == 0 {
					log.Trace().
						Str("path", path).
						Msg("empty file")
					file, err := os.Create(path)
					if err != nil {
						return errors.Wrapf(err, "%s - unable to create file", path)
					}
					file.Close()

					// TODO fix metadata on new empty file
					srcFd.State = lfs.Skip
					continue
				}
				srcFd.State = lfs.Missing
				// store!
				w.srcList[srcFd.Idx] = srcFd
				log.Debug().
					Str("sender path", srcFd.RelPath).
					Uint64("source file size", srcFd.FileSize).
					Time("source file modified", srcFd.Modified.UTC()).
					Msg("receiver MISS")
			}
		}
	}

	// starting checksums workers
	g := new(errgroup.Group)
	ccIo := viper.GetInt("io_concurrency")
	hashChan := make(chan *core.Message)
	for i := 0; i < ccIo; i++ {
		dCtx := context.Context(w.ctx)
		w := NewHashreader(dCtx, hashChan)
		g.Go(func() error { return w.Start() })
	}

	// send data to checksum workers

	for dstFd := range diffMap {
		log.Trace().
			Str("state", dstFd.State.String()).
			Str("file name", dstFd.Prefix+"/"+dstFd.FileName).
			Msg("sending to hash reader")
		hashChan <- &core.Message{
			Flag:     core.HSH,
			FileDesc: dstFd,
		}
	}
	for i := 0; i < ccIo; i++ {
		hashChan <- &core.Message{
			Flag: core.FIN,
		}
	}
	err = g.Wait()
	if err != nil {
		return errors.Wrap(err, "error caclulation initial check sums")
	}

	// make sure that we copy the block hashes!!
	for dstFd, srcFd := range diffMap {
		srcFd.Weak = dstFd.Weak
	}

	msg.Flag = core.SUM
	err = sendWithTimeout(msg, w.sender)
	if err != nil {
		return err
	}
	return nil
}

type HttpReceiver struct {
	ctx        context.Context
	inbox      <-chan *core.Message
	sender     chan<- *core.Message
	state      senderState // not sure if needed, or if I ever implement this
	senderUUID uuid.UUID   // same here
	srcList    map[int64]*lfs.FileDesc
	writersMap map[int64]FileWriter
}

func NewHttpReceiver(ctx context.Context, in <-chan *core.Message, sender chan<- *core.Message) *HttpReceiver {
	return &HttpReceiver{
		ctx:        ctx,
		inbox:      in,
		sender:     sender,
		state:      RST,
		srcList:    make(map[int64]*lfs.FileDesc),
		writersMap: make(map[int64]FileWriter),
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

	// RESTy routes for "articles" resource
	r.Route("/list", func(r chi.Router) {

		r.Post("/", processList)
	})

	address := viper.GetString("bind_address")
	if net.ParseIP(address) == nil {
		return errors.New("invalid bind address: " + address)
	}
	port := strconv.Itoa(viper.GetInt("port"))
	address = address + ":" + port
	err := http.ListenAndServe(address, r)
	if err != nil {
		return errors.Wrapf(err, "listen failed on %s", address)
	}

	return nil
}

func processList(w http.ResponseWriter, r *http.Request) {
	var (
		list []*lfs.FileDesc
	)

	buf, err := decompress(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Error().
			Err(err).
			Caller().
			Msg("internal server error")
		return
	}

	err = json.NewDecoder(buf).
		Decode(&list)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Error().
			Err(err).
			Caller().
			Msg("internal server error")
		return
	}
	err = respondWithJSON(w, http.StatusOK, list)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Error().
			Err(err).
			Caller().
			Msg("internal server error")
		return
	}
}
