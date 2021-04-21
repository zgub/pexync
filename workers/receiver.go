package workers

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"net"
	"net/http"
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
				//spew.Dump(msg)
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
				if fileWritter, ok := w.writersMap[fi]; ok {
					// new message
					fileWritter.inbox <- &core.Message{
						Flag: core.WSQ,
						//FileDesc: msg.List[fi],
						DataDesc: dd,
					}
				} else {
					log.Debug().
						Str("filename", msg.FileDesc.FileName).
						Msg("starting new writter")
					inbox := make(chan *core.Message)
					fr := NewFileWriter(w.ctx, w.senderUUID, msg.FileDesc, inbox)
					w.writersMap[fi] = fr
					// send a new message
					g.Go(func() error { return fr.Start() })
					fr.inbox <- &core.Message{
						Flag: core.WSQ,
						//FileDesc: w.srcList[fi],
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

	dstDir := viper.GetString("destination")

	// store source filelist for future reference
	for i, fd := range msg.FileList {
		w.srcList[fd.Idx] = fd
		if int64(i) != fd.Idx {
			log.Warn().
				Int("slice index", i).
				Int64("file index", fd.Idx).
				Msg("WHOA!!!")
		}
	}

	diffMap, err := compare(msg.FileList, dstDir)
	if err != nil {
		return errors.Wrap(err, "file comparator failed")
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

	// routes
	r.Route("/list", func(r chi.Router) {
		r.Post("/", processList)
	})

	r.Route("/data", func(r chi.Router) {
		r.Post("/", processData)
	})

	address := viper.GetString("bind_address")
	if net.ParseIP(address) == nil {
		return errors.New("invalid bind address: " + address)
	}
	port := strconv.Itoa(viper.GetInt("port"))
	address = address + ":" + port
	err := http.ListenAndServe(address, r)
	if err != nil {
		return errors.Wrapf(err, "unable to listen on %s", address)
	}

	return nil
}

func processList(w http.ResponseWriter, r *http.Request) {

	buf, err := decompress(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Error().
			Err(err).
			Caller().
			Msg("internal server error")
		return
	}

	msg := *&core.Message{}
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
	err = respondWithJSON(w, http.StatusOK, msg)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Error().
			Err(err).
			Caller().
			Msg("internal server error")
		return
	}

}

func processData(w http.ResponseWriter, r *http.Request) {}
