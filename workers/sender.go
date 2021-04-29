package workers

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
	"github.com/zgub/pexync/core"
	"github.com/zgub/pexync/lfs"
	"golang.org/x/sync/errgroup"
)

// LocalSender represents blah balh
type LocalSender struct {
	ctx      context.Context
	srcList  []*lfs.FileDesc
	inbox    <-chan *core.Message
	receiver chan<- *core.Message
	uuid     uuid.UUID
}

func NewLocalSender(ctx context.Context, fl []*lfs.FileDesc, in <-chan *core.Message, receiver chan<- *core.Message) *LocalSender {
	return &LocalSender{
		ctx:      ctx,
		srcList:  fl,
		inbox:    in,
		receiver: receiver,
		uuid:     uuid.New(),
	}
}

func (w *LocalSender) Start() error {

	// calculate block sizes
	for _, fd := range w.srcList {
		if !fd.IsDir {
			fd.SetBlockSize()
			log.Trace().
				Str("file name", fd.FileName).
				Int64("file size", int64(fd.FileSize)).
				Int64("block size calculated", fd.BlockSize).
				Msg("local sender")
		}
	}

	// prepare a message for the receiver
	msg := &core.Message{
		Flag:     core.INI,
		UUID:     w.uuid,
		FileList: w.srcList,
	}

	log.Debug().
		Msgf("local sender - source file list, length: %d", len(w.srcList))

	err := sendWithTimeout(msg, w.receiver)
	if err != nil {
		return errors.Wrap(err, "local sender")
	}

	// receive the filelist with checksums
	msg, err = recvWithTimeout(w.inbox)
	if err != nil {
		return errors.Wrap(err, "local sender")
	}

	// prepare a slice with the delta
	diffList := make([]*lfs.FileDesc, 0)
	missList := make([]*lfs.FileDesc, 0)
	for _, fd := range msg.FileList {
		if fd.State == lfs.Missing && !fd.IsDir {
			// new file
			log.Debug().
				Int64("block size", fd.BlockSize).
				Str("file", fd.Prefix+"/"+fd.FileName).
				Msgf("local sender %s", fd.State.String())
			missList = append(missList, fd)
		} else if fd.State == lfs.Diff {
			// diff file
			log.Debug().
				Int64("block size", fd.BlockSize).
				Int("hashes count", len(fd.Weak)).
				Str("file", fd.Prefix+"/"+fd.FileName).
				Msgf("local sender %s", fd.State.String())
			diffList = append(diffList, fd)

		} else {
			// skipped file
			log.Debug().
				Str("file", fd.Prefix+"/"+fd.FileName).
				Msgf("local sender %s", fd.State.String())
		}
	}

	// prepare for transfer
	rrInbox := make(chan *core.Message)
	brInbox := make(chan *core.Message)
	ccIo := viper.GetInt("io_concurrency")
	g := new(errgroup.Group)
	dCtx := context.Context(w.ctx)

	// spawn readers if we have diff files
	if len(diffList) > 0 {
		log.Debug().
			Msg("local sender - spawning roll readers")

		for i := 0; i < ccIo; i++ {
			rr := NewRollReader(dCtx, rrInbox, w.receiver)
			g.Go(rr.Start)
		}
	}

	// spawn missing file senders if we have missing files
	if len(missList) > 0 {
		log.Debug().
			Msg("local sender - spawning bytes readers")

		for i := 0; i < ccIo; i++ {
			br := NewBytesReader(dCtx, brInbox, w.receiver)
			g.Go(br.Start)
		}
	}

	// send data - diff first
	for _, fd := range diffList {

		rrInbox <- &core.Message{
			FileDesc: fd,
			Flag:     core.RSQ,
			Offset:   0,
			Limit:    int64(fd.FileSize),
		}
	}

	// new files next
	for _, fd := range missList {

		brInbox <- &core.Message{
			FileDesc: fd,
			Flag:     core.RSQ,
			Offset:   0,
			Limit:    int64(fd.FileSize),
		}
	}

	// all data sent, stop zee workerz
	if len(diffList) > 0 {
		for i := 0; i < ccIo; i++ {
			rrInbox <- &core.Message{
				Flag: core.FIN,
			}
		}
	}
	if len(missList) > 0 {
		for i := 0; i < ccIo; i++ {
			brInbox <- &core.Message{
				Flag: core.FIN,
			}
		}
	}
	// validate ???

	// end
	err = g.Wait()
	if err != nil {
		return errors.Wrap(err, "file reader error")
	}
	log.Trace().
		Msg("local sender - finished, sending FIN to receciver")
	msg = &core.Message{
		Flag: core.FIN,
		UUID: w.uuid,
	}
	err = sendWithTimeout(msg, w.receiver)
	if err != nil {
		return errors.Wrap(err, "sender failure")
	}
	return nil
}

type HttpSender struct {
	url      *url.URL
	ctx      context.Context
	uuid     uuid.UUID
	client   *http.Client
	sendChan chan *core.Message
}

func NewHttpSender(ctx context.Context) (*HttpSender, error) {

	// first, prepare http client
	host := viper.GetString("remote_host")
	port := viper.GetInt("port")

	url, err := url.Parse(fmt.Sprintf("http://%s:%d", host, port))
	if err != nil {
		return nil, errors.Wrap(err, "http sender - invalid URL")
	}

	log.Debug().
		Str("remote host", host).
		Int("port", port).
		Msg("destiantion set")

	defaultTimeout := viper.GetDuration("timeout")
	ccIo := viper.GetInt("io_concurrency")

	tr := &http.Transport{
		ResponseHeaderTimeout: defaultTimeout,
		Proxy:                 http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			KeepAlive: 0,
			Timeout:   defaultTimeout,
		}).DialContext,
		MaxIdleConns:          ccIo,
		IdleConnTimeout:       defaultTimeout * 10,
		TLSHandshakeTimeout:   defaultTimeout,
		MaxIdleConnsPerHost:   2 * ccIo,
		ExpectContinueTimeout: defaultTimeout,
		DisableCompression:    false,
	}

	c := &http.Client{
		Timeout:   defaultTimeout,
		Transport: tr,
	}

	s := &HttpSender{
		url:      url,
		ctx:      ctx,
		uuid:     uuid.New(),
		client:   c,
		sendChan: make(chan *core.Message),
	}

	return s, nil
}

func (w *HttpSender) Start() error {

	// perform directory listing
	srcList, err := lfs.ParseDir(viper.GetString("source"))
	if err != nil {
		return errors.Wrap(err, "http sender - directory parsing failed")
	}

	// calculate blocksizes for each file
	for _, fd := range srcList {
		if !fd.IsDir {
			fd.SetBlockSize()
			log.Trace().
				Str("file name", fd.FileName).
				Int64("file size", int64(fd.FileSize)).
				Int64("calculated block size", fd.BlockSize).
				Msg("http sender")
		}
	}

	// create a new message for the other side
	msg := &core.Message{
		Flag:     core.INI,
		UUID:     w.uuid,
		FileList: srcList,
	}

	// send
	url := w.url.String() + "/list"
	msg, err = w.send(url, msg)
	if err != nil {
		errors.Wrap(err, "http sender - send failed")
	}

	// prepare a slice with the delta
	diffList := make([]*lfs.FileDesc, 0)
	missList := make([]*lfs.FileDesc, 0)
	for _, fd := range msg.FileList {
		if fd.State == lfs.Missing && !fd.IsDir {
			// new file
			log.Debug().
				Int64("block size", fd.BlockSize).
				Str("file", fd.Prefix+"/"+fd.FileName).
				Msgf("local sender %s", fd.State.String())
			missList = append(missList, fd)
		} else if fd.State == lfs.Diff {
			// diff file
			log.Debug().
				Int64("block size", fd.BlockSize).
				Int("hashes count", len(fd.Weak)).
				Str("file", fd.Prefix+"/"+fd.FileName).
				Msgf("local sender %s", fd.State.String())
			diffList = append(diffList, fd)

		} else {
			// skipped file
			log.Debug().
				Str("file", fd.Prefix+"/"+fd.FileName).
				Msgf("local sender %s", fd.State.String())
		}
	}

	// prepare for transfer
	rrInbox := make(chan *core.Message)
	brInbox := make(chan *core.Message)
	ccIo := viper.GetInt("io_concurrency")
	g := new(errgroup.Group)
	dCtx := context.Context(w.ctx)

	// starting http senders
	for i := 0; i < 2*ccIo; i++ {
		log.Trace().
			Msg("http sender - starting http client worker")
		g.Go(w.runClient)
	}

	// spawn roll readers if there are diff files
	if len(diffList) > 0 {
		log.Debug().
			Msg("https sender - spawning roll readers")
		for i := 0; i < ccIo; i++ {
			rr := NewRollReader(dCtx, rrInbox, w.sendChan)
			g.Go(rr.Start)
		}
	}

	// spawn missing file senders (readers) if there are new (missing) files
	if len(missList) > 0 {
		log.Debug().
			Msg("http sender - spawning bytes readers")

		for i := 0; i < ccIo; i++ {
			br := NewBytesReader(dCtx, brInbox, w.sendChan)
			g.Go(br.Start)
		}
	}

	// send data - diff first
	for _, fd := range diffList {

		rrInbox <- &core.Message{
			FileDesc: fd,
			Flag:     core.RSQ,
			Offset:   0,
			Limit:    int64(fd.FileSize),
		}
	}

	// new files next
	for _, fd := range missList {

		brInbox <- &core.Message{
			FileDesc: fd,
			Flag:     core.RSQ,
			Offset:   0,
			Limit:    int64(fd.FileSize),
		}
	}

	// all data sent, stop zee workerz
	if len(diffList) > 0 {
		for i := 0; i < ccIo; i++ {
			rrInbox <- &core.Message{
				Flag: core.FIN,
			}
		}
	}
	if len(missList) > 0 {
		for i := 0; i < ccIo; i++ {
			brInbox <- &core.Message{
				Flag: core.FIN,
			}
		}
	}

	// don't forget zee http senderz
	for i := 0; i < 2*ccIo; i++ {
		w.sendChan <- &core.Message{
			Flag: core.FIN,
		}
	}

	return nil
}
