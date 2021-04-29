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

type sender struct {
	ctx                context.Context
	srcList            []*lfs.FileDesc
	uuid               uuid.UUID
	diffList, missList []*lfs.FileDesc
}

func (s *sender) getSrcList() error {
	// perform directory listing
	list, err := lfs.ParseDir(viper.GetString("source"))
	if err != nil {
		return errors.Wrap(err, "http sender - directory parsing failed")
	}

	// calculate blocksizes for each file
	for _, fd := range list {
		if !fd.IsDir {
			fd.SetBlockSize()
			log.Trace().
				Str("file name", fd.FileName).
				Int64("file size", int64(fd.FileSize)).
				Int64("calculated block size", fd.BlockSize).
				Msg("http sender")
		}
	}

	s.srcList = list

	return nil
}

func (s *sender) parseRemoteList(msg *core.Message) {
	// prepare a slice with the delta
	diff := make([]*lfs.FileDesc, 0)
	miss := make([]*lfs.FileDesc, 0)
	for _, fd := range msg.FileList {
		if fd.State == lfs.Missing && !fd.IsDir {
			// new file
			log.Debug().
				Int64("block size", fd.BlockSize).
				Str("file", fd.Prefix+"/"+fd.FileName).
				Msgf("local sender %s", fd.State.String())
			miss = append(miss, fd)
		} else if fd.State == lfs.Diff {
			// diff file
			log.Debug().
				Int64("block size", fd.BlockSize).
				Int("hashes count", len(fd.Weak)).
				Str("file", fd.Prefix+"/"+fd.FileName).
				Msgf("local sender %s", fd.State.String())
			diff = append(diff, fd)

		} else {
			// skipped file
			log.Debug().
				Str("file", fd.Prefix+"/"+fd.FileName).
				Msgf("local sender %s", fd.State.String())
		}
	}
	s.diffList = diff
	s.missList = miss
}

func (s *sender) spawnreaders() {

}

// LocalSender represents blah balh
type LocalSender struct {
	inbox    <-chan *core.Message
	receiver chan<- *core.Message
	sender
}

func NewLocalSender(ctx context.Context, fl []*lfs.FileDesc, in <-chan *core.Message, receiver chan<- *core.Message) *LocalSender {
	return &LocalSender{
		sender: sender{
			ctx:     ctx,
			srcList: fl,
			uuid:    uuid.New(),
		},
		inbox:    in,
		receiver: receiver,
	}
}

func (w *LocalSender) Start() error {

	if err := w.getSrcList(); err != nil {
		return err
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

	w.parseRemoteList(msg)

	// prepare for transfer
	rrInbox := make(chan *core.Message)
	brInbox := make(chan *core.Message)
	ccIo := viper.GetInt("io_concurrency")
	g := new(errgroup.Group)
	dCtx := context.Context(w.ctx)

	// spawn readers if we have diff files
	if len(w.diffList) > 0 {
		log.Debug().
			Msg("local sender - spawning roll readers")

		for i := 0; i < ccIo; i++ {
			rr := NewRollReader(dCtx, rrInbox, w.receiver)
			g.Go(rr.Start)
		}
	}

	// spawn missing file senders if we have missing files
	if len(w.missList) > 0 {
		log.Debug().
			Msg("local sender - spawning bytes readers")

		for i := 0; i < ccIo; i++ {
			br := NewBytesReader(dCtx, brInbox, w.receiver)
			g.Go(br.Start)
		}
	}

	// send data - diff first
	for _, fd := range w.diffList {

		rrInbox <- &core.Message{
			FileDesc: fd,
			Flag:     core.RSQ,
			Offset:   0,
			Limit:    int64(fd.FileSize),
		}
	}

	// new files next
	for _, fd := range w.missList {

		brInbox <- &core.Message{
			FileDesc: fd,
			Flag:     core.RSQ,
			Offset:   0,
			Limit:    int64(fd.FileSize),
		}
	}

	// all data sent, stop zee workerz
	if len(w.diffList) > 0 {
		for i := 0; i < ccIo; i++ {
			rrInbox <- &core.Message{
				Flag: core.FIN,
			}
		}
	}
	if len(w.missList) > 0 {
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
	client   *http.Client
	sendChan chan *core.Message
	sender
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
		client:   c,
		sendChan: make(chan *core.Message),
		sender: sender{
			ctx:  ctx,
			uuid: uuid.New(),
		},
	}

	return s, nil
}

func (w *HttpSender) Start() error {

	if err := w.getSrcList(); err != nil {
		return err
	}

	// create a new message for the other side
	msg := &core.Message{
		Flag:     core.INI,
		UUID:     w.uuid,
		FileList: w.srcList,
	}

	// send
	url := w.url.String() + "/list"
	msg, err := w.send(url, msg)
	if err != nil {
		errors.Wrap(err, "http sender - send failed")
	}

	w.parseRemoteList(msg)

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
	if len(w.diffList) > 0 {
		log.Debug().
			Msg("https sender - spawning roll readers")
		for i := 0; i < ccIo; i++ {
			rr := NewRollReader(dCtx, rrInbox, w.sendChan)
			g.Go(rr.Start)
		}
	}

	// spawn missing file senders (readers) if there are new (missing) files
	if len(w.missList) > 0 {
		log.Debug().
			Msg("http sender - spawning bytes readers")

		for i := 0; i < ccIo; i++ {
			br := NewBytesReader(dCtx, brInbox, w.sendChan)
			g.Go(br.Start)
		}
	}

	// send data - diff first
	for _, fd := range w.diffList {

		rrInbox <- &core.Message{
			FileDesc: fd,
			Flag:     core.RSQ,
			Offset:   0,
			Limit:    int64(fd.FileSize),
		}
	}

	// new files next
	for _, fd := range w.missList {

		brInbox <- &core.Message{
			FileDesc: fd,
			Flag:     core.RSQ,
			Offset:   0,
			Limit:    int64(fd.FileSize),
		}
	}

	// all data sent, stop zee workerz
	if len(w.diffList) > 0 {
		for i := 0; i < ccIo; i++ {
			rrInbox <- &core.Message{
				Flag: core.FIN,
			}
		}
	}
	if len(w.missList) > 0 {
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
