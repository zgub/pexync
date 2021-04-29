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
	ctx                  context.Context
	srcList              []*lfs.FileDesc
	uuid                 uuid.UUID
	diffList, missList   []*lfs.FileDesc
	g                    *errgroup.Group
	rrCh, brCh, receiver chan *core.Message
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

func (s *sender) spawnReaders() {
	ccIo := viper.GetInt("io_concurrency")
	dCtx := context.Context(s.ctx)

	// spawn readers if we have diff files
	if len(s.diffList) > 0 {
		log.Debug().
			Msg("local sender - spawning roll readers")

		for i := 0; i < ccIo; i++ {
			rr := NewRollReader(dCtx, s.rrCh, s.receiver)
			s.g.Go(rr.Start)
		}
	}

	// spawn missing file senders if we have missing files
	if len(s.missList) > 0 {
		log.Debug().
			Msg("local sender - spawning bytes readers")

		for i := 0; i < ccIo; i++ {
			br := NewBytesReader(dCtx, s.brCh, s.receiver)
			s.g.Go(br.Start)
		}
	}
}

// LocalSender represents blah balh
type LocalSender struct {
	inbox <-chan *core.Message
	sender
}

func NewLocalSender(ctx context.Context, fl []*lfs.FileDesc, in <-chan *core.Message, receiver chan *core.Message) *LocalSender {
	return &LocalSender{
		sender: sender{
			ctx:      ctx,
			srcList:  fl,
			uuid:     uuid.New(),
			receiver: receiver,
		},
		inbox: in,
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
	w.rrCh = make(chan *core.Message)
	w.brCh = make(chan *core.Message)
	ccIo := viper.GetInt("io_concurrency")
	w.g = new(errgroup.Group)

	w.spawnReaders()

	// send data - diff first
	for _, fd := range w.diffList {

		w.rrCh <- &core.Message{
			FileDesc: fd,
			Flag:     core.RSQ,
			Offset:   0,
			Limit:    int64(fd.FileSize),
		}
	}

	// new files next
	for _, fd := range w.missList {

		w.brCh <- &core.Message{
			FileDesc: fd,
			Flag:     core.RSQ,
			Offset:   0,
			Limit:    int64(fd.FileSize),
		}
	}

	// all data sent, stop zee workerz
	if len(w.diffList) > 0 {
		for i := 0; i < ccIo; i++ {
			w.rrCh <- &core.Message{
				Flag: core.FIN,
			}
		}
	}
	if len(w.missList) > 0 {
		for i := 0; i < ccIo; i++ {
			w.brCh <- &core.Message{
				Flag: core.FIN,
			}
		}
	}
	// validate ???

	// end
	err = w.g.Wait()
	if err != nil {
		return errors.Wrap(err, "local sender worker failed")
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
	url    *url.URL
	client *http.Client
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
		url:    url,
		client: c,
		sender: sender{
			ctx:      ctx,
			uuid:     uuid.New(),
			receiver: make(chan *core.Message),
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
	w.rrCh = make(chan *core.Message)
	w.brCh = make(chan *core.Message)
	ccIo := viper.GetInt("io_concurrency")
	w.g = new(errgroup.Group)

	// starting http senders
	for i := 0; i < 2*ccIo; i++ {
		log.Trace().
			Msg("http sender - starting http client worker")
		w.g.Go(w.runClient)
	}

	w.spawnReaders()

	// send data - diff first
	for _, fd := range w.diffList {

		w.rrCh <- &core.Message{
			FileDesc: fd,
			Flag:     core.RSQ,
			Offset:   0,
			Limit:    int64(fd.FileSize),
		}
	}

	// new files next
	for _, fd := range w.missList {

		w.brCh <- &core.Message{
			FileDesc: fd,
			Flag:     core.RSQ,
			Offset:   0,
			Limit:    int64(fd.FileSize),
		}
	}

	// all data sent, stop zee workerz
	if len(w.diffList) > 0 {
		for i := 0; i < ccIo; i++ {
			w.rrCh <- &core.Message{
				Flag: core.FIN,
			}
		}
	}
	if len(w.missList) > 0 {
		for i := 0; i < ccIo; i++ {
			w.brCh <- &core.Message{
				Flag: core.FIN,
			}
		}
	}

	// don't forget zee http senderz
	for i := 0; i < 2*ccIo; i++ {
		w.receiver <- &core.Message{
			Flag: core.FIN,
		}
	}

	// end
	err = w.g.Wait()
	if err != nil {
		return errors.Wrap(err, "http sender worker failed")
	}
	// do not send FIN to remote workers vi http
	log.Trace().
		Msg("http sender - finished")

	return nil
}
