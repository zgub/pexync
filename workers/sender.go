package workers

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha1"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
	"github.com/zgub/pexync/core"
	"github.com/zgub/pexync/lfs"
	"golang.org/x/sync/errgroup"
)

const splitSize = int64(536870912)

type sender struct {
	ctx                  context.Context
	srcList              []*lfs.FileDesc
	uuid                 uuid.UUID
	diffList, missList   []*lfs.FileDesc
	g                    *errgroup.Group
	rrCh, brCh, receiver chan *core.Message
	ccIo                 int // this gets used so may times, it deserves an instance var
}

func (s *sender) getSrcList() error {
	// perform directory listing
	list, err := lfs.ParseDir(viper.GetString("source"))
	if err != nil {
		return errors.Wrap(err, "sender - directory parsing failed")
	}

	// calculate blocksizes for each file
	for _, fd := range list {
		if !fd.IsDir {
			fd.SetBlockSize()
			log.Trace().
				Str("file name", fd.FileName).
				Int64("file size", int64(fd.FileSize)).
				Int64("calculated block size", fd.BlockSize).
				Msg("sender")
		}
	}

	s.srcList = list

	return nil
}

func (s *sender) parseRemoteList(msg *core.Message) error {
	// prepare a slice with the delta
	diff := make([]*lfs.FileDesc, 0)
	miss := make([]*lfs.FileDesc, 0)
	for _, fd := range msg.GetList() {
		if fd.State == lfs.Missing && !fd.IsDir {
			// new file
			log.Debug().
				Int64("block size", fd.BlockSize).
				Str("file", filepath.FromSlash(fd.Prefix+"/"+fd.FileName)).
				Msgf("sender %s", fd.State.String())
			miss = append(miss, fd)
		} else if fd.State == lfs.Diff || fd.State == lfs.Meta {
			// diff file
			log.Debug().
				Int64("block size", fd.BlockSize).
				Int("hashes count", len(fd.Weak)).
				Str("file", filepath.FromSlash(fd.Prefix+"/"+fd.FileName)).
				Msgf("sender %s", fd.State.String())
			if fd.State == lfs.Meta {
				sha1sh := sha1.New()
				p := filepath.Join(fd.Prefix, fd.FileName)
				mf, err := os.Open(p)
				if err != nil {
					return errors.Wrapf(err, "unable to read file: %s", filepath.FromSlash(fd.Prefix+"/"+fd.FileName))
				}
				r := io.Reader(mf)
				br := bufio.NewReader(r)
				_, err = io.Copy(sha1sh, br)
				if err != nil {
					return errors.Wrapf(err, "unable to read file: %s", filepath.FromSlash(fd.Prefix+"/"+fd.FileName))
				}
				rSha1 := fd.Sha1
				lSha1 := sha1sh.Sum(nil)[:20]
				if bytes.Equal(rSha1, lSha1) {
					log.Trace().
						Msgf("sender - file: %s has matching SHA1 digest, skipping", filepath.FromSlash(fd.Prefix+"/"+fd.FileName))
					continue
				}
			}
			diff = append(diff, fd)

		} else {
			// skipped file
			log.Debug().
				Str("file", filepath.FromSlash(fd.Prefix+"/"+fd.FileName)).
				Msgf("sender %s", fd.State.String())
		}
	}
	s.diffList = diff
	s.missList = miss
	return nil
}

func (s *sender) spawnReaders() {
	// concurrent IO parameter
	dCtx := context.Context(s.ctx)

	// spawn readers if we have diff files
	if len(s.diffList) > 0 {
		log.Debug().
			Msg("sender - spawning roll readers")

		for i := 0; i < s.ccIo; i++ {
			rr := NewRollReader(dCtx, s.rrCh, s.receiver)
			s.g.Go(rr.Start)
		}
	}

	// spawn missing file senders if we have missing files
	if len(s.missList) > 0 {
		log.Debug().
			Msg("sender - spawning bytes readers")

		for i := 0; i < s.ccIo; i++ {
			br := NewBytesReader(dCtx, s.brCh, s.receiver)
			s.g.Go(br.Start)
		}
	}
}

func (s *sender) sendDataToReaders() {

	// send data - diff first
	for _, fd := range s.diffList {

		if fd.FileSize > uint64(splitSize) && s.ccIo > 1 {
			chunkSize := int64(fd.FileSize / uint64(s.ccIo))
			log.Debug().
				Int64("file size", int64(fd.FileSize)).
				Int64("chunk size", chunkSize).
				Int("io_concurency", s.ccIo).
				Msg("sender - using paralel reading")

			for chunk := 0; chunk < s.ccIo; chunk++ {
				limit := chunkSize * (int64(chunk) + 1)
				if limit > int64(fd.FileSize) {
					limit = int64(fd.FileSize)
				}
				s.rrCh <- core.NewRSQ(fd, int64(chunk)*chunkSize, limit)
			}

		} else {
			s.rrCh <- core.NewRSQ(fd, 0, int64(fd.FileSize))
		}
	}

	// new files next
	for _, fd := range s.missList {

		if fd.FileSize > uint64(splitSize) && s.ccIo > 1 {
			chunkSize := int64(fd.FileSize / uint64(s.ccIo))
			log.Debug().
				Int64("file size", int64(fd.FileSize)).
				Int64("chunk size", chunkSize).
				Int("io_concurency", s.ccIo).
				Msg("sender - using paralel reading")

			for chunk := 0; chunk < s.ccIo; chunk++ {

				limit := chunkSize * (int64(chunk) + 1)
				if limit > int64(fd.FileSize) {
					limit = int64(fd.FileSize)
				}
				offset := int64(chunk) * chunkSize

				log.Trace().
					Str("filename", fd.FileName).
					Int("chunk", chunk).
					Int64("offset", offset).
					Int64("limit", limit).
					Msg("sender - reading file per partes")

				s.brCh <- &core.Message{
					FileDesc: fd,
					Flag:     core.RSQ,
					Offset:   offset,
					Limit:    limit,
				}
			}

		} else {
			s.brCh <- &core.Message{
				FileDesc: fd,
				Flag:     core.RSQ,
				Offset:   0,
				Limit:    int64(fd.FileSize),
			}
		}
	}
}

func (s *sender) stopReaders() {
	// all data sent, stop zee workerz
	if len(s.diffList) > 0 {
		for i := 0; i < s.ccIo; i++ {
			s.rrCh <- &core.Message{
				Flag: core.FIN,
			}
		}
	}
	if len(s.missList) > 0 {
		for i := 0; i < s.ccIo; i++ {
			s.brCh <- &core.Message{
				Flag: core.FIN,
			}
		}
	}
}

// LocalSender represents blah balh
type LocalSender struct {
	inbox <-chan *core.Message
	sender
}

func NewLocalSender(ctx context.Context, in <-chan *core.Message, receiver chan *core.Message) *LocalSender {
	ccIo := viper.GetInt("io_concurrency")
	log.Debug().
		Int("ccio", ccIo).
		Msg("starting new local sender")
	return &LocalSender{
		sender: sender{
			ctx:      ctx,
			uuid:     uuid.New(),
			receiver: receiver,
			ccIo:     ccIo,
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

	err = w.parseRemoteList(msg)
	if err != nil {
		return errors.Wrap(err, "failed parsong remote file listing")
	}

	// prepare for transfer
	w.rrCh = make(chan *core.Message, w.ccIo)
	w.brCh = make(chan *core.Message, w.ccIo)
	w.g = new(errgroup.Group)

	w.spawnReaders()

	w.sendDataToReaders()

	w.stopReaders()

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
	log.Debug().
		Msgf("local sender - done")
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
			ccIo:     ccIo,
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
	msg, err := w.sendJson(url, msg)
	if err != nil {
		errors.Wrap(err, "http sender - send failed")
	}

	err = w.parseRemoteList(msg)
	if err != nil {
		return errors.Wrap(err, "local sender")
	}

	// prepare for transfer
	w.rrCh = make(chan *core.Message, w.ccIo)
	w.brCh = make(chan *core.Message, w.ccIo)
	w.g = new(errgroup.Group)

	// starting http senders
	for i := 0; i < 2*w.ccIo; i++ {
		log.Trace().
			Msg("http sender - starting http client worker")
		w.g.Go(w.dataSender)
	}

	w.spawnReaders()

	w.sendDataToReaders()

	w.stopReaders()

	// don't forget zee http senderz
	for i := 0; i < 2*w.ccIo; i++ {
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
