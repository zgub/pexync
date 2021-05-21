package workers

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"path/filepath"

	"github.com/fsnotify/fsnotify"
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
	srcDir               string
	srcList              []*lfs.FileDesc
	id                   uuid.UUID
	diffList, missList   []*lfs.FileDesc
	g                    *errgroup.Group
	rrCh, brCh, receiver chan *core.Message
	ccIo                 int64 // this gets used so may times, it deserves an instance var
	lastFileIdx          int
}

func (s *sender) genSrcList() error {
	// perform directory listing
	list, err := lfs.ParseDir(s.srcDir)
	if err != nil {
		return errors.Wrap(err, "sender - directory parsing failed")
	}
	fmt.Println("=============================== seting idx")
	s.lastFileIdx = len(list)

	// calculate blocksizes for each file
	for _, fd := range list {
		if !fd.IsDir {
			fd.SetBlockSize()
			// beware of empty files
			if fd.BlockSize == 0 {
				fd.BlockSize = 700
			}
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
				/*
					sha1sh := sha1.New()
					p := filepath.Join(fd.Prefix, fd.FileName)
					mf, err := os.Open(p)
					if err != nil {
						return errors.Wrapf(err, "unable to read file: %s", filepath.FromSlash(fd.Prefix+"/"+fd.FileName))
					}
					defer mf.Close()
					r := io.Reader(mf)
					br := bufio.NewReader(r)
					_, err = io.Copy(sha1sh, br)
					if err != nil {
						return errors.Wrapf(err, "unable to read file: %s", filepath.FromSlash(fd.Prefix+"/"+fd.FileName))
					}
				*/
				rSha1 := fd.Sha1
				lSha1, err := fd.GetSha1()
				if err != nil {
					return errors.Wrapf(err, "failed to calculate SHA1 digest from: %s", filepath.Join(fd.Prefix, fd.FileName))
				}
				if bytes.Equal(rSha1, lSha1) {
					log.Trace().
						Msgf("sender - file: %s has matching SHA1 digest, skipping", filepath.Join(fd.Prefix, fd.FileName))
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

		for i := int64(0); i < s.ccIo; i++ {
			rr := NewRollReader(dCtx, s.rrCh, s.receiver)
			s.g.Go(rr.Start)
		}
	}

	// spawn missing file senders if we have missing files
	if len(s.missList) > 0 {
		log.Debug().
			Int64("io concurrency", s.ccIo).
			Msg("sender - spawning bytes readers")

		for i := int64(0); i < s.ccIo; i++ {
			br := NewBytesReader(dCtx, s.brCh, s.receiver)
			s.g.Go(br.Start)
		}
	}
}

func (s *sender) sendDataToReaders() {

	// send data - diff first
	for _, fd := range s.diffList {

		if fd.FileSize > splitSize && s.ccIo > 1 {
			chunkSize := fd.FileSize / s.ccIo
			log.Debug().
				Int64("file size", fd.FileSize).
				Int64("chunk size", chunkSize).
				Int64("io_concurency", s.ccIo).
				Msg("sender - using paralel reading")

			for chunk := int64(0); chunk < s.ccIo; chunk++ {
				limit := chunkSize * (chunk + 1)
				if limit > fd.FileSize {
					limit = fd.FileSize
				}
				// data stream count = s.ccIo
				s.rrCh <- core.NewRSQ(s.id, fd, chunk*chunkSize, limit, s.ccIo)
			}

		} else {
			// data streams count = 1
			s.rrCh <- core.NewRSQ(s.id, fd, 0, fd.FileSize, 1)
		}
	}

	// new files next
	for _, fd := range s.missList {

		// data streams count = 1
		s.brCh <- core.NewRSQ(s.id, fd, 0, fd.FileSize, 1)
	}
}

func (s *sender) stopReaders() {
	// all data sent, stop zee workerz
	if len(s.diffList) > 0 {
		for i := int64(0); i < s.ccIo; i++ {
			s.rrCh <- core.NewFIN(s.id)
		}
	}
	if len(s.missList) > 0 {
		for i := int64(0); i < s.ccIo; i++ {
			s.brCh <- core.NewFIN(s.id)
		}
	}
}

// LocalSender represents blah balh
type LocalSender struct {
	inbox <-chan *core.Message
	sender
}

func NewLocalSender(ctx context.Context, senderID uuid.UUID, in <-chan *core.Message, receiver chan *core.Message) *LocalSender {
	ccIo := viper.GetInt("io_concurrency")
	log.Debug().
		Int("ccio", ccIo).
		Msg("starting new local sender")
	return &LocalSender{
		sender: sender{
			srcDir:   viper.GetString("source"),
			ctx:      ctx,
			id:       senderID,
			receiver: receiver,
			rrCh:     make(chan *core.Message, ccIo),
			brCh:     make(chan *core.Message, ccIo),
			ccIo:     int64(ccIo),
		},
		inbox: in,
	}
}

func (ls *LocalSender) Start() error {

	if err := ls.genSrcList(); err != nil {
		return err
	}

	// prepare a message for the receiver
	msg := core.NewINI(ls.id, ls.srcList)

	log.Debug().
		Msgf("local sender - source file list, length: %d", len(ls.srcList))

	err := sendWithTimeout(msg, ls.receiver)
	if err != nil {
		return errors.Wrap(err, "local sender")
	}

	// receive the filelist with checksums
	msg, err = recvWithTimeout(ls.inbox)
	if err != nil {
		return errors.Wrap(err, "local sender")
	}

	err = ls.parseRemoteList(msg)
	if err != nil {
		return errors.Wrap(err, "failed parsong remote file listing")
	}

	// prepare for transfer
	ls.g = new(errgroup.Group)

	ls.spawnReaders()

	ls.sendDataToReaders()

	ls.stopReaders()

	// validate ???

	// end
	err = ls.g.Wait()
	if err != nil {
		return errors.Wrap(err, "local sender worker failed")
	}
	log.Trace().
		Msg("local sender - finished, sending FIN to receciver")
	msg = core.NewFIN(ls.id)
	err = sendWithTimeout(msg, ls.receiver)
	if err != nil {
		return errors.Wrap(err, "sender failure")
	}
	log.Debug().
		Msgf("local sender - done")
	return nil
}

type HttpSender struct {
	url      *url.URL
	client   *http.Client
	syncOnce bool
	watcher  *fsnotify.Watcher
	watchMap map[string]*lfs.FileDesc
	sender
}

// NewHttpSender returns a http sender instance, syncOnce set to false makes it stop after all the initial files are synced
func NewHttpSender(ctx context.Context, senderID uuid.UUID, syncOnce bool) (*HttpSender, error) {

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
		Msg("destination set")

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
		syncOnce: syncOnce,
		sender: sender{
			srcDir:   viper.GetString("source"),
			ctx:      ctx,
			id:       senderID,
			receiver: make(chan *core.Message),
			rrCh:     make(chan *core.Message, ccIo),
			brCh:     make(chan *core.Message, ccIo),
			ccIo:     int64(ccIo),
		},
	}

	return s, nil
}

func (hs *HttpSender) Start() error {

	if err := hs.genSrcList(); err != nil {
		return err
	}

	// create a new message for the other side
	msg := core.NewINI(hs.id, hs.srcList)

	// send
	url := hs.url.String() + "/meta"
	resp, err := hs.sendJson(url, msg)
	if err != nil {
		log.Fatal().
			Err(err).
			Msg("error comunicating with server")
	}

	err = hs.parseRemoteList(resp)
	if err != nil {
		return errors.Wrap(err, "local sender")
	}

	// one errorgroup for readers and data senders
	hs.g = new(errgroup.Group)

	// another, independent one, just for http senders
	eg := new(errgroup.Group)

	// starting http senders
	for i := int64(0); i < 2*hs.ccIo; i++ {
		log.Trace().
			Msgf("http sender - starting http client worker %d", i)
		eg.Go(hs.dataSender)
	}

	hs.spawnReaders()

	hs.sendDataToReaders()

	// stop the readers if in sync once mode
	if hs.syncOnce {
		hs.stopReaders()

		err = hs.g.Wait()
		if err != nil {
			return errors.Wrap(err, "http sender worker failed")
		}

		// don't forget to stop http senders in sync_once mode
		for i := int64(0); i < 2*hs.ccIo; i++ {
			hs.receiver <- core.NewFIN(hs.id)
		}

		err = eg.Wait()
		if err != nil {
			return errors.Wrap(err, "http reader failed")
		}

		log.Trace().
			Msg("http sender - initial sync finished")

	}
	return nil
}
