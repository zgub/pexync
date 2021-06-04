package workers

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"sync"

	//"github.com/fsnotify/fsnotify"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
	"github.com/zgub/pexync/core"
	"github.com/zgub/pexync/fsnotify"
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
	syncOnce             bool  // valid for http sender only, however it's required here due file readers
	lastFileIdx          int
}

func (s *sender) genSrcList() error {
	// perform directory listing
	list, err := lfs.ParseDir(s.srcDir)
	if err != nil {
		return errors.Wrap(err, "sender - directory parsing failed")
	}
	s.lastFileIdx = len(list)

	// calculate blocksizes for each file
	for _, fd := range list {
		if fd.IsDir == false {
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
		if fd.SyncState == lfs.Missing && fd.IsDir == false {
			// new file
			log.Debug().
				Int64("block size", fd.BlockSize).
				Str("file", filepath.Join(fd.Prefix, fd.FileName)).
				Msgf("sender %s", fd.SyncState.String())
			miss = append(miss, fd)
		} else if fd.SyncState == lfs.Diff || fd.SyncState == lfs.Meta {
			// diff file
			log.Debug().
				Int64("block size", fd.BlockSize).
				Int("hashes count", len(fd.Weak)).
				Str("file", filepath.Join(fd.Prefix, fd.FileName)).
				Msgf("sender %s", fd.SyncState.String())
			if fd.SyncState == lfs.Meta {
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
				Msgf("sender %s", fd.SyncState.String())
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
	if len(s.diffList) > 0 || s.syncOnce == false {
		log.Debug().
			Msg("sender - spawning roll readers")

		for i := int64(0); i < s.ccIo; i++ {
			rrw := NewRollReader(dCtx, s.rrCh, s.receiver)
			s.g.Go(rrw.Start)
		}
	}

	// spawn missing file senders if we have missing files
	if len(s.missList) > 0 || !s.syncOnce == false {
		log.Debug().
			Int64("io concurrency", s.ccIo).
			Msg("sender - spawning bytes readers")

		for i := int64(0); i < s.ccIo; i++ {
			brw := NewBytesReader(dCtx, s.brCh, s.receiver)
			s.g.Go(brw.Start)
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
				s.rrCh <- core.NewSyncRSQ(s.id, fd, chunk*chunkSize, limit, s.ccIo)
			}

		} else {
			// data streams count = 1
			s.rrCh <- core.NewSyncRSQ(s.id, fd, 0, fd.FileSize, 1)
		}
	}

	// new files next
	for _, fd := range s.missList {

		// data streams count = 1
		s.brCh <- core.NewSyncRSQ(s.id, fd, 0, fd.FileSize, 1)
	}
}

func (s *sender) stopReaders() {
	fmt.Println("stopping readers")
	// all data sent, stop zee workerz
	if len(s.diffList) > 0 {
		for i := int64(0); i < s.ccIo; i++ {
			fmt.Println("----------------------- sending FIN to roll readers")
			s.rrCh <- core.NewFIN(s.id)
		}
	}
	if len(s.missList) > 0 {
		for i := int64(0); i < s.ccIo; i++ {
			fmt.Println("----------------------- sending FIN to bytes readers")
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

func (lsw *LocalSender) Start() error {

	if err := lsw.genSrcList(); err != nil {
		return err
	}

	// prepare a message for the receiver
	msg := core.NewINI(lsw.id, lsw.srcList)

	log.Debug().
		Msgf("local sender - source file list, length: %d", len(lsw.srcList))

	err := sendWithTimeout(msg, lsw.receiver)
	if err != nil {
		return errors.Wrap(err, "local sender")
	}

	// receive the filelist with checksums
	msg, err = recvWithTimeout(lsw.inbox)
	if err != nil {
		return errors.Wrap(err, "local sender")
	}

	err = lsw.parseRemoteList(msg)
	if err != nil {
		return errors.Wrap(err, "failed parsong remote file listing")
	}

	// prepare for transfer
	lsw.g = new(errgroup.Group)

	lsw.spawnReaders()

	lsw.sendDataToReaders()

	lsw.stopReaders()

	fmt.Println(">>>>>>>>>>>>>>>>>>>>>> sent stop to readers")

	// validate ???

	// end
	err = lsw.g.Wait()
	if err != nil {
		return errors.Wrap(err, "local sender worker failed")
	}
	fmt.Println(">>>>>>>>>>>>>>>>>>>>>>>> want to send FIN")
	log.Trace().
		Msg("local sender - finished, sending FIN to receciver")
	msg = core.NewFIN(lsw.id)
	err = sendWithTimeout(msg, lsw.receiver)
	if err != nil {
		return errors.Wrap(err, "sender failure")
	}
	log.Debug().
		Msgf("local sender - done")
	return nil
}

type HttpSender struct {
	url              *url.URL
	client           *http.Client
	directoryWatcher *fsnotify.Watcher
	syncStatus       map[string]*fileSync
	syncStatusMux    sync.RWMutex
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
		url:    url,
		client: c,
		sender: sender{
			syncOnce: syncOnce,
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

func (hsw *HttpSender) Start() error {

	if err := hsw.genSrcList(); err != nil {
		return err
	}

	// create a new message for the other side
	msg := core.NewINI(hsw.id, hsw.srcList)

	// send
	url := hsw.url.String() + "/meta"
	resp, err := hsw.sendJson(url, msg)
	if err != nil {
		log.Fatal().
			Err(err).
			Msg("error comunicating with server")
	}

	err = hsw.parseRemoteList(resp)
	if err != nil {
		return errors.Wrap(err, "local sender")
	}

	// one errorgroup for readers and data senders
	hsw.g = new(errgroup.Group)

	// another, independent one, just for http senders
	eg := new(errgroup.Group)

	// starting http senders
	for i := int64(0); i < 2*hsw.ccIo; i++ {
		log.Trace().
			Msgf("http sender - starting http client worker %d", i)
		eg.Go(hsw.dataSender)
	}

	hsw.spawnReaders()

	hsw.sendDataToReaders()

	// stop the readers if in sync once mode
	if hsw.syncOnce {
		hsw.stopReaders()

		err = hsw.g.Wait()
		if err != nil {
			return errors.Wrap(err, "http sender worker failed")
		}

		// don't forget to stop http senders in sync_once mode
		for i := int64(0); i < 2*hsw.ccIo; i++ {
			hsw.receiver <- core.NewFIN(hsw.id)
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
