package workers

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"

	"github.com/davecgh/go-spew/spew"
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
				Send()
		}
	}

	// prepare a message for the receiver
	msg := &core.Message{
		Flag:     core.INI,
		UUID:     w.uuid,
		FileList: w.srcList,
	}

	log.Debug().
		Msgf("sending source file list, length: %d", len(w.srcList))

	err := sendWithTimeout(msg, w.receiver)
	if err != nil {
		return err
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
				Msgf("sender %s", fd.State.String())
			missList = append(missList, fd)
		} else if fd.State == lfs.Diff {
			// diff file
			log.Debug().
				Int64("block size", fd.BlockSize).
				Int("hashes count", len(fd.Weak)).
				Str("file", fd.Prefix+"/"+fd.FileName).
				Msgf("sender %s", fd.State.String())
			diffList = append(diffList, fd)

		} else {
			// skipped file
			log.Debug().
				Str("file", fd.Prefix+"/"+fd.FileName).
				Msg(fd.State.String())
		}
	}

	rrInbox := make(chan *core.Message)
	brInbox := make(chan *core.Message)
	ccIo := viper.GetInt("io_concurrency")
	g := new(errgroup.Group)
	dCtx := context.Context(w.ctx)

	// spawn readers if we have diff files
	if len(diffList) > 0 {
		log.Debug().
			Msg("sender spawning roll readers")

		for i := 0; i < ccIo; i++ {
			rr := NewRollReader(dCtx, rrInbox, w.receiver)
			g.Go(func() error { return rr.Start() })
		}
	}

	// spawn missing file senders if we have missing files
	if len(missList) > 0 {
		log.Debug().
			Msg("sender spawning bytes readers")

		for i := 0; i < ccIo; i++ {
			log.Debug().
				Msgf("starting byte reader: %d", i)
			br := NewBytesReader(dCtx, brInbox, w.receiver, i)
			g.Go(func() error { return br.Start() })
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
		Msg("local sender finished, sending FIN to receciver")
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
	ctx  context.Context
	uuid uuid.UUID
}

func NewHttpSender(ctx context.Context) *HttpSender {
	return &HttpSender{
		ctx:  ctx,
		uuid: uuid.New(),
	}
}

func (w *HttpSender) Start() error {
	srcList, err := lfs.ParseDir(viper.GetString("directory"))
	if err != nil {
		log.Fatal().
			Err(err).
			Stack().
			Caller().
			Send()
	}

	for _, fd := range srcList {
		if !fd.IsDir {
			fd.SetBlockSize()
			log.Trace().
				Str("file name", fd.FileName).
				Int64("file size", int64(fd.FileSize)).
				Int64("block size calculated", fd.BlockSize).
				Send()
		}
	}

	msg, err := json.Marshal(srcList)

	// do a simple validation, though not strictly neccessary, http would take care
	host := viper.GetString("remote_destination")
	port := viper.GetInt("port")

	url, err := url.Parse(fmt.Sprintf("http://%s:%d", host, port))
	if err != nil {
		return errors.Wrap(err, "invalid URL")
	}

	url.Path += "/list"

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

	client := &http.Client{
		Timeout:   defaultTimeout,
		Transport: tr,
	}

	buf, err := compress(msg)
	if err != nil {
		return errors.Wrap(err, "error compressing data")
	}

	req, err := http.NewRequestWithContext(w.ctx, http.MethodPost, url.String(), buf)
	//req.Header.Set("X-Custom-Header", "myvalue")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "PeXync-client-mode")
	req.Header.Set("Accept-Encoding", "gzip")
	req.Header.Set("Content-Encoding", "gzip")
	if err != nil {
		return errors.Wrap(err, "error creating http request")
	}

	resp, err := client.Do(req)
	if err != nil {
		return errors.Wrap(err, "error connecting server")
	}

	defer resp.Body.Close()

	fmt.Println("response Status:", resp.Status)
	fmt.Println("response Headers:", resp.Header)

	buf, err = decompress(resp.Body)
	if err != nil {
		return errors.Wrap(err, "error reading server response")
	}

	var dstList []*lfs.FileDesc
	err = json.Unmarshal(buf.Bytes(), &dstList)
	if err != nil {
		return errors.Wrap(err, "error reading server response")
	}

	spew.Dump(dstList)

	return nil
}
