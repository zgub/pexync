package workers

import (
	"context"
	"fmt"

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
	list     []*lfs.FileDesc
	inbox    <-chan *core.Message
	receiver chan<- *core.Message
	uuid     uuid.UUID
}

func NewLocalSender(ctx context.Context, fl []*lfs.FileDesc, in <-chan *core.Message, receiver chan<- *core.Message) *LocalSender {
	return &LocalSender{
		ctx:      ctx,
		list:     fl,
		inbox:    in,
		receiver: receiver,
		uuid:     uuid.New(),
	}
}

func (w *LocalSender) Start() error {

	// send the filelist to the receiver
	// q := []int{2, 3, 5, 7, 11, 13}
	msg := &core.Message{
		Flag: core.RST,
		UUID: w.uuid,
		List: w.list,
	}

	// calculate block sizes
	for _, fd := range w.list {
		if !fd.IsDir {
			fd.SetBlockSize()
			log.Trace().
				Str("file name", fd.FileName).
				Int64("file size", int64(fd.FileSize)).
				Int("calculated block size", fd.BlockSize).
				Send()
		}
	}

	log.Trace().
		Msgf("sender list length: %d", len(w.list))

	err := sendWithTimeout(msg, w.receiver)
	if err != nil {
		return err
	}

	// receive the filelist with checksums
	msg, err = recvWithTimeout(w.inbox)
	if err != nil {
		return errors.Wrap(err, "local sender")
	}

	w.list = msg.List
	log.Debug().
		Int("files", len(w.list)).
		Msg("sender analyzing data from receiver")

	// analyze
	//g := new(errgroup.Group)
	sendList := make([]*lfs.FileDesc, 0)
	for _, fd := range w.list {
		if fd.State == lfs.Missing && !fd.IsDir {
			// new file
			log.Debug().
				Int("block size", fd.BlockSize).
				Str("file", fd.Prefix+"/"+fd.FileName).
				Msg(fd.State.String())
			sendList = append(sendList, fd)
		} else if fd.State == lfs.Diff {
			// diff file
			log.Debug().
				Int("block size", fd.BlockSize).
				Int("checksum received", len(fd.Weak)).
				Str("file", fd.Prefix+"/"+fd.FileName).
				Msg(fd.State.String())
			sendList = append(sendList, fd)

		} else {
			// skipped file
			log.Debug().
				Str("file", fd.Prefix+"/"+fd.FileName).
				Msg(fd.State.String())
		}
	}

	// spawn readers
	rrInbox := make(chan *core.Message)
	ccIo := viper.GetInt("io_concurrency")
	fmt.Printf("iocc: %d\n", ccIo)
	g := new(errgroup.Group)
	for i := 0; i < ccIo; i++ {
		log.Debug().
			Msgf("starting roll reader: %d", i)
		dCtx := context.Context(w.ctx)
		w := NewRollReader(dCtx, rrInbox, w.receiver)
		g.Go(func() error { return w.Start() })
	}

	// send data
	for _, fd := range sendList {

		fd.Offset = 0
		fd.Limit = int64(fd.FileSize)
		rrInbox <- &core.Message{
			FileDesc: fd,
			Flag:     core.DTA,
		}
		log.Debug().
			Str("filename", fd.Prefix+"/"+fd.FileName).
			Caller().
			Msg("sent to (rock 'n') roll")
		//f, err := os.Open(fd.Prefix + "/" + fd.RelPath)
		//if err != nil {
		//	return errors.Wrap(err, "error opening file")
		//}
		//stat, err := os.Stat(fd.Prefix + "/" + fd.RelPath)
		//if err != nil {
		//	return errors.Wrap(err, "error file stat")
		//}
		//size := stat.Size()
		//r := io.ReaderAt(f)
		//sr := io.NewSectionReader(r, 0, size)
		//fileReader := NewRollReader(w.ctx, w.uuid, fd, sr, w.receiver)

		//g.Go(func() error { return fileReader.Start() })

	}

	// sent all data, stop zee workerz
	for i := 0; i < ccIo; i++ {
		rrInbox <- &core.Message{
			Flag: core.FIN,
		}
	}
	// validate ???

	// end
	fmt.Println("waiting")
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
	sendWithTimeout(msg, w.receiver)
	return nil
}
