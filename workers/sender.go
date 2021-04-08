package workers

import (
	"context"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
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
	pkt := &core.Message{
		Flag: core.RST,
		UUID: w.uuid,
		List: w.list,
	}

	log.Trace().
		Msgf("sender list length: %d", len(w.list))

	err := sendWithTimeout(pkt, w.receiver)
	if err != nil {
		return err
	}

	// receive the filelist with checksums
	pkt, err = recvWithTimeout(w.inbox)
	if err != nil {
		return errors.Wrap(err, "local sender")
	}

	w.list = pkt.List
	log.Debug().
		Int("sender received files list, length", len(w.list)).
		Msg("analyzing")

	// analyze
	g := new(errgroup.Group)
	sendList := make([]*lfs.FileDesc, 0)
	for _, fd := range w.list {
		if fd.State == lfs.Missing && !fd.IsDir {
			fd.BlockSize = lfs.GetBlockSize(fd)
			log.Debug().
				Int("block size", fd.BlockSize).
				Str("file", fd.Prefix+"/"+fd.FileName).
				Msg(fd.State.String())
			sendList = append(sendList, fd)
		} else if fd.State == lfs.Diff {
			fd.BlockSize = lfs.GetBlockSize(fd)
			log.Debug().
				Int("block size", fd.BlockSize).
				Int("checksum received", len(fd.Weak)).
				Str("file", fd.Prefix+"/"+fd.FileName).
				Msg(fd.State.String())
			sendList = append(sendList, fd)

		} else {
			log.Debug().
				Str("file", fd.Prefix+"/"+fd.FileName).
				Msg(fd.State.String())
		}
	}

	// spawn readers

	// send data
	for _, fd := range sendList {
		log.Debug().
			Str("sending", fd.Prefix+"/"+fd.FileName).
			Send()

		/*
			f, err := os.Open(fd.Prefix + "/" + fd.RelPath)
			if err != nil {
				return errors.Wrap(err, "error opening file")
			}
			stat, err := os.Stat(fd.Prefix + "/" + fd.RelPath)
			if err != nil {
				return errors.Wrap(err, "error file stat")
			}
			size := stat.Size()
			r := io.ReaderAt(f)
			sr := io.NewSectionReader(r, 0, size)
			fileReader := NewRollReader(w.ctx, w.uuid, fd, fd.BlockSize, sr, w.receiver)

			g.Go(func() error { return fileReader.Start() })
		*/

	}

	// wait for the transfer to finish

	// validate ???

	// end
	err = g.Wait()
	if err != nil {
		return errors.Wrap(err, "file reader error")
	}
	log.Trace().
		Msg("local sender finished, sending FIN to receciver")
	pkt = &core.Message{
		Flag: core.FIN,
		UUID: w.uuid,
	}
	sendWithTimeout(pkt, w.receiver)
	return nil
}
