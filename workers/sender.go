package workers

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/zgub/pexync/core"
	"github.com/zgub/pexync/lfs"
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
		return err
	}
	w.list = pkt.List
	//spew.Dump(w.list)

	// spawn filereaders

	// this has to be reworked
	var wg sync.WaitGroup

	for _, fd := range w.list {
		if fd.State == lfs.Missing {
			fmt.Printf("[-] %s\n", fd.RelPath)
		} else if fd.State == lfs.Diff {
			fmt.Printf("[+] %s\t %d checksums\n", fd.RelPath, len(fd.Weak))
			blockSize := lfs.GetBlockSize(fd)
			f, err := os.Open(fd.Prefix + "/" + fd.RelPath)
			stat, err := os.Stat(fd.Prefix + "/" + fd.RelPath)
			if err != nil {
				return errors.Wrap(err, "error file stat")
			}
			size := stat.Size()
			r := io.ReaderAt(f)
			sr := io.NewSectionReader(r, 0, size)
			fileReader := NewRollReader(w.ctx, &wg, w.uuid, fd, blockSize, sr, w.receiver)
			go fileReader.Start()
			wg.Add(1)
		} else {
			fmt.Printf("[x] %s\n", fd.RelPath)
		}
	}

	// wait for the transfer to finish

	// validate ???

	// end
	wg.Wait()
	log.Trace().
		Msg("local sender finished, sending FIN to receciver")
	pkt = &core.Message{
		Flag: core.FIN,
		UUID: w.uuid,
	}
	sendWithTimeout(pkt, w.receiver)
	return nil
}
