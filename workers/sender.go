package workers

import (
	"context"
	"fmt"
	"sync"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/zgub/pexync/core"
	"github.com/zgub/pexync/lfs"
)

// LocalSender represents blah balh
type LocalSender struct {
	ctx      context.Context
	wg       *sync.WaitGroup
	list     []*lfs.FileDesc
	inbox    <-chan []*core.Message
	receiver chan<- []*core.Message
	uuid     uuid.UUID
}

func NewLocalSender(ctx context.Context, wg *sync.WaitGroup, fl []*lfs.FileDesc, in <-chan []*core.Message, receiver chan<- []*core.Message) *LocalSender {
	return &LocalSender{
		ctx:      ctx,
		wg:       wg,
		list:     fl,
		inbox:    in,
		receiver: receiver,
		uuid:     uuid.New(),
	}
}

func (w *LocalSender) Start() {
	defer w.wg.Done()

	// send the filelist to the receiver
	// q := []int{2, 3, 5, 7, 11, 13}
	pkt := []*core.Message{
		{
			Flag: core.RST,
			UUID: w.uuid,
			List: w.list,
		},
	}

	log.Trace().
		Msgf("sender list length: %d", len(w.list))

	err := sendWithTimeout(pkt, w.receiver)
	err.Handle()

	// receive the filelist with checksums
	pkt, err = recvWithTimeout(w.inbox)
	err.Handle()
	w.list = pkt[0].List
	//spew.Dump(w.list)
	for _, fd := range w.list {
		if fd.State == lfs.Missing {
			fmt.Printf("[-] %s\n", fd.FilePath)
		} else if fd.State == lfs.Diff {
			fmt.Printf("[+] %s'n", fd.FilePath)
		} else {
			fmt.Printf("[x] %s\n", fd.FilePath)
		}
	}
	// spawn filereaders

	// wait for the transfer to finish

	// validate ???

	// end
	log.Trace().
		Msg("local sender finished, sending FIN to receciver")
	pkt = []*core.Message{
		{
			Flag: core.FIN,
			UUID: w.uuid,
		},
	}
	sendWithTimeout(pkt, w.receiver)
}
