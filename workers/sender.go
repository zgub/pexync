package workers

import (
	"context"
	"sync"
	"time"

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

	log.Info().
		Caller().
		Msgf("list length: %d", len(w.list))

	err := sendWithTimeout(pkt, w.receiver)
	if err != nil {
	}

	// receive the filelist with checksums
	timeout = time.After(timeoutValue)
	select {
	case pkt = <-w.inbox:
	case <-timeout:
		log.Fatal().
			Msgf("timeout %s reached while waiting for response from remote", timeoutValue)
	}
	err := pkt[0].Error
	if err != nil {
		log.Fatal().
			Err(err).
			Msg("local sender received an error from remote")
	}
	w.list = pkt[0].List
	//spew.Dump(w.list)
	// spaw filereaders

	// wait for the transfer to finish

	// validate ???

	// end
	log.Info().
		Msg("local sender finished, sending FIN to receciver")
	pkt = []*core.Message{
		{
			Flag: core.FIN,
			UUID: w.uuid,
		},
	}
}
