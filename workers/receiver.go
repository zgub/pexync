package workers

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
	"github.com/zgub/pexync/core"
	"github.com/zgub/pexync/lfs"
)

type senderState int

// if I want to reset the receiver to initial state I need a state :-/
const (
	RST senderState = iota // initialized state
	RCV                    // file list received
	WRT                    // spawned writers
)

// LocalSender represents blah balh
type LocalReceiver struct {
	ctx        context.Context
	wg         *sync.WaitGroup
	list       []*lfs.FileDesc
	inbox      <-chan []*core.Message
	sender     chan<- []*core.Message
	state      senderState
	senderUUID uuid.UUID
}

func NewLocalReceiver(ctx context.Context, wg *sync.WaitGroup, in <-chan []*core.Message, sender chan<- []*core.Message) *LocalReceiver {
	return &LocalReceiver{
		ctx:    ctx,
		wg:     wg,
		inbox:  in,
		sender: sender,
		state:  RST,
	}
}

func (w *LocalReceiver) Start() {
	defer w.wg.Done()

	// sending initial packet
	timeoutValue := viper.GetDuration("timeout")

	var pkt []*core.Message

	for {
		select {
		case <-w.ctx.Done():
			log.Info().Msg("local receiver closing, context done")
			break
		case pkt = <-w.inbox:
			msg := pkt[0]
			switch msg.Flag {
			case core.RST:
				w.list = msg.List
				w.senderUUID = msg.UUID
				log.Info().
					Str("sender uuid", w.senderUUID.String()).
					Msgf("local receiver received file list, length: %d", len(w.list))
				// stop all writers if any, this is a reset!
				timeout := time.After(timeoutValue)
				select {
				case w.sender <- pkt:
				case <-timeout:
					log.Fatal().
						Msgf("timout reached while trying to send delta to sender")
				}
			default:
				log.Fatal().
					Err(core.NotImplemented).
					Send()
			}
		}
	}

	// spawn filewriters

	// wait for the transfer to finish

	// validate ???

	// end
	//log.Info().
	//	Msg("local receiver finished")
}
