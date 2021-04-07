package workers

import (
	"context"
	"os"
	"sync"

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
	inbox      <-chan *core.Message
	sender     chan<- *core.Message
	state      senderState
	senderUUID uuid.UUID
}

func NewLocalReceiver(ctx context.Context, wg *sync.WaitGroup, in <-chan *core.Message, sender chan<- *core.Message) *LocalReceiver {
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

	var (
		pkt   *core.Message
		check bool = true
	)

	for check {
		select {
		case <-w.ctx.Done():
			log.Trace().Msg("local receiver closing, context done")
			check = false
			break
		case pkt = <-w.inbox:
			msg := pkt
			switch msg.Flag {
			case core.RST:
				w.list = msg.List
				w.senderUUID = msg.UUID
				log.Trace().
					Str("sender uuid", w.senderUUID.String()).
					Msgf("local receiver received file list, length: %d", len(w.list))
				// stop all writers if any, this is a reset!

				// get local (destination file list)
				dst := viper.GetString("local_destination")

				// check if the destination dir exists
				if _, err := os.Stat(dst); os.IsNotExist(err) {
					// create one
					os.Mkdir(dst, os.ModeDir)
				}

				lfl, err := lfs.GetList(dst)
				core.Fatality(err)

				for _, senderFile := range w.list {
					for _, receiverFile := range lfl {
						senderFile.State = lfs.Missing
						if senderFile.RelPath == receiverFile.RelPath {
							if senderFile.FileSize == receiverFile.FileSize && senderFile.Modified == receiverFile.Modified {
								// check permissions and ownership
								senderFile.State = lfs.Skip
							} else {
								log.Trace().
									Str("sender path", senderFile.RelPath).
									Str("receiver path", receiverFile.RelPath).
									Uint64("sender file size", senderFile.FileSize).
									Uint64("receiver file size", receiverFile.FileSize).
									Time("sender file mod", senderFile.Modified).
									Time("receiver file mod", receiverFile.Modified).
									Msg("DIFF")
								senderFile.State = lfs.Diff

								// determine what has changed, if permission and/or modtime only, do not set it to diff

								if !senderFile.IsDir {
									blockSize := lfs.GetBlockSize(senderFile)
									log.Info().
										Int("checksum block size", blockSize).
										Send()
									viper.Set("block_size", blockSize)
									// ???
									err := core.AddChecksums(senderFile)
									core.Fatality(err)
									//senderFile.Weak = receiverFile.Weak
								}
								// add checksum
							}
							break
						}
					}
				}

				sendWithTimeout(pkt, w.sender)
			case core.FIN:
				log.Trace().
					Msg("receiver received FIN")
				check = false
				break
			case core.DTA:
				log.Trace().
					Msg("data received")
			default:
				core.Fatality(core.NotImplemented)
			}
		}
	}

	// spawn filewriters

	// wait for the transfer to finish

	// validate ???

	// end
	log.Trace().
		Msg("local receiver finished")
}
