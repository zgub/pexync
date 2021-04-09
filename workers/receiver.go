package workers

import (
	"context"
	"os"

	"github.com/google/uuid"
	"github.com/pkg/errors"
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
	list       []*lfs.FileDesc
	inbox      <-chan *core.Message
	sender     chan<- *core.Message
	state      senderState
	senderUUID uuid.UUID
}

func NewLocalReceiver(ctx context.Context, in <-chan *core.Message, sender chan<- *core.Message) *LocalReceiver {
	return &LocalReceiver{
		ctx:    ctx,
		inbox:  in,
		sender: sender,
		state:  RST,
	}
}

func (w *LocalReceiver) Start() error {

	var (
		pkt   *core.Message
		check bool = true
	)

	for check {
		select {
		case <-w.ctx.Done():
			log.Debug().Msg("local receiver closing, context done")
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
				} else if err != nil {
					return err
				}

				log.Debug().Msg("receiver listing files")
				lfl, err := lfs.ParseDir(dst)
				if err != nil {
					return errors.Wrap(err, "unable to list directory")
				}

				log.Debug().Msg("receiver parsing files")
				for _, senderFile := range w.list {
					for _, receiverFile := range lfl {
						senderFile.State = lfs.Missing
						if senderFile.RelPath == receiverFile.RelPath {
							if senderFile.FileSize == receiverFile.FileSize && senderFile.Modified == receiverFile.Modified {
								// check permissions and ownership
								senderFile.State = lfs.Skip
							} else {
								log.Debug().
									Str("sender path", senderFile.RelPath).
									Str("receiver path", receiverFile.RelPath).
									Uint64("sender file size", senderFile.FileSize).
									Uint64("receiver file size", receiverFile.FileSize).
									Time("sender file mod", senderFile.Modified).
									Time("receiver file mod", receiverFile.Modified).
									Msg("receiver DIFF")

								senderFile.State = lfs.Diff

								// determine what has changed, if permission and/or modtime only, do not set it to diff

								if !senderFile.IsDir {
									// treat "remote" files smaller than block sizes as missing
									if uint64(senderFile.BlockSize) > receiverFile.FileSize {
										senderFile.State = lfs.Missing
										break
									}
									err := core.AddChecksums(senderFile)
									if err != nil {
										return err
									}
									//senderFile.Weak = receiverFile.Weak
								}
								// add checksum
							}
							break
						}
					}
				}
				pkt.Flag = core.SUM
				err = sendWithTimeout(pkt, w.sender)
				if err != nil {
					return err
				}
			case core.FIN:
				log.Trace().
					Msg("receiver received FIN")
				check = false
				break
			case core.DTA:
				log.Trace().
					Msg("data received")
			default:
				return errors.New("unknown message received")
			}
		}
	}

	// spawn filewriters

	// wait for the transfer to finish

	// validate ???

	// end
	log.Trace().
		Msg("local receiver finished")
	return nil
}
