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
	"golang.org/x/sync/errgroup"
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
	inbox      <-chan *core.Message
	sender     chan<- *core.Message
	state      senderState // not sure if needed, or if I ever implement this
	senderUUID uuid.UUID   // same here
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

	var done bool

	for !done {
		select {
		case <-w.ctx.Done():
			log.Debug().Msg("local receiver closing, context done")
			done = true
			break
		case msg := <-w.inbox:
			switch msg.Flag {
			case core.RST:
				err := w.handleRst(msg)
				if err != nil {
					return errors.Wrap(err, "failed during sync init")
				}
			case core.FIN:
				log.Trace().
					Msg("receiver received FIN")
				done = true
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

func (w *LocalReceiver) handleRst(msg *core.Message) error {
	w.senderUUID = msg.UUID
	log.Trace().
		Str("sender uuid", w.senderUUID.String()).
		Msgf("local receiver received file list, length: %d", len(msg.List))
	// stop all writers if any, this is a reset!

	// get local (destination file list)
	dstDir := viper.GetString("local_destination")

	// check if the destination dir exists
	if _, err := os.Stat(dstDir); os.IsNotExist(err) {
		// create one
		os.Mkdir(dstDir, os.ModeDir)
	} else if err != nil {
		return err
	}

	log.Debug().Msg("receiver listing files")
	dstList, err := lfs.ParseDir(dstDir)
	if err != nil {
		return errors.Wrap(err, "unable to list directory")
	}

	log.Debug().Msg("receiver parsing files")

	// build a map of local entries for faster lookup
	dstMap := make(map[string]*lfs.FileDesc, len(dstList))
	for _, dstFd := range dstList {
		dstMap[dstFd.RelPath] = dstFd
	}

	for _, srcFd := range msg.List {
		path := dstDir + srcFd.RelPath
		if dstFd, ok := dstMap[srcFd.RelPath]; ok {
			log.Debug().
				Str("path", path).
				Msg("file exists")
			// it does exist on destination
			if srcFd.FileSize == dstFd.FileSize && srcFd.Modified == dstFd.Modified {
				// check permission, modtime and ownership and aupdate if needed
				err = fixMeta(dstDir, srcFd, dstFd)
				if err != nil {
					return nil
				}
				srcFd.State = lfs.Skip
				log.Debug().
					Str("path", path).
					Msg("receiver updating metadata")
			} else {
				log.Debug().
					Str("sender path", srcFd.RelPath).
					Uint64("source file size", srcFd.FileSize).
					Uint64("destination file size", dstFd.FileSize).
					Time("source file mod", srcFd.Modified).
					Time("receiver file mod", dstFd.Modified).
					Msg("receiver DIFF")

				srcFd.State = lfs.Diff

				// determine what has changed, if permission and/or modtime only, do not set it to diff

				if !srcFd.IsDir {
					// treat "remote" files smaller than block sizes as missing
					if uint64(srcFd.BlockSize) > dstFd.FileSize {
						srcFd.State = lfs.Missing
						continue
					}
					// check for zero sized files
					if srcFd.FileSize == 0 {
						log.Debug().
							Str("path", path).
							Msg("empty file")

						// file creted, modify meta if required and set as done
						err = fixMeta(dstDir, srcFd, dstFd)
						if err != nil {
							return nil
						}
						srcFd.State = lfs.Skip
					}
				} else {
					log.Debug().
						Str("path", path).
						Msg("receiver fixing dir meta")
					// directory that exists, check meta only
					err = fixMeta(dstDir, srcFd, dstFd)
					if err != nil {
						return nil
					}
					srcFd.State = lfs.Skip
				}
			}
			continue
		} else {
			// it does not exist on destination, check if it's a ditrectory
			log.Debug().
				Str("path", path).
				Msg("file does not exist")
			if srcFd.IsDir {
				// create directory
				log.Debug().
					Str("path", path).
					Msg("creating directory")
				if _, err := os.Stat(path); os.IsNotExist(err) {
					// create one
					os.Mkdir(path, os.ModeDir)
				} else if err != nil {
					return errors.Wrapf(err, "%s - unable to create directory", path)
				}
			} else {
				// set it as missing
				// check for zero sized files
				if srcFd.FileSize == 0 {
					log.Debug().
						Str("path", path).
						Msg("empty file")
					file, err := os.Create(path)
					if err != nil {
						return errors.Wrapf(err, "%s - unable to create file", path)
					}
					file.Close()

					// TODO fix metadata on new empty file
					srcFd.State = lfs.Skip
					continue
				}
				srcFd.State = lfs.Missing
				log.Debug().
					Str("path", path).
					Msg("receiver regular missing file")
			}
		}
	}

	// add checksums
	ccIo := viper.GetInt("io_concurrency")
	rc := 0
	g := new(errgroup.Group)
	for _, srcFd := range msg.List {
		if srcFd.State == lfs.Diff || srcFd.State == lfs.Missing {
			if rc < ccIo {
				log.Debug().
					Str("file name", srcFd.FileName).
					Msg("calculating checksums")
				g.Go(func() error { return core.AddChecksums(srcFd) })
				rc++
			} else {
				if err := g.Wait(); err != nil {
					return errors.Wrapf(err, "%s - error adding checksum", srcFd.RelPath)
				}
				rc -= ccIo
			}
		}
	}

	msg.Flag = core.SUM
	err = sendWithTimeout(msg, w.sender)
	if err != nil {
		return err
	}
	return nil
}

func fixMeta(dstDir string, srcFd, dstFd *lfs.FileDesc) error {
	path := dstDir + "/" + srcFd.RelPath
	// check permissions and ownership
	if srcFd.Modified != dstFd.Modified {
		err := os.Chtimes(path, srcFd.Modified, srcFd.Modified)
		if err != nil {
			return errors.Wrapf(err, "%s - unable to modify mtime", path)
		}
	}
	if srcFd.Mode.Perm() != dstFd.Mode.Perm() {
		err := os.Chmod(path, srcFd.Mode.Perm())
		if err != nil {
			return errors.Wrapf(err, "%s - unable to modify permissions", path)
		}
	}
	if srcFd.Gid != dstFd.Gid || srcFd.Uid != dstFd.Uid {
		err := os.Chown(path, int(srcFd.Uid), int(srcFd.Gid))
		if err != nil {
			return errors.Wrapf(err, "%s - unable to modify ownership", path)
		}
	}
	return nil
}
