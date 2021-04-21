package workers

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"

	"github.com/spf13/viper"
	"github.com/zgub/pexync/core"
	"github.com/zgub/pexync/lfs"
)

func sendWithTimeout(msg *core.Message, dst chan<- *core.Message) error {
	timeoutValue := viper.GetDuration("timeout")
	timeout := time.After(timeoutValue)
	select {
	case dst <- msg:
		return nil
	case <-timeout:
		return errors.New("send timeout")
	}
}

func recvWithTimeout(src <-chan *core.Message) (*core.Message, error) {
	timeoutValue := viper.GetDuration("timeout")
	timeout := time.After(timeoutValue)

	var msg *core.Message

	select {
	case msg = <-src:
		return msg, nil
	case <-timeout:
		return nil, errors.New("read timeout")
	}
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

func respondWithJSON(w http.ResponseWriter, code int, payload interface{}) error {
	response, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write(response)
	return nil
}

func compress(p []byte) (*bytes.Buffer, error) {
	buf := new(bytes.Buffer)
	gz := gzip.NewWriter(buf)
	_, err := gz.Write(p)
	if err != nil {
		return nil, err
	}
	if err = gz.Close(); err != nil {
		return nil, err
	}
	return buf, nil
}

func decompress(r io.Reader) (*bytes.Buffer, error) {
	buf := new(bytes.Buffer)
	gz, err := gzip.NewReader(r)
	if err != nil {
		return nil, err
	}
	_, err = io.Copy(buf, gz)
	if err != nil {
		return nil, err
	}
	if err = gz.Close(); err != nil {
		return nil, err
	}
	return buf, nil
}

func compare(srcList []*lfs.FileDesc, dstDir string) (map[*lfs.FileDesc]*lfs.FileDesc, error) {
	// check if the destination dir exists
	if _, err := os.Stat(dstDir); os.IsNotExist(err) {
		// create one
		os.Mkdir(dstDir, os.ModeDir)
	} else if err != nil {
		return nil, err
	}

	dstList, err := lfs.ParseDir(dstDir)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to list directory %s", dstDir)
	}

	// build a map of local entries for faster lookup
	dstMap := make(map[string]*lfs.FileDesc, len(dstList))
	for _, dstFd := range dstList {
		dstMap[dstFd.RelPath] = dstFd
	}
	diffMap := make(map[*lfs.FileDesc]*lfs.FileDesc)
	for _, srcFd := range srcList {
		path := dstDir + srcFd.RelPath
		if dstFd, ok := dstMap[srcFd.RelPath]; ok {
			// it does exist on destination
			if srcFd.FileSize == dstFd.FileSize && srcFd.Modified == dstFd.Modified {
				// check permission, modtime and ownership and aupdate if needed
				err = fixMeta(dstDir, srcFd, dstFd)
				if err != nil {
					return nil, errors.Wrap(err, "unable to fix metadata")
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
					Time("source file modified", srcFd.Modified.UTC()).
					Time("receiver file modified", dstFd.Modified.UTC()).
					Msg("receiver DIFF")

				// sync the states in both structs
				srcFd.State = lfs.Diff
				dstFd.State = lfs.Diff
				// important for block checksum calculation
				dstFd.BlockSize = srcFd.BlockSize
				// remote index is not important, this is required for file writer
				dstFd.Idx = srcFd.Idx
				// map both here for block checksum calculation later
				diffMap[dstFd] = srcFd
				// store!
				srcList[srcFd.Idx] = srcFd

				// determine what has changed, if permission and/or modtime only, do not set it to diff

				if !srcFd.IsDir {
					// treat "remote" files smaller than block sizes as missing
					if uint64(srcFd.BlockSize) > dstFd.FileSize {
						srcFd.State = lfs.Missing
						continue
					}
					// check for zero sized files
					if srcFd.FileSize == 0 {
						log.Trace().
							Str("path", path).
							Msg("empty file")

						// file creted, modify meta if required and set as done
						err = fixMeta(dstDir, srcFd, dstFd)
						if err != nil {
							return nil, errors.Wrap(err, "error changing metadata")
						}
						srcFd.State = lfs.Skip
					}
				} else {
					log.Trace().
						Str("path", path).
						Msg("receiver fixing dir meta")
					// directory that exists, check meta only
					err = fixMeta(dstDir, srcFd, dstFd)
					if err != nil {
						return nil, errors.Wrap(err, "error changing metadata")
					}
					srcFd.State = lfs.Skip
				}
			}
			continue
		} else {
			// it does not exist on destination, check if it's a ditrectory
			if srcFd.IsDir {
				// create directory
				log.Debug().
					Str("path", path).
					Msg("creating directory")
				if _, err := os.Stat(path); os.IsNotExist(err) {
					// create one
					os.Mkdir(path, os.ModeDir)
				} else if err != nil {
					return nil, errors.Wrapf(err, "%s - unable to create directory", path)
				}
			} else {
				// set it as missing
				// check for zero sized files
				if srcFd.FileSize == 0 {
					log.Trace().
						Str("path", path).
						Msg("empty file")
					file, err := os.Create(path)
					if err != nil {
						return nil, errors.Wrapf(err, "%s - unable to create file", path)
					}
					file.Close()

					// TODO fix metadata on new empty file
					srcFd.State = lfs.Skip
					continue
				}
				srcFd.State = lfs.Missing
				// store!
				srcList[srcFd.Idx] = srcFd
				log.Debug().
					Str("sender path", srcFd.RelPath).
					Uint64("source file size", srcFd.FileSize).
					Time("source file modified", srcFd.Modified.UTC()).
					Msg("receiver MISS")
			}
		}
	}
	return diffMap, nil
}
