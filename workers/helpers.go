package workers

import (
	"os"
	"time"

	"github.com/pkg/errors"

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
