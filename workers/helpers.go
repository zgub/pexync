package workers

import (
	"errors"
	"time"

	"github.com/spf13/viper"
	"github.com/zgub/pexync/core"
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
