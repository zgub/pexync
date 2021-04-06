package workers

import (
	"time"

	"github.com/spf13/viper"
	"github.com/zgub/pexync/core"
)

func sendWithTimeout(pkt *core.Message, dst chan<- *core.Message) core.Error {
	timeoutValue := viper.GetDuration("timeout")
	timeout := time.After(timeoutValue)
	select {
	case dst <- pkt:
		return core.NoError
	case <-timeout:
		return core.Timeout
	}
}

func recvWithTimeout(src <-chan *core.Message) (*core.Message, core.Error) {
	timeoutValue := viper.GetDuration("timeout")
	timeout := time.After(timeoutValue)
	var pkt *core.Message

	select {
	case pkt = <-src:
		return pkt, core.NoError
	case <-timeout:
		return nil, core.Timeout
	}
}
