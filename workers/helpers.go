package workers

import (
	"time"

	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
	"github.com/zgub/pexync/core"
)

func sendWithTimeout(pkt *core.Message, dst chan<- *core.Message) error {
	timeoutValue := viper.GetDuration("timeout")
	timeout := time.After(timeoutValue)
	select {
	case dst <- pkt:
		return nil
	case <-timeout:
		log.Trace().Msg("timeout")
		return core.Timeout
	}
}

func recvWithTimeout(src <-chan *core.Message) (*core.Message, error) {
	timeoutValue := viper.GetDuration("timeout")
	timeout := time.After(timeoutValue)
	var pkt *core.Message

	select {
	case pkt = <-src:
		return pkt, nil
	case <-timeout:
		return nil, core.Timeout
	}
}
