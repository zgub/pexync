package workers

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"

	"github.com/spf13/viper"
	"github.com/zgub/pexync/core"
	"github.com/zgub/pexync/lfs"
)

func sendWithTimeout(msg *core.Message, dst chan<- *core.Message) error {
	timeoutValue := viper.GetDuration("timeout")
	timeout := time.After(timeoutValue)
	//spew.Dump(msg)
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
	p := filepath.Join(dstDir, srcFd.RelPath)
	// check permissions and ownership
	if srcFd.Modified != dstFd.Modified {
		err := os.Chtimes(p, srcFd.Modified, srcFd.Modified)
		if err != nil {
			return errors.Wrapf(err, "%s - unable to modify mtime", p)
		}
	}
	if srcFd.Mode.Perm() != dstFd.Mode.Perm() {
		err := os.Chmod(p, srcFd.Mode.Perm())
		if err != nil {
			return errors.Wrapf(err, "%s - unable to modify permissions", p)
		}
	}
	if srcFd.Gid != dstFd.Gid || srcFd.Uid != dstFd.Uid {
		err := os.Chown(p, int(srcFd.Uid), int(srcFd.Gid))
		if err != nil {
			return errors.Wrapf(err, "%s - unable to modify ownership", p)
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

	deBuf := new(bytes.Buffer)
	teer := io.TeeReader(gz, deBuf)
	fmt.Println("teereading")
	spew.Dump(deBuf.Bytes())

	n, err := io.Copy(buf, teer)
	if err != nil {
		return nil, err
	}
	fmt.Printf("decompressed %d bytes\n", n)
	if err = gz.Close(); err != nil {
		return nil, err
	}
	spew.Dump(buf.Bytes())
	return buf, nil
}

func (w *HttpSender) sendJson(url string, msg *core.Message) (*core.Message, error) {
	fmt.Println("*** MSG ***")
	spew.Dump(msg)
	j, err := json.Marshal(msg)
	if err != nil {
		return nil, errors.Wrap(err, "json marshal failed")
	}

	fmt.Printf("json: %s\n", string(j))

	buf, err := compress(j)
	if err != nil {
		return nil, errors.Wrap(err, "error compressing data")
	}
	fmt.Printf("compressed data: %d bytes\n", buf.Len())

	req, err := http.NewRequestWithContext(w.ctx, http.MethodPost, url, buf)
	//req.Header.Set("X-Custom-Header", "myvalue")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "PeXync-client-mode")
	req.Header.Set("Accept-Encoding", "gzip")
	req.Header.Set("Content-Encoding", "gzip")
	if err != nil {
		return nil, errors.Wrap(err, "error creating http request")
	}

	spew.Dump(req)

	resp, err := w.client.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "error connecting server")
	}
	defer resp.Body.Close()

	log.Trace().
		Str("status:", resp.Status).
		Msg("http response")

	if resp.StatusCode != http.StatusOK {
		err = errors.New(resp.Status)
		return nil, err
	}

	buf, err = decompress(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "unable to decompress server response")
	}

	msg = &core.Message{}
	err = json.Unmarshal(buf.Bytes(), msg)
	if err != nil {
		return nil, errors.Wrap(err, "error reading server response")
	}

	return msg, nil
}

func (w *HttpSender) sendFileDesc(url string, data []byte) (*core.Message, error) {

	buf, err := compress(data)
	if err != nil {
		return nil, errors.Wrap(err, "error compressing data")
	}

	req, err := http.NewRequestWithContext(w.ctx, http.MethodPost, url, buf)
	//req.Header.Set("X-Custom-Header", "myvalue")
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("User-Agent", "PeXync-client-mode")
	req.Header.Set("Accept-Encoding", "gzip")
	req.Header.Set("Content-Encoding", "gzip")
	if err != nil {
		return nil, errors.Wrap(err, "error creating http request")
	}

	resp, err := w.client.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "error connecting server")
	}
	defer resp.Body.Close()

	log.Trace().
		Str("status:", resp.Status).
		Msg("http response")

	buf, err = decompress(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "error reading server response")
	}

	msg := &core.Message{}
	err = json.Unmarshal(buf.Bytes(), msg)
	if err != nil {
		return nil, errors.Wrap(err, "error reading server response")
	}

	return msg, nil
}

// this is not optimal, well... I would refactor the whole worker / sender / receiver design for possible next release
func (w *HttpSender) dataSender() error {
	for {
		select {
		case <-w.ctx.Done():
			log.Debug().
				Msgf("http client worker - closing, context done")
		case msg := <-w.receiver:
			// if FIN was send, don't send it to the standalone process
			// but stop
			if msg.GetFlag() == core.FIN {
				return nil
			}
			//spew.Dump(msg)
			url := w.url.String() + "/data"
			data, err := msg.GetDataDesc().Serialize()
			if err != nil {
				return errors.Wrap(err, "failed to serialize data")
			}
			resp, err := w.sendFileDesc(url, data)
			if err != nil {
				return errors.Wrap(err, "failed to send data")
			}
			switch resp.GetFlag() {
			case core.ACK:
				log.Trace().
					Msg("http client worker - received ack")
			default:
				log.Error().
					Msgf("http client worker - receives %s", resp.GetFlag().String())
			}
			//spew.Dump(resp)

		}
	}
}
