package ws

import (
	"context"
	"io"
	"net"
	"time"

	"github.com/gorilla/websocket"
	"github.com/xjdrew/glog"
)

type Conn struct {
	*websocket.Conn
	pr     io.ReadCloser
	pw     io.WriteCloser
	ctx    context.Context
	cancel context.CancelFunc
}

func (c *Conn) Read(b []byte) (int, error) {
	return c.pr.Read(b)
}

func (c *Conn) Write(b []byte) (int, error) {
	if err := c.WriteMessage(websocket.BinaryMessage, b); err != nil {
		return 0, err
	}
	return len(b), nil
}

func (c *Conn) SetDeadline(t time.Time) error {
	if err := c.SetWriteDeadline(t); err != nil {
		return err
	}
	if err := c.SetReadDeadline(t); err != nil {
		return err
	}
	return nil
}

func NewConn(c *websocket.Conn) *Conn {
	ctx, cancel := context.WithCancel(context.Background())
	pr, pw := io.Pipe()
	wsc := Conn{
		Conn:   c,
		pw:     pw,
		pr:     pr,
		ctx:    ctx,
		cancel: cancel,
	}

	// receive routine
	go func() {
		for {
			_, rd, err := c.NextReader()
			if err != nil {
				select {
				case <-ctx.Done():
					pw.Close()
					return
				default:
				}

				if glog.V(1) {
					glog.Infof("websocket read failed. c:%v, err:%v", c, err)
				}
				pw.CloseWithError(err)
				return
			}
			if _, err := io.Copy(pw, rd); err != nil {
				select {
				case <-ctx.Done():
					pw.Close()
					return
				default:
				}
				pw.CloseWithError(err)
				return
			}
		}
	}()

	return &wsc
}

func Dial(host string) (net.Conn, error) {
	c, resp, err := websocket.DefaultDialer.DialContext(context.Background(), host, nil)
	if resp != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		return nil, err
	}

	return NewConn(c), nil
}
