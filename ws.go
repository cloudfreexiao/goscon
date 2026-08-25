package main

import (
	"bytes"
	"io"
	"net"
	"strings"
	"time"

	"github.com/gobwas/ws"
	"github.com/xjdrew/glog"
)

// wsAddr 表示从反向代理透传的真实客户端地址
type wsAddr struct {
	network string
	addr    string
}

func (a *wsAddr) Network() string { return a.network }
func (a *wsAddr) String() string  { return a.addr }

type wsConn struct {
	*net.TCPConn
	readTimeout time.Duration
	// realIPHeader 指定从哪个 HTTP 头解析真实客户端 IP，为空表示不信任代理头
	realIPHeader string

	upgraded bool
	realAddr net.Addr
	length   int64
	offset   int64
	mask     [4]byte
}

// setRealAddr 解析反向代理透传的真实客户端 IP，并记录为 realAddr。
// 支持 X-Forwarded-For 形式（可能为 "client, proxy1, proxy2"，取第一个），
// 也支持已带端口的形式；仅含 IP 时用底层连接端口补齐，维持 ip:port 格式。
func (conn *wsConn) setRealAddr(value string) {
	ip := value
	if i := strings.IndexByte(ip, ','); i >= 0 {
		ip = ip[:i]
	}
	ip = strings.TrimSpace(ip)
	if ip == "" {
		return
	}

	if _, _, err := net.SplitHostPort(ip); err != nil {
		// 不含端口，用底层连接的端口补齐
		if _, port, e := net.SplitHostPort(conn.TCPConn.RemoteAddr().String()); e == nil {
			ip = net.JoinHostPort(ip, port)
		}
	}
	conn.realAddr = &wsAddr{network: "tcp", addr: ip}
}

// RemoteAddr 优先返回反向代理透传的真实客户端地址，否则返回底层 TCP 地址
func (conn *wsConn) RemoteAddr() net.Addr {
	if conn.realAddr != nil {
		return conn.realAddr
	}
	return conn.TCPConn.RemoteAddr()
}

func readMaskData(conn *wsConn, buf []byte, remain int64) (n int, err error) {
	sz := int64(len(buf))
	if sz > remain {
		sz = remain
	}

	b := buf[:sz]
	n, err = io.ReadFull(conn.TCPConn, b)
	if err != nil {
		return
	}

	ws.Cipher(b, conn.mask, int(conn.offset))
	conn.offset += sz
	return
}

func (conn *wsConn) Read(b []byte) (int, error) {
	if conn.readTimeout > 0 {
		conn.SetReadDeadline(time.Now().Add(conn.readTimeout))
	}

	if !conn.upgraded {
		u := ws.Upgrader{}
		if conn.realIPHeader != "" {
			headerKey := []byte(conn.realIPHeader)
			u.OnHeader = func(key, value []byte) error {
				if bytes.EqualFold(key, headerKey) {
					conn.setRealAddr(string(value))
				}
				return nil
			}
		}

		_, err := u.Upgrade(conn.TCPConn)
		if err != nil {
			return 0, err
		}
		conn.upgraded = true

		if glog.V(1) {
			glog.Infof("upgrade websocket connection: addr=%s", conn.RemoteAddr())
		}
	}

	remain := conn.length - conn.offset
	if remain > 0 {
		return readMaskData(conn, b, remain)
	}

	for {
		header, err := ws.ReadHeader(conn.TCPConn)
		if err != nil {
			return 0, err
		}

		switch header.OpCode {
		case ws.OpClose:
			return 0, io.EOF
		case ws.OpPing:
			payload := make([]byte, header.Length)
			if _, err := io.ReadFull(conn.TCPConn, payload); err != nil {
				return 0, err
			}
			if header.Masked {
				ws.Cipher(payload, header.Mask, 0)
			}
			if err := ws.WriteFrame(conn.TCPConn, ws.NewPongFrame(payload)); err != nil {
				return 0, err
			}
			continue
		default:
			conn.length = header.Length
			conn.offset = 0
			conn.mask = header.Mask
			return readMaskData(conn, b, header.Length)
		}
	}
}

func (conn *wsConn) Write(b []byte) (int, error) {
	f := ws.NewBinaryFrame(b)
	err := ws.WriteFrame(conn.TCPConn, f)
	if err != nil {
		return 0, err
	}
	return len(b), nil
}

// WSListener .
type WSListener struct {
	net.Listener
}

// Accept .
func (l *WSListener) Accept() (conn net.Conn, err error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return
	}

	keepalive := configItemBool("websocket_option.keepalive")
	keepaliveInterval := configItemTime("websocket_option.keepalive_interval")
	keepaliveCount := configItemInt("websocket_option.keepalive_count")
	readTimeout := configItemTime("websocket_option.read_timeout")
	realIPHeader := configItemString("websocket_option.real_ip_header")

	t := c.(*net.TCPConn)
	t.SetKeepAliveConfig(net.KeepAliveConfig{
		Enable:   keepalive,
		Idle:     keepaliveInterval,
		Interval: keepaliveInterval,
		Count:    keepaliveCount,
	})
	// t.SetLinger(0)

	if glog.V(1) {
		glog.Infof("accept new websocket connection: addr=%s", c.RemoteAddr())
	}

	conn = &wsConn{TCPConn: t, readTimeout: readTimeout, realIPHeader: realIPHeader}
	return
}

// NewWSListener creates a new WSListener
func NewWSListener(laddr string) (*WSListener, error) {
	ln, err := net.Listen("tcp", laddr)
	if err != nil {
		return nil, err
	}
	return &WSListener{ln}, nil
}
