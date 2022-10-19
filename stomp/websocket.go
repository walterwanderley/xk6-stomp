package stomp

import (
	"context"
	"io"
	"log"
	"net/url"
	"time"

	"github.com/gorilla/websocket"
)

type wsConn struct {
	conn *websocket.Conn
}

func openWSConn(opts *Options, timeout time.Duration) (*wsConn, error) {
	u := url.URL{Scheme: opts.Protocol, Host: opts.Addr, Path: opts.Path}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	headers := make(map[string][]string)
	for k, v := range opts.Headers {
		headers[k] = []string{v}
	}
	dialer := *websocket.DefaultDialer
	conn, resp, err := dialer.DialContext(ctx, u.String(), headers)
	if err != nil {
		if err == websocket.ErrBadHandshake {
			b, _ := io.ReadAll(resp.Body)
			defer resp.Body.Close()
			log.Println("[xk6-stomp] error: ", resp.StatusCode, string(b))
		}
		return nil, err
	}
	return &wsConn{conn}, nil
}

func (w *wsConn) Read(p []byte) (int, error) {
	_, m, err := w.conn.ReadMessage()
	if err != nil {
		return 0, err
	}
	return copy(p, m), nil
}

func (w *wsConn) Write(p []byte) (int, error) {
	wr, err := w.conn.NextWriter(websocket.TextMessage)
	if err != nil {
		return 0, err
	}
	defer wr.Close()
	return wr.Write(p)
}

func (w *wsConn) Close() error {
	return w.conn.Close()
}
