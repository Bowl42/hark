package sherpa

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Bowl42/hark"
	"github.com/coder/websocket"
)

// An idle connection in the pool has gone stale (for example, the model server restarted): retry on a new connection, and the call still succeeds.
func TestRetriesStaleConnection(t *testing.T) {
	var mu sync.Mutex
	conns := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		mu.Lock()
		conns++
		mu.Unlock()
		c.SetReadLimit(8 << 20)
		ctx := context.Background()
		if _, _, err := c.Read(ctx); err != nil {
			return
		}
		b, _ := json.Marshal(map[string]any{"text": "好"})
		c.Write(ctx, websocket.MessageText, b)
		// each connection serves only once and then disconnects: the pool is left with only stale connections
		c.Close(websocket.StatusGoingAway, "restart")
	}))
	defer ts.Close()
	o, _ := New(Config{URL: "ws" + strings.TrimPrefix(ts.URL, "http")})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := 0; i < 3; i++ {
		text, err := o.Recognize(ctx, hark.Request{}, make([]byte, 3200))
		if err != nil || text != "好" {
			t.Fatalf("call %d: %q %v", i, text, err)
		}
		time.Sleep(50 * time.Millisecond) // let the server's disconnect arrive first
	}
	mu.Lock()
	defer mu.Unlock()
	if conns != 3 {
		t.Fatalf("want 3 connections (1 + 2 retries), got %d", conns)
	}
}
