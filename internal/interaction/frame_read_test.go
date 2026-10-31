//go:build !js

package interaction

import (
	"bytes"
	"fmt"
	"github.com/gorilla/websocket"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestNegotiatedReadBudgetAcceptsLargeMessages(t *testing.T) {
	for _, size := range []int{900 << 10, 2 << 20, 16 << 20, 32 << 20} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			payload := bytes.Repeat([]byte{0x42}, size)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				ws, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
				if err != nil {
					return
				}
				defer ws.Close()
				ws.WriteMessage(websocket.BinaryMessage, payload)
			}))
			defer server.Close()
			conn := NewWebSocket(0)
			if _, err := conn.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil); err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			conn.SetReadLimit(maxMessageSize)
			_, got, err := conn.ReadMessage()
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, payload) {
				t.Fatal("message corrupted")
			}
		})
	}
}
