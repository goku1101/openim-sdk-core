package interaction

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

func TestDecodedMessageLimit(t *testing.T) {
	for _, size := range []int{31, 32, 33} {
		decoded, err := readDecompressedMessage(bytes.NewReader(bytes.Repeat([]byte("a"), size)), 32)
		if size > 32 {
			if !errors.Is(err, ErrDecompressedMessageTooLarge) || decoded != nil {
				t.Fatalf("oversize exposed: len=%d err=%v", len(decoded), err)
			}
		} else if err != nil || len(decoded) != size {
			t.Fatalf("size=%d len=%d err=%v", size, len(decoded), err)
		}
	}
}

type oversizedDecodedConn struct {
	LongConn
	payload []byte
	reads   int
	closed  bool
}

func (c *oversizedDecodedConn) SetReadLimit(limit int64)            {}
func (c *oversizedDecodedConn) SetReadDeadline(time.Duration) error { return nil }
func (c *oversizedDecodedConn) ReadMessage() (int, []byte, error) {
	c.reads++
	return MessageBinary, c.payload, nil
}
func (c *oversizedDecodedConn) Close() error { c.closed = true; return nil }

type repeatedZeroReader struct{}

func (repeatedZeroReader) Read(p []byte) (int, error) { clear(p); return len(p), nil }

func TestOversizedGzipStopsReaderWithoutReconnect(t *testing.T) {
	if maxDecompressedMessageSize != 128<<20 {
		t.Fatal("unexpected decoded limit")
	}
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := io.CopyN(writer, repeatedZeroReader{}, maxDecompressedMessageSize+1); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	conn := &oversizedDecodedConn{payload: compressed.Bytes()}
	ctx := context.Background()
	mgr := &LongConnMgr{ctx: ctx, conn: conn, connStatus: Connected, sub: newSubscription(), IsCompression: true, compressor: NewGzipCompressor()}
	mgr.readPump(ctx)
	if conn.reads != 1 || !conn.closed || !errors.Is(mgr.getClosedErr(), ErrDecompressedMessageTooLarge) {
		t.Fatalf("reads=%d closed=%v err=%v", conn.reads, conn.closed, mgr.getClosedErr())
	}
	// A pooled reader used on an oversize message must still decode the next session.
	small, err := NewGzipCompressor().Compress([]byte("next-session"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := NewGzipCompressor().DecompressWithPool(small)
	if err != nil || string(got) != "next-session" {
		t.Fatalf("pooled reader invalid: %v", err)
	}
}
