package seekstream

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
)

type chanReader struct {
	cond    *sync.Cond // Broadcasts when reader is blocked and when a read completes. Its lock protects the following.
	ch      <-chan []byte
	buf     []byte
	blocked bool
}

func (r *chanReader) Read(p []byte) (int, error) {
	var n int

	r.cond.L.Lock()
	defer func() {
		r.blocked = false
		r.cond.Broadcast()
		r.cond.L.Unlock()
	}()

	for {
		if len(r.buf) > 0 {
			n = copy(p, r.buf)
			r.buf = r.buf[n:]
			p = p[n:]
		}
		if len(p) == 0 {
			return n, nil
		}

		var (
			buf []byte
			ok  bool
		)

		select {
		case buf, ok = <-r.ch:
			if !ok {
				return n, io.EOF
			}

		default:
			r.blocked = true
			r.cond.Broadcast()

			r.cond.L.Unlock()
			buf, ok = <-r.ch
			r.cond.L.Lock()

			if !ok {
				return n, io.EOF
			}
		}

		r.buf = append(r.buf, buf...)
	}
}

func TestFile(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		ch   = make(chan []byte)
		mu   = &sync.Mutex{}
		cond = sync.NewCond(mu)
		chr  = &chanReader{cond: cond, ch: ch}
	)

	f, err := New(ctx, chr)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	t.Log("waiting for reader to block")
	mu.Lock()
	for !chr.blocked {
		cond.Wait()
	}
	mu.Unlock()

	t.Log("writing 9 bytes to channel")
	ch <- []byte("123456789")

	t.Log("reading 4 bytes")
	var got [4]byte
	n, err := f.Read(got[:])
	if err != nil {
		t.Fatal(err)
	}
	if n != 4 {
		t.Errorf("got %d, want 4", n)
	}
	if string(got[:n]) != "1234" {
		t.Errorf("got %s, want 1234", string(got[:n]))
	}

	t.Log("reading 4 more bytes")
	n, err = f.Read(got[:])
	if err != nil {
		t.Fatal(err)
	}
	if n != 4 {
		t.Errorf("got %d, want 4", n)
	}
	if string(got[:n]) != "5678" {
		t.Errorf("got %s, want 5678", string(got[:n]))
	}

	t.Log("launching goroutine to read three more bytes")
	done := make(chan struct{})
	go func() {
		n, err = f.Read(got[:])
		close(done)
	}()

	t.Log("waiting for reader to block")
	mu.Lock()
	for !chr.blocked {
		cond.Wait()
	}
	mu.Unlock()

	t.Log("writing final 2 bytes to channel")
	ch <- []byte("xy")
	close(ch)

	t.Log("waiting for goroutine to complete")
	<-done

	if !errors.Is(err, io.EOF) {
		t.Errorf("got error %v, want io.EOF", err)
	}
	if n != 3 {
		t.Errorf("got %d, want 3", n)
	}
	if string(got[:n]) != "9xy" {
		t.Errorf("got %s, want 9xy", string(got[:n]))
	}
}
