package seekstream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
)

type testReader struct {
	cond    *sync.Cond // Broadcasts when reader is blocked and when a read completes. Its lock protects the following.
	buf     *bytes.Buffer
	blocked bool
	closed  bool
}

func (r *testReader) Read(p []byte) (int, error) {
	r.cond.L.Lock()
	defer r.cond.L.Unlock()
	defer r.cond.Broadcast()

	if r.closed || len(p) <= r.buf.Len() {
		return r.buf.Read(p)
	}

	r.blocked = true
	r.cond.Broadcast()

	for !r.closed && len(p) > r.buf.Len() {
		r.cond.Wait()
	}

	return r.buf.Read(p)
}

func (r *testReader) ReadByte() (byte, error) {
	r.cond.L.Lock()
	defer r.cond.L.Unlock()
	defer r.cond.Broadcast()

	if r.closed || r.buf.Len() > 0 {
		return r.buf.ReadByte()
	}

	r.blocked = true
	r.cond.Broadcast()

	for !r.closed && r.buf.Len() == 0 {
		r.cond.Wait()
	}

	return r.buf.ReadByte()
}

func (r *testReader) Write(p []byte) (int, error) {
	r.cond.L.Lock()
	defer r.cond.L.Unlock()
	defer r.cond.Broadcast()

	if r.closed {
		return 0, io.ErrClosedPipe
	}

	return r.buf.Write(p)
}

func (r *testReader) Close() error {
	r.cond.L.Lock()
	defer r.cond.L.Unlock()
	defer r.cond.Broadcast()

	r.closed = true
	return nil
}

func TestFile(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		mu   = &sync.Mutex{}
		cond = sync.NewCond(mu)
		tr   = &testReader{cond: cond, buf: new(bytes.Buffer)}
	)

	f, err := New(ctx, tr)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	mu.Lock()
	for !tr.blocked {
		cond.Wait()
	}
	mu.Unlock()

	t.Log("writing 9 bytes to testReader")
	fmt.Fprint(tr, "123456789")

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
	for !tr.blocked {
		cond.Wait()
	}
	mu.Unlock()

	t.Log("writing final 2 bytes to testReader")
	fmt.Fprint(tr, "xy")
	tr.Close()

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
