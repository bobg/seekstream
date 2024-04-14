// Package seekstream defines a file-like object populated from a streaming source of data.
// Reading the file may be done concurrently with downloading the stream,
// and will wait as necessary for the data to arrive.
package seekstream

import (
	"bufio"
	"context"
	"io"
	"os"
	"sync"
	"time"

	"github.com/bobg/errors"
)

type File struct {
	source io.Reader

	ctx context.Context

	cond        *sync.Cond // protects the following
	written     int
	offset      int64
	eof, closed bool
	tmpfile     string
	err         error
}

var _ io.ReadSeekCloser = &File{}

func New(ctx context.Context, r io.Reader) (*File, error) {
	tmpfile, err := os.CreateTemp("", "seekstream")
	if err != nil {
		return nil, errors.Wrap(err, "creating temporary file")
	}
	if err := tmpfile.Close(); err != nil {
		return nil, errors.Wrap(err, "closing temporary file")
	}

	result := &File{
		source:  r,
		cond:    sync.NewCond(&sync.Mutex{}),
		tmpfile: tmpfile.Name(),
	}

	go result.populate(ctx)

	return result, nil
}

type bytereader interface {
	io.ByteReader
	io.Reader
}

// Populate reads from the source and writes to the temporary file.
func (f *File) populate(ctx context.Context) {
	ch := make(chan byte, 32768)

	// This goroutine produces the bytes of the source on a channel.
	// Reading that channel,
	// we can multiplex with a ticker and context cancellation.
	go func() {
		defer close(ch)

		var br bytereader
		if r, ok := f.source.(bytereader); ok {
			br = r
		} else {
			br = bufio.NewReader(f.source)
		}

		for {
			b, err := br.ReadByte()
			if errors.Is(err, io.EOF) {
				return
			}
			if err != nil {
				f.cond.L.Lock()
				f.err = errors.Join(f.err, err)
				f.cond.L.Unlock()
				return
			}
			select {
			case ch <- b:
				// ok, do nothing

			case <-ctx.Done():
				return
			}
		}
	}()

	defer f.cond.Broadcast()

	var (
		buf    [32768]byte
		n      = 0 // counts how much of buf is filled
		ticker = time.NewTicker(time.Second)
	)

	defer ticker.Stop()

	// Consume the bytes of the source from the channel and write them to the temporary file.
	// Add bytes when the buffer is full or when the ticker fires.
	for {
		if n == len(buf) {
			if err := f.append(buf[:]); err != nil {
				f.cond.L.Lock()
				f.err = errors.Join(f.err, err)
				f.cond.L.Unlock()
				return
			}
			n = 0
		}
		select {
		case b, ok := <-ch:
			if !ok {
				err := f.append(buf[:n])
				f.cond.L.Lock()
				f.eof = true
				f.err = errors.Join(f.err, err)
				f.cond.L.Unlock()
				return
			}
			buf[n] = b
			n++

		case <-ticker.C:
			if n == 0 {
				continue
			}
			if err := f.append(buf[:n]); err != nil {
				f.cond.L.Lock()
				f.err = errors.Join(f.err, err)
				f.cond.L.Unlock()
				return
			}
			n = 0

		case <-ctx.Done():
			f.cond.L.Lock()
			f.err = errors.Join(f.err, ctx.Err())
			f.cond.L.Unlock()
			return
		}
	}
}

func (f *File) append(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}

	defer f.cond.Broadcast()

	ff, err := os.OpenFile(f.tmpfile, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		return errors.Wrapf(err, "opening %s for appending", f.tmpfile)
	}
	defer ff.Close()

	n, err := ff.Write(buf)

	f.cond.L.Lock()
	f.written += n
	f.cond.L.Unlock()

	return errors.Wrap(err, "appending to tmpfile")
}

func (f *File) Read(p []byte) (int, error) {
	f.cond.L.Lock()
	defer f.cond.L.Unlock()

	for f.waitToRead(p) {
		f.cond.Wait()
	}

	ff, err := os.Open(f.tmpfile)
	if err != nil {
		return 0, errors.Wrap(err, "opening temporary file for read")
	}
	defer ff.Close()

	n, err := ff.ReadAt(p, f.offset)
	f.offset += int64(n)

	err = errors.Join(err, f.err)

	return n, errors.Wrap(err, "reading from temporary file")
}

func (f *File) Seek(offset int64, whence int) (int64, error) {
	f.cond.L.Lock()
	defer f.cond.L.Unlock()

	for f.waitToSeek(offset, whence) {
		f.cond.Wait()
	}

	switch whence {
	case io.SeekStart:
		f.offset = offset
	case io.SeekCurrent:
		f.offset += offset
	case io.SeekEnd:
		f.offset = int64(f.written) + offset
	}

	return f.offset, f.err
}

func (f *File) Close() error {
	f.cond.L.Lock()
	defer f.cond.L.Unlock()

	if f.closed {
		return nil
	}
	f.closed = true
	return os.Remove(f.tmpfile)
}

// Precondition: f.cond.L is locked.
func (f *File) waitToRead(p []byte) bool {
	if f.eof {
		return false
	}
	return f.offset > int64(f.written-len(p))
}

// Precondition: f.cond.L is locked.
func (f *File) waitToSeek(offset int64, whence int) bool {
	if f.eof {
		return false
	}
	switch whence {
	case io.SeekStart:
		return offset > int64(f.written)
	case io.SeekCurrent:
		return f.offset+offset > int64(f.written)
	case io.SeekEnd:
		return offset > 0
	}
	return false
}
