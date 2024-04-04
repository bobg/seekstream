// Package seekstream defines a file-like object populated from a streaming source of data.
// Reading the file may be done concurrently with downloading the stream,
// and will wait as necessary for the data to arrive.
package seekstream

import (
	"context"
	"io"
	"os"
	"sync"

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

	go func() {
		a := appender{
			file: result,
		}
		_, err := io.Copy(a, cancelReader{ctx: ctx, r: r})
		result.cond.L.Lock()
		result.err = err
		result.eof = true
		result.cond.Broadcast()
		result.cond.L.Unlock()
	}()

	return result, nil
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

type appender struct {
	file *File
}

func (a appender) Write(p []byte) (int, error) {
	a.file.cond.L.Lock()
	defer a.file.cond.L.Unlock()

	f, err := os.OpenFile(a.file.tmpfile, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		return 0, errors.Wrap(err, "opening temporary file for append")
	}
	defer f.Close()

	n, err := f.Write(p)
	a.file.written += n
	a.file.cond.Broadcast()
	return n, errors.Wrap(err, "writing to temporary file")
}

type cancelReader struct {
	ctx context.Context
	r   io.Reader
}

func (r cancelReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}
