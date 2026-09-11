package core

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const writeBufferTestTimeout = time.Second

type countingWriter struct {
	calls atomic.Int64
}

func (w *countingWriter) Write(p []byte) (int, error) {
	w.calls.Add(1)
	return len(p), nil
}

type racingErrorWriter struct {
	calls   atomic.Int64
	ready   chan struct{}
	entered chan struct{}
	release chan struct{}
	err     error
	once    sync.Once
}

func newRacingErrorWriter(err error) *racingErrorWriter {
	return &racingErrorWriter{
		ready:   make(chan struct{}),
		entered: make(chan struct{}),
		release: make(chan struct{}),
		err:     err,
	}
}

func (w *racingErrorWriter) Write(p []byte) (int, error) {
	if w.calls.Add(1) == 1 {
		close(w.ready)
		return len(p), nil
	}

	w.once.Do(func() { close(w.entered) })
	<-w.release
	return 0, w.err
}

func waitWriteBufferResult(t *testing.T, result <-chan error) error {
	t.Helper()

	select {
	case err := <-result:
		return err
	case <-time.After(writeBufferTestTimeout):
		t.Fatal("WriteToContext did not return")
		return nil
	}
}

func TestWriteBufferWriteToContextCancelWithoutWrite(t *testing.T) {
	for range 100 {
		ctx, cancel := context.WithCancel(context.Background())
		buffer := NewWriteBuffer(nil)
		started := make(chan struct{})
		result := make(chan error, 1)

		go func() {
			close(started)
			_, err := buffer.WriteToContext(ctx, io.Discard)
			result <- err
		}()

		<-started
		cancel()

		require.ErrorIs(t, waitWriteBufferResult(t, result), context.Canceled)
	}
}

func TestWriteBufferWriteToContextRejectsWriteAfterCancel(t *testing.T) {
	for range 100 {
		ctx, cancel := context.WithCancel(context.Background())
		writer := &countingWriter{}
		buffer := NewWriteBuffer(writer)
		result := make(chan error, 1)

		go func() {
			_, err := buffer.WriteToContext(ctx, writer)
			result <- err
		}()

		cancel()
		require.ErrorIs(t, waitWriteBufferResult(t, result), context.Canceled)

		n, err := buffer.Write([]byte("late"))
		require.Zero(t, n)
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, writer.calls.Load())
	}
}

func TestWriteBufferWriteErrorWinsConcurrentCancel(t *testing.T) {
	writeErr := errors.New("write failed")

	for range 100 {
		writer := newRacingErrorWriter(writeErr)
		buffer := NewWriteBuffer(nil)
		_, err := buffer.Write([]byte("buffered"))
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() {
			_, err := buffer.WriteToContext(ctx, writer)
			result <- err
		}()

		select {
		case <-writer.ready:
		case <-time.After(writeBufferTestTimeout):
			t.Fatal("initial buffered write did not complete")
		}

		writeResult := make(chan error, 1)
		go func() {
			_, err := buffer.Write([]byte("packet"))
			writeResult <- err
		}()

		select {
		case <-writer.entered:
		case <-time.After(writeBufferTestTimeout):
			t.Fatal("concurrent write did not start")
		}

		cancel()
		close(writer.release)

		require.ErrorIs(t, waitWriteBufferResult(t, writeResult), writeErr)
		require.ErrorIs(t, waitWriteBufferResult(t, result), writeErr)
	}
}

func TestWriteBufferWriteToContextAlreadyCanceled(t *testing.T) {
	for range 100 {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		buffer := NewWriteBuffer(nil)
		result := make(chan error, 1)
		go func() {
			_, err := buffer.WriteToContext(ctx, io.Discard)
			result <- err
		}()

		require.ErrorIs(t, waitWriteBufferResult(t, result), context.Canceled)
	}
}
