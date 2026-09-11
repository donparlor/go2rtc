package mpegts

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/AlexxIT/go2rtc/internal/streams"
	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/pion/rtp"
	"github.com/stretchr/testify/require"
)

const mpegTSTestTimeout = time.Second

type h264Producer struct {
	core.Connection
}

func newH264Producer() *h264Producer {
	codec := &core.Codec{
		Name:        core.CodecH264,
		ClockRate:   90000,
		PayloadType: 96,
	}
	return &h264Producer{Connection: core.Connection{
		Medias: []*core.Media{{
			Kind:      core.KindVideo,
			Direction: core.DirectionRecvonly,
			Codecs:    []*core.Codec{codec},
		}},
	}}
}

func (p *h264Producer) Start() error {
	return nil
}

type testResponseWriter struct {
	header     http.Header
	mu         sync.Mutex
	body       bytes.Buffer
	firstWrite chan struct{}
	once       sync.Once
}

func newTestResponseWriter() *testResponseWriter {
	return &testResponseWriter{
		header:     make(http.Header),
		firstWrite: make(chan struct{}),
	}
}

func (w *testResponseWriter) Header() http.Header {
	return w.header
}

func (w *testResponseWriter) WriteHeader(int) {}

func (w *testResponseWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.once.Do(func() { close(w.firstWrite) })
	return w.body.Write(p)
}

func (w *testResponseWriter) Flush() {}

func (w *testResponseWriter) Len() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.body.Len()
}

type mpegTSFixture struct {
	stream   *streams.Stream
	producer *h264Producer
	writer   *testResponseWriter
	cancel   context.CancelFunc
	done     chan struct{}
}

func newMPEGTSFixture(t *testing.T) *mpegTSFixture {
	t.Helper()

	name := t.Name()
	stream := streams.New(name)
	producer := newH264Producer()
	stream.AddProducer(producer)

	ctx, cancel := context.WithCancel(context.Background())
	writer := newTestResponseWriter()
	request := httptest.NewRequest(
		http.MethodGet,
		"/api/stream.ts?src="+url.QueryEscape(name),
		nil,
	).WithContext(ctx)
	done := make(chan struct{})

	go func() {
		outputMpegTS(writer, request)
		close(done)
	}()

	select {
	case <-writer.firstWrite:
	case <-time.After(mpegTSTestTimeout):
		t.Fatal("MPEG-TS handler did not write its header")
	}

	fixture := &mpegTSFixture{
		stream:   stream,
		producer: producer,
		writer:   writer,
		cancel:   cancel,
		done:     done,
	}
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(mpegTSTestTimeout):
			t.Error("MPEG-TS handler did not stop during cleanup")
		}
		streams.Delete(name)
	})

	require.Equal(t, 1, streamConsumerCount(t, stream))
	require.Len(t, producer.Receivers, 1)
	require.Len(t, producer.Receivers[0].Senders(), 1)

	return fixture
}

func streamConsumerCount(t *testing.T, stream *streams.Stream) int {
	t.Helper()

	data, err := json.Marshal(stream)
	require.NoError(t, err)

	var state struct {
		Consumers []json.RawMessage `json:"consumers"`
	}
	require.NoError(t, json.Unmarshal(data, &state))
	return len(state.Consumers)
}

func waitMPEGTSHandler(t *testing.T, done <-chan struct{}) {
	t.Helper()

	select {
	case <-done:
	case <-time.After(mpegTSTestTimeout):
		t.Fatal("MPEG-TS handler did not return")
	}
}

func requireMPEGTSCleaned(t *testing.T, fixture *mpegTSFixture) {
	t.Helper()

	waitMPEGTSHandler(t, fixture.done)
	require.Zero(t, streamConsumerCount(t, fixture.stream))
	require.Empty(t, fixture.producer.Receivers[0].Senders())
}

func TestOutputMPEGTSKeepsSilentConnectedConsumer(t *testing.T) {
	fixture := newMPEGTSFixture(t)

	select {
	case <-fixture.done:
		t.Fatal("connected silent consumer was removed")
	case <-time.After(50 * time.Millisecond):
	}

	require.Equal(t, 1, streamConsumerCount(t, fixture.stream))
	require.Len(t, fixture.producer.Receivers[0].Senders(), 1)

	fixture.cancel()
	requireMPEGTSCleaned(t, fixture)
}

func TestOutputMPEGTSCleansSilentDisconnectedConsumer(t *testing.T) {
	fixture := newMPEGTSFixture(t)

	fixture.cancel()
	requireMPEGTSCleaned(t, fixture)
}

func TestOutputMPEGTSDisconnectDoesNotAccumulate(t *testing.T) {
	name := t.Name()
	stream := streams.New(name)
	producer := newH264Producer()
	stream.AddProducer(producer)
	t.Cleanup(func() { streams.Delete(name) })

	for range 100 {
		ctx, cancel := context.WithCancel(context.Background())
		writer := newTestResponseWriter()
		request := httptest.NewRequest(
			http.MethodGet,
			"/api/stream.ts?src="+url.QueryEscape(name),
			nil,
		).WithContext(ctx)
		done := make(chan struct{})

		go func() {
			outputMpegTS(writer, request)
			close(done)
		}()

		select {
		case <-writer.firstWrite:
		case <-time.After(mpegTSTestTimeout):
			t.Fatal("MPEG-TS handler did not write its header")
		}

		require.Equal(t, 1, streamConsumerCount(t, stream))
		require.Len(t, producer.Receivers, 1)
		require.Len(t, producer.Receivers[0].Senders(), 1)

		cancel()
		waitMPEGTSHandler(t, done)

		require.Zero(t, streamConsumerCount(t, stream))
		require.Empty(t, producer.Receivers[0].Senders())
	}
}

func TestOutputMPEGTSCleansAfterProducerBecomesSilent(t *testing.T) {
	fixture := newMPEGTSFixture(t)
	receiver := fixture.producer.Receivers[0]
	headerSize := fixture.writer.Len()

	for i := range 3 {
		receiver.Input(&rtp.Packet{
			Header: rtp.Header{
				Version:        2,
				Marker:         true,
				PayloadType:    96,
				SequenceNumber: uint16(i),
				Timestamp:      uint32(i+1) * 90000,
			},
			Payload: []byte{0x65, 0x88, 0x84},
		})
	}

	require.Eventually(t, func() bool {
		return fixture.writer.Len() > headerSize
	}, mpegTSTestTimeout, time.Millisecond)
	require.Equal(t, 3, receiver.Packets)

	// Stop producing RTP, then disconnect the HTTP client while the stream is silent.
	fixture.cancel()
	requireMPEGTSCleaned(t, fixture)
}
