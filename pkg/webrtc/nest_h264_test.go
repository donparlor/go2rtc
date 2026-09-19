package webrtc

import (
	"errors"
	"sync"
	"testing"
	"time"

	pion "github.com/pion/webrtc/v4"
)

func newNestH264TestConn(t *testing.T) *Conn {
	t.Helper()

	pc, err := pion.NewPeerConnection(pion.Configuration{})
	if err != nil {
		t.Fatalf("NewPeerConnection: %v", err)
	}

	c := NewConn(pc)
	c.FormatName = "nest/webrtc"

	t.Cleanup(func() {
		_ = c.Close()
	})

	return c
}

func TestNestH264TimeoutReasonInitial(t *testing.T) {
	var c Conn

	started := 12 * time.Second

	tests := []struct {
		name string
		now  time.Duration
		want string
	}{
		{
			name: "before deadline",
			now:  started + nestH264Timeout - time.Nanosecond,
			want: "",
		},
		{
			name: "at deadline",
			now:  started + nestH264Timeout,
			want: "no_video_initial",
		},
		{
			name: "after deadline",
			now:  started + nestH264Timeout + time.Second,
			want: "no_video_initial",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := c.nestH264TimeoutReason(started, tt.now)

			if got != tt.want {
				t.Fatalf(
					"reason=%q; want=%q",
					got,
					tt.want,
				)
			}
		})
	}
}

func TestNestH264TimeoutReasonAfterVideo(t *testing.T) {
	var c Conn

	c.nestVideoSeen.Store(true)

	started := 5 * time.Second
	now := 90 * time.Second

	tests := []struct {
		name string
		last time.Duration
		want string
	}{
		{
			name: "recent video",
			last: now - 3*time.Second,
			want: "",
		},
		{
			name: "before deadline",
			last: now - nestH264Timeout + time.Nanosecond,
			want: "",
		},
		{
			name: "at deadline",
			last: now - nestH264Timeout,
			want: "stalled",
		},
		{
			name: "after deadline",
			last: now - nestH264Timeout - time.Second,
			want: "stalled",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c.nestLastVideoNS.Store(int64(tt.last))

			got := c.nestH264TimeoutReason(started, now)

			if got != tt.want {
				t.Fatalf(
					"reason=%q; want=%q",
					got,
					tt.want,
				)
			}
		})
	}
}

func TestNestH264WatchdogClosesStalledConnection(t *testing.T) {
	c := newNestH264TestConn(t)

	// Simulate a video track whose last real RTP predates
	// the timeout. The watchdog itself still uses its real ticker.
	c.nestVideoSeen.Store(true)
	c.nestLastVideoNS.Store(
		int64(time.Since(c.nestEpoch) - nestH264Timeout),
	)

	startReturned := make(chan struct{})

	go func() {
		defer close(startReturned)
		_ = c.Start()
	}()

	c.startNestH264Watchdog()

	select {
	case <-c.done:
	// Expected: watchdog triggered Conn.Close().
	case <-time.After(3 * time.Second):
		t.Fatal("watchdog did not close stalled connection")
	}

	select {
	case <-startReturned:
	// Expected: the existing producer reconnect path can proceed.
	case <-time.After(3 * time.Second):
		t.Fatal("Conn.Start remained blocked after watchdog Close")
	}
}

func TestNestH264WatchdogNoInitialVideo(t *testing.T) {
	c := newNestH264TestConn(t)

	// Simulate Connected having occurred more than 30 seconds ago,
	// with no valid H264 packet ever received.
	started := time.Since(c.nestEpoch) -
		nestH264Timeout - time.Second

	startReturned := make(chan struct{})

	go func() {
		defer close(startReturned)
		_ = c.Start()
	}()

	c.startNestH264WatchdogAt(started)

	select {
	case <-c.done:
		// Expected: no initial video triggers recovery.
	case <-time.After(3 * time.Second):
		t.Fatal("watchdog did not close connection without initial H264")
	}

	select {
	case <-startReturned:
		// Expected: Conn.Start() is unblocked.
	case <-time.After(3 * time.Second):
		t.Fatal("Conn.Start remained blocked after initial-video timeout")
	}
}

func TestNestH264WatchdogDoesNotKillLiveVideo(t *testing.T) {
	c := newNestH264TestConn(t)

	// The initial deadline has passed, but valid video has arrived.
	c.nestVideoSeen.Store(true)
	c.nestLastVideoNS.Store(int64(time.Since(c.nestEpoch)))

	started := time.Since(c.nestEpoch) -
		nestH264Timeout - time.Second

	c.startNestH264WatchdogAt(started)

	select {
	case <-c.done:
		t.Fatal("watchdog closed a connection with recent H264")
	case <-time.After(1200 * time.Millisecond):
		// Expected: initial deadline must not override recent video.
	}

	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestNestH264ReadFailureClosesConnection(t *testing.T) {
	c := newNestH264TestConn(t)

	c.failNestH264("read_error", errors.New("synthetic read failure"))

	select {
	case <-c.done:
	// Expected.
	default:
		t.Fatal("read failure did not close connection")
	}

	if state := c.pc.ConnectionState(); state != pion.PeerConnectionStateClosed {
		t.Fatalf("PeerConnection state=%v; want closed", state)
	}
}

func TestNestH264ConcurrentRecoveryAndClose(t *testing.T) {
	c := newNestH264TestConn(t)

	var wg sync.WaitGroup

	for i := 0; i < 8; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			c.failNestH264("stalled", nil)
		}()
	}

	for i := 0; i < 8; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			_ = c.Close()
		}()
	}

	wg.Wait()

	select {
	case <-c.done:
	// Expected.
	default:
		t.Fatal("connection remained open")
	}
}
