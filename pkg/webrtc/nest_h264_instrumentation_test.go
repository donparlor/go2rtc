package webrtc

import (
	"sync"
	"testing"
	"time"
)

func TestNestH264RTPObservations(t *testing.T) {
	c := newNestH264TestConn(t)

	c.observeNestH264RTP([]byte{0x61, 0x01}, time.Second)
	c.observeNestH264RTP([]byte{0x65, 0x01}, 2*time.Second)
	c.observeNestH264RTP([]byte{0x67, 0x01}, 3*time.Second)
	c.observeNestH264RTP([]byte{0x68, 0x01}, 4*time.Second)

	if got := c.nestVideoRTPPackets.Load(); got != 4 {
		t.Fatalf("video RTP packets=%d; want=4", got)
	}
	if got := c.nestValidH264Packets.Load(); got != 4 {
		t.Fatalf("valid H264 packets=%d; want=4", got)
	}
	if got := c.nestIDRCount.Load(); got != 1 {
		t.Fatalf("IDR count=%d; want=1", got)
	}
	if got := c.nestSPSCount.Load(); got != 1 {
		t.Fatalf("SPS count=%d; want=1", got)
	}
	if got := c.nestPPSCount.Load(); got != 1 {
		t.Fatalf("PPS count=%d; want=1", got)
	}
	if got := time.Duration(c.nestFirstVideoNS.Load()); got != time.Second {
		t.Fatalf("first video=%s; want=1s", got)
	}
	if got := time.Duration(c.nestLastValidH264NS.Load()); got != 4*time.Second {
		t.Fatalf("last valid H264=%s; want=4s", got)
	}
	if got := time.Duration(c.nestLastIDRNS.Load()); got != 2*time.Second {
		t.Fatalf("last IDR=%s; want=2s", got)
	}
	if got := time.Duration(c.nestLastSPSNS.Load()); got != 3*time.Second {
		t.Fatalf("last SPS=%s; want=3s", got)
	}
	if got := time.Duration(c.nestLastPPSNS.Load()); got != 4*time.Second {
		t.Fatalf("last PPS=%s; want=4s", got)
	}
}

func TestNestH264InvalidPayloadStillCountsVideoRTP(t *testing.T) {
	c := newNestH264TestConn(t)

	c.observeNestH264RTP(
		[]byte{0x78, 0x00, 0x04, 0x67},
		time.Second,
	)

	if got := c.nestVideoRTPPackets.Load(); got != 1 {
		t.Fatalf("video RTP packets=%d; want=1", got)
	}
	if got := c.nestValidH264Packets.Load(); got != 0 {
		t.Fatalf("valid H264 packets=%d; want=0", got)
	}
}

func TestNestH264FUAIDRCountedOnlyAtStart(t *testing.T) {
	c := newNestH264TestConn(t)

	c.observeNestH264RTP([]byte{0x7C, 0x85, 0x01}, time.Second)
	c.observeNestH264RTP([]byte{0x7C, 0x05, 0x02}, 2*time.Second)
	c.observeNestH264RTP([]byte{0x7C, 0x45, 0x03}, 3*time.Second)

	if got := c.nestVideoRTPPackets.Load(); got != 3 {
		t.Fatalf("video RTP packets=%d; want=3", got)
	}
	if got := c.nestValidH264Packets.Load(); got != 3 {
		t.Fatalf("valid H264 packets=%d; want=3", got)
	}
	if got := c.nestIDRCount.Load(); got != 1 {
		t.Fatalf("IDR count=%d; want=1", got)
	}
	if got := time.Duration(c.nestLastIDRNS.Load()); got != time.Second {
		t.Fatalf("last IDR=%s; want=1s", got)
	}
}

func TestNestH264IDRDiagnosticThresholds(t *testing.T) {
	c := newNestH264TestConn(t)
	c.observeNestH264RTP([]byte{0x61, 0x01}, time.Second)

	var state nestH264DiagnosticState

	tests := []struct {
		name      string
		now       time.Duration
		threshold time.Duration
		want      bool
	}{
		{
			name: "before 30 seconds",
			now:  31*time.Second - time.Nanosecond,
		},
		{
			name:      "at 30 seconds",
			now:       31 * time.Second,
			threshold: 30 * time.Second,
			want:      true,
		},
		{
			name: "no repeat before 60 seconds",
			now:  60*time.Second + 999*time.Millisecond,
		},
		{
			name:      "at 60 seconds",
			now:       61 * time.Second,
			threshold: 60 * time.Second,
			want:      true,
		},
		{
			name:      "at 90 seconds",
			now:       91 * time.Second,
			threshold: 90 * time.Second,
			want:      true,
		},
		{
			name:      "at 120 seconds",
			now:       121 * time.Second,
			threshold: 120 * time.Second,
			want:      true,
		},
		{
			name: "no repeat after 120 seconds",
			now:  301 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Keep RTP active. Only IDR is absent.
			c.nestLastVideoNS.Store(int64(tt.now))

			threshold, got := c.nextNestH264IDRDiagnostic(tt.now, &state)
			if got != tt.want {
				t.Fatalf("diagnostic=%t; want=%t", got, tt.want)
			}
			if threshold != tt.threshold {
				t.Fatalf("threshold=%s; want=%s", threshold, tt.threshold)
			}
		})
	}

	if c.isDone() {
		t.Fatal("IDR diagnostics closed the connection")
	}
	// OnTrack normally marks video as seen before RTP instrumentation.
	// Reproduce that state for this isolated watchdog assertion.
	c.nestVideoSeen.Store(true)

	if reason := c.nestH264TimeoutReason(0, 301*time.Second); reason != "" {
		t.Fatalf("watchdog reason=%q with active RTP; want empty", reason)
	}
}

func TestNestH264IDRDiagnosticResetsAfterIDR(t *testing.T) {
	c := newNestH264TestConn(t)
	c.observeNestH264RTP([]byte{0x61, 0x01}, time.Second)

	var state nestH264DiagnosticState

	now := 31 * time.Second
	c.nestLastVideoNS.Store(int64(now))

	if threshold, ok := c.nextNestH264IDRDiagnostic(now, &state); !ok ||
		threshold != 30*time.Second {
		t.Fatalf("initial diagnostic=(%s,%t); want=(30s,true)", threshold, ok)
	}

	c.observeNestH264RTP([]byte{0x65, 0x01}, 40*time.Second)
	c.nestLastVideoNS.Store(int64(40 * time.Second))

	if threshold, ok := c.nextNestH264IDRDiagnostic(40*time.Second, &state); ok {
		t.Fatalf("diagnostic immediately after IDR=(%s,%t)", threshold, ok)
	}

	now = 70 * time.Second
	c.nestLastVideoNS.Store(int64(now))

	if threshold, ok := c.nextNestH264IDRDiagnostic(now, &state); !ok ||
		threshold != 30*time.Second {
		t.Fatalf("diagnostic after new drought=(%s,%t); want=(30s,true)", threshold, ok)
	}
}

func TestNestH264IDRDiagnosticSuppressedWithoutVideo(t *testing.T) {
	c := newNestH264TestConn(t)
	var state nestH264DiagnosticState

	if threshold, ok := c.nextNestH264IDRDiagnostic(
		120*time.Second,
		&state,
	); ok {
		t.Fatalf("diagnostic without video=(%s,%t)", threshold, ok)
	}
}

func TestNestH264IDRDiagnosticSuppressedForStalledVideo(t *testing.T) {
	c := newNestH264TestConn(t)
	c.observeNestH264RTP([]byte{0x61, 0x01}, time.Second)
	c.nestLastVideoNS.Store(int64(time.Second))

	var state nestH264DiagnosticState

	if threshold, ok := c.nextNestH264IDRDiagnostic(
		time.Second+nestH264Timeout,
		&state,
	); ok {
		t.Fatalf("diagnostic for stalled video=(%s,%t)", threshold, ok)
	}
}

func TestNestH264ObservationConcurrent(t *testing.T) {
	c := newNestH264TestConn(t)

	const iterations = 100
	var wg sync.WaitGroup

	for i := 0; i < iterations; i++ {
		wg.Add(2)

		go func() {
			defer wg.Done()
			c.observeNestH264RTP([]byte{0x61, 0x01}, time.Second)
		}()

		go func() {
			defer wg.Done()
			c.observeNestH264RTP([]byte{0x65, 0x01}, time.Second)
		}()
	}

	wg.Wait()

	if got := c.nestVideoRTPPackets.Load(); got != 2*iterations {
		t.Fatalf("video RTP packets=%d; want=%d", got, 2*iterations)
	}
	if got := c.nestValidH264Packets.Load(); got != 2*iterations {
		t.Fatalf("valid H264 packets=%d; want=%d", got, 2*iterations)
	}
	if got := c.nestIDRCount.Load(); got != iterations {
		t.Fatalf("IDR count=%d; want=%d", got, iterations)
	}
}

func TestNestH264ElapsedAgeDoesNotBecomeNegative(t *testing.T) {
	if got := nestH264ElapsedAge(time.Second, 2*time.Second); got != 0 {
		t.Fatalf("age=%s; want=0", got)
	}
}
