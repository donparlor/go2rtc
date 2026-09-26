package webrtc

import (
	"bytes"
	"io"
	"log"
	"strings"
	"testing"

	pion "github.com/pion/webrtc/v4"
)

// This test uses a real Pion PeerConnection, but injects an EOF at go2rtc's
// recovery-classification boundary. It does NOT create a two-peer RTP session.
func newNestH264StopRegressionConn(t *testing.T) *Conn {
	t.Helper()

	pc, err := pion.NewPeerConnection(pion.Configuration{})
	if err != nil {
		t.Fatalf("NewPeerConnection: %v", err)
	}

	c := NewConn(pc)
	c.FormatName = "nest/webrtc"

	releaseClosedCallback := make(chan struct{})
	// Control the normally asynchronous callback so the test can observe the
	// actual interval after Pion.Close but before go2rtc Conn.Close.
	pc.OnConnectionStateChange(func(state pion.PeerConnectionState) {
		if state == pion.PeerConnectionStateClosed {
			<-releaseClosedCallback
			_ = c.Close()
		}
	})

	t.Cleanup(func() {
		close(releaseClosedCallback)
		_ = c.Close()
	})
	return c
}

// These tests are intentionally non-parallel: log.SetOutput is process-wide.
func captureNestH264RecoveryLog(f func()) string {
	original := log.Writer()
	var captured bytes.Buffer
	log.SetOutput(&captured)
	defer log.SetOutput(original)
	f()
	return captured.String()
}

func TestNestH264ConnCloseSuppressesReadError(t *testing.T) {
	c := newNestH264StopRegressionConn(t)
	if err := c.Close(); err != nil {
		t.Fatalf("Conn.Close: %v", err)
	}
	if !c.isDone() {
		t.Fatal("Conn.Close did not close c.done")
	}
	got := captureNestH264RecoveryLog(func() {
		c.failNestH264("read_error", io.EOF)
	})
	if strings.Contains(got, "[nest/h264] recovery") {
		t.Fatalf("ordinary Conn.Close was misclassified as recovery: %s", got)
	}
}

func TestNestH264PromotedStopSuppressesReadError(t *testing.T) {
	c := newNestH264StopRegressionConn(t)
	if c.isDone() {
		t.Fatal("new connection unexpectedly already done")
	}

	// The Stop method is resolved just as it is by an interface-based caller.
	var stopper interface{ Stop() error } = c
	if err := stopper.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if got := c.pc.SignalingState(); got != pion.SignalingStateClosed {
		t.Fatalf("Pion signaling state after Stop = %s; want closed", got)
	}
	doneBeforeClassification := c.isDone()
	t.Logf("After Stop: signaling=%s c.done_closed=%t",
		c.pc.SignalingState(), doneBeforeClassification)

	// Simulate the teardown EOF returned to the H264 read loop. The current
	// production method should NOT count a teardown as an independent failure.
	got := captureNestH264RecoveryLog(func() {
		c.failNestH264("read_error", io.EOF)
	})
	if strings.Contains(got, "[nest/h264] recovery") {
		t.Fatalf("REGRESSION REPRODUCED: streams-side Stop caused teardown EOF to be classified as read_error; c.done_closed_before_classification=%t; log=%s",
			doneBeforeClassification, got)
	}
}

func TestNestH264LiveReadErrorTriggersRecovery(t *testing.T) {
	c := newNestH264StopRegressionConn(t)
	if c.isDone() {
		t.Fatal("new connection unexpectedly already done")
	}
	if got := c.pc.SignalingState(); got == pion.SignalingStateClosed {
		t.Fatal("new PeerConnection unexpectedly already closed")
	}
	got := captureNestH264RecoveryLog(func() {
		c.failNestH264("read_error", io.EOF)
	})
	if !strings.Contains(got, "[nest/h264] recovery") ||
		!strings.Contains(got, "reason=read_error") {
		t.Fatalf("live-connection read error did not trigger recovery: %s", got)
	}
	if !c.isDone() {
		t.Fatal("live-connection read error did not close c.done")
	}
}
