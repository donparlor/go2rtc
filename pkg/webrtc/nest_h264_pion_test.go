package webrtc

import (
	"testing"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/pion/rtp"
	pion "github.com/pion/webrtc/v4"
)

// TestNestH264PionTrackStall exercises the actual Pion OnTrack/read
// path, rather than directly simulating a video packet arrival.
//
// A local peer sends real H264 RTP. After go2rtc receives video,
// transmission stops and the last-video timestamp is made stale.
// The Nest-specific watchdog must then close the connection and
// unblock Conn.Start().
func TestNestH264PionTrackStall(t *testing.T) {
	api, err := NewAPI()
	if err != nil {
		t.Fatalf("NewAPI: %v", err)
	}

	localPC, err := api.NewPeerConnection(pion.Configuration{})
	if err != nil {
		t.Fatalf("local NewPeerConnection: %v", err)
	}

	conn := NewConn(localPC)
	conn.FormatName = "nest/webrtc"
	conn.Mode = core.ModeActiveProducer

	t.Cleanup(func() {
		_ = conn.Close()
	})

	remotePC, err := pion.NewPeerConnection(pion.Configuration{})
	if err != nil {
		t.Fatalf("remote NewPeerConnection: %v", err)
	}

	t.Cleanup(func() {
		_ = remotePC.Close()
	})

	video, err := pion.NewTrackLocalStaticRTP(
		pion.RTPCodecCapability{
			MimeType:  pion.MimeTypeH264,
			ClockRate: 90000,
		},
		"video",
		"nest-h264-local-test",
	)
	if err != nil {
		t.Fatalf("NewTrackLocalStaticRTP: %v", err)
	}

	if _, err = remotePC.AddTrack(video); err != nil {
		t.Fatalf("remote AddTrack: %v", err)
	}

	offer, err := conn.CreateCompleteOffer([]*core.Media{
		{
			Kind:      core.KindVideo,
			Direction: core.DirectionRecvonly,
		},
	})
	if err != nil {
		t.Fatalf("CreateCompleteOffer: %v", err)
	}

	err = remotePC.SetRemoteDescription(pion.SessionDescription{
		Type: pion.SDPTypeOffer,
		SDP:  offer,
	})
	if err != nil {
		t.Fatalf("remote SetRemoteDescription: %v", err)
	}

	answer, err := remotePC.CreateAnswer(nil)
	if err != nil {
		t.Fatalf("remote CreateAnswer: %v", err)
	}

	if err = remotePC.SetLocalDescription(answer); err != nil {
		t.Fatalf("remote SetLocalDescription: %v", err)
	}

	<-pion.GatheringCompletePromise(remotePC)

	if err = conn.SetAnswer(remotePC.LocalDescription().SDP); err != nil {
		t.Fatalf("conn SetAnswer: %v", err)
	}

	startReturned := make(chan struct{})

	go func() {
		defer close(startReturned)
		_ = conn.Start()
	}()

	// Send actual RTP until OnTrack has successfully parsed
	// at least one nonempty H264 payload.
	deadline := time.Now().Add(12 * time.Second)
	var sequence uint16
	var lastWriteErr error

	for !conn.nestVideoSeen.Load() && time.Now().Before(deadline) {
		sequence++

		lastWriteErr = video.WriteRTP(&rtp.Packet{
			Header: rtp.Header{
				Version:        2,
				SequenceNumber: sequence,
				Timestamp:      uint32(sequence) * 3000,
			},
			Payload: []byte{0x65, 0x88, 0x84},
		})

		time.Sleep(40 * time.Millisecond)
	}

	if !conn.nestVideoSeen.Load() {
		t.Fatalf(
			"real H264 RTP never reached OnTrack; local=%v remote=%v lastWriteErr=%v",
			localPC.ConnectionState(),
			remotePC.ConnectionState(),
			lastWriteErr,
		)
	}

	if conn.nestLastVideoNS.Load() <= 0 {
		t.Fatal("OnTrack did not record a monotonic H264 timestamp")
	}

	t.Log("PASS: real H264 RTP reached Nest OnTrack")

	// No more video packets are transmitted after this point.
	// Accelerate only the stall clock; the Pion connection itself
	// remains real and open until the watchdog acts.
	conn.nestLastVideoNS.Store(
		int64(time.Since(conn.nestEpoch) -
			nestH264Timeout -
			time.Second),
	)

	select {
	case <-conn.done:
		t.Log("PASS: H264 watchdog closed the stalled PeerConnection")

	case <-time.After(4 * time.Second):
		t.Fatalf(
			"watchdog did not close stalled H264; local=%v remote=%v",
			localPC.ConnectionState(),
			remotePC.ConnectionState(),
		)
	}

	select {
	case <-startReturned:
		t.Log("PASS: Conn.Start returned for producer reconnect")

	case <-time.After(3 * time.Second):
		t.Fatal("Conn.Start remained blocked after H264 recovery")
	}

	// Conn.Close signals c.done and releases Conn.Start before
	// pc.Close has necessarily completed. Wait for Pion's actual
	// closed state instead of racing the close operation.
	deadline = time.Now().Add(3 * time.Second)

	for {
		state := localPC.ConnectionState()

		if state == pion.PeerConnectionStateClosed {
			t.Log("PASS: Pion PeerConnection reached closed state")
			break
		}

		if time.Now().After(deadline) {
			t.Fatalf(
				"Pion PeerConnection never reached closed state; last=%v",
				state,
			)
		}

		time.Sleep(10 * time.Millisecond)
	}
}
