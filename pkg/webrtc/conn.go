package webrtc

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/AlexxIT/go2rtc/pkg/h264"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

const nestH264Timeout = 30 * time.Second

type Conn struct {
	core.Connection
	core.Listener

	Mode core.Mode `json:"mode"`

	pc *webrtc.PeerConnection

	offer  string
	closed core.Waiter

	// Independent of core.Waiter: the watchdog must not hold Start() open.
	done     chan struct{}
	doneOnce sync.Once

	// time.Since(nestEpoch) preserves Go's monotonic clock.
	nestEpoch            time.Time
	nestWatchOnce        sync.Once
	nestFailOnce         sync.Once
	nestVideoSeen        atomic.Bool
	nestFirstVideoNS     atomic.Int64
	nestLastVideoNS      atomic.Int64
	nestLastValidH264NS  atomic.Int64
	nestLastIDRNS        atomic.Int64
	nestLastSPSNS        atomic.Int64
	nestLastPPSNS        atomic.Int64
	nestVideoRTPPackets  atomic.Uint64
	nestValidH264Packets atomic.Uint64
	nestIDRCount         atomic.Uint64
	nestSPSCount         atomic.Uint64
	nestPPSCount         atomic.Uint64
	nestUnmarshalErrors  atomic.Uint64
}

func NewConn(pc *webrtc.PeerConnection) *Conn {
	c := &Conn{
		Connection: core.Connection{
			ID:         core.NewID(),
			FormatName: "webrtc",
			Transport:  pc,
		},
		pc:        pc,
		done:      make(chan struct{}),
		nestEpoch: time.Now(),
	}
	c.nestFirstVideoNS.Store(-1)

	pc.OnICECandidate(func(candidate *webrtc.ICECandidate) {
		// last candidate will be empty
		if candidate != nil {
			c.Fire(candidate)
		}
	})

	pc.OnDataChannel(func(channel *webrtc.DataChannel) {
		c.Fire(channel)
	})

	pc.OnICEConnectionStateChange(func(state webrtc.ICEConnectionState) {
		if state != webrtc.ICEConnectionStateChecking {
			return
		}
		pc.SCTP().Transport().ICETransport().OnSelectedCandidatePairChange(
			func(pair *webrtc.ICECandidatePair) {
				// fix situation when candidate pair changes multiple times
				if i := strings.IndexByte(c.Protocol, '+'); i > 0 {
					c.Protocol = c.Protocol[:i]
				}
				c.Protocol += "+" + pair.Remote.Protocol.String()
				c.RemoteAddr = fmt.Sprintf(
					"%s:%d %s", sanitizeIP6(pair.Remote.Address), pair.Remote.Port, pair.Remote.Typ,
				)
				if pair.Remote.RelatedAddress != "" {
					c.RemoteAddr += fmt.Sprintf(
						" %s:%d", sanitizeIP6(pair.Remote.RelatedAddress), pair.Remote.RelatedPort,
					)
				}
			},
		)
	})

	pc.OnTrack(func(remote *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
		media, codec := c.getMediaCodec(remote)
		if media == nil {
			return
		}

		track, err := c.GetTrack(media, codec)
		if err != nil {
			return
		}

		nestH264 := c.FormatName == "nest/webrtc" &&
			c.Mode == core.ModeActiveProducer &&
			remote.Kind() == webrtc.RTPCodecTypeVideo &&
			codec.Name == core.CodecH264

		switch c.Mode {
		case core.ModePassiveProducer, core.ModeActiveProducer:
			// replace the theoretical list of codecs with the actual list of codecs
			if len(media.Codecs) > 1 {
				media.Codecs = []*core.Codec{codec}
			}
		}

		if c.Mode == core.ModePassiveProducer && remote.Kind() == webrtc.RTPCodecTypeVideo {
			go func() {
				pkts := []rtcp.Packet{&rtcp.PictureLossIndication{MediaSSRC: uint32(remote.SSRC())}}
				for range time.NewTicker(time.Second * 2).C {
					if err := pc.WriteRTCP(pkts); err != nil {
						return
					}
				}
			}()
		}

		for {
			b := make([]byte, ReceiveMTU)
			n, _, err := remote.Read(b)
			if err != nil {
				if nestH264 && !c.isDone() {
					c.failNestH264("read_error", err)
				}
				return
			}

			c.Recv += n

			packet := &rtp.Packet{}
			if err := packet.Unmarshal(b[:n]); err != nil {
				if nestH264 {
					c.nestUnmarshalErrors.Add(1)
					continue
				}
				return
			}

			if len(packet.Payload) == 0 {
				continue
			}

			if nestH264 {
				now := time.Since(c.nestEpoch)
				c.nestLastVideoNS.Store(int64(now))
				c.nestVideoSeen.Store(true)
				c.observeNestH264RTP(packet.Payload, now)
			}

			track.WriteRTP(packet)
		}
	})

	// OK connection:
	// 15:01:46 ICE connection state changed: checking
	// 15:01:46 peer connection state changed: connected
	// 15:01:54 peer connection state changed: disconnected
	// 15:02:20 peer connection state changed: failed
	//
	// Fail connection:
	// 14:53:08 ICE connection state changed: checking
	// 14:53:39 peer connection state changed: failed
	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		c.Fire(state)

		switch state {
		case webrtc.PeerConnectionStateConnected:
			// Nest requests an H264 video media in its WebRTC session.
			if c.FormatName == "nest/webrtc" &&
				c.Mode == core.ModeActiveProducer {
				c.startNestH264Watchdog()
			}

			for _, sender := range c.Senders {
				sender.Start()
			}
		case webrtc.PeerConnectionStateDisconnected, webrtc.PeerConnectionStateFailed, webrtc.PeerConnectionStateClosed:
			// disconnect event comes earlier, than failed
			// but it comes only for success connections
			_ = c.Close()
		}
	})

	return c
}

func (c *Conn) MarshalJSON() ([]byte, error) {
	return json.Marshal(c.Connection)
}

func (c *Conn) Close() error {
	c.doneOnce.Do(func() {
		close(c.done)
	})

	c.closed.Done(nil)
	return c.pc.Close()
}

func (c *Conn) isDone() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

type nestH264DiagnosticState struct {
	idrCount uint64
	level    uint8
}

func (c *Conn) observeNestH264RTP(payload []byte, now time.Duration) {
	c.nestFirstVideoNS.CompareAndSwap(-1, int64(now))
	c.nestVideoRTPPackets.Add(1)

	info := h264.InspectRTPPayload(payload)
	if !info.Valid {
		return
	}

	c.nestLastValidH264NS.Store(int64(now))
	c.nestValidH264Packets.Add(1)

	if info.IDR != 0 {
		c.nestLastIDRNS.Store(int64(now))
		c.nestIDRCount.Add(uint64(info.IDR))
	}
	if info.SPS != 0 {
		c.nestLastSPSNS.Store(int64(now))
		c.nestSPSCount.Add(uint64(info.SPS))
	}
	if info.PPS != 0 {
		c.nestLastPPSNS.Store(int64(now))
		c.nestPPSCount.Add(uint64(info.PPS))
	}
}

func (c *Conn) nextNestH264IDRDiagnostic(
	now time.Duration,
	state *nestH264DiagnosticState,
) (time.Duration, bool) {
	if c.nestVideoRTPPackets.Load() == 0 {
		// no_video_initial already explains this condition
		return 0, false
	}

	lastVideo := time.Duration(c.nestLastVideoNS.Load())
	if now-lastVideo >= nestH264Timeout {
		// stalled already explains this condition
		return 0, false
	}

	idrCount := c.nestIDRCount.Load()
	if idrCount != state.idrCount {
		state.idrCount = idrCount
		state.level = 0
	}

	var base time.Duration
	if idrCount == 0 {
		base = time.Duration(c.nestFirstVideoNS.Load())
	} else {
		base = time.Duration(c.nestLastIDRNS.Load())
	}

	age := now - base
	var level uint8

	switch {
	case age >= 120*time.Second:
		level = 4
	case age >= 90*time.Second:
		level = 3
	case age >= 60*time.Second:
		level = 2
	case age >= 30*time.Second:
		level = 1
	default:
		return 0, false
	}

	if level <= state.level {
		return 0, false
	}

	state.level = level
	return time.Duration(level) * 30 * time.Second, true
}

func (c *Conn) logNestH264IDRDiagnostic(now, threshold time.Duration) {
	idrCount := c.nestIDRCount.Load()

	log.Printf(
		"[nest/h264] diagnostic conn=%v reason=no_recent_idr threshold=%s video_age=%s valid_h264_age=%s idr_age=%s idr_wait=%s sps_rtp_age=%s pps_rtp_age=%s video_rtp_packets=%d valid_h264_packets=%d idr_count=%d sps_count=%d pps_count=%d rtp_unmarshal_errors=%d idr_seen=%t sps_rtp_seen=%t pps_rtp_seen=%t",
		c.ID,
		threshold,
		nestH264ObservedAge(now, c.nestLastVideoNS.Load(), c.nestVideoRTPPackets.Load()),
		nestH264ObservedAge(now, c.nestLastValidH264NS.Load(), c.nestValidH264Packets.Load()),
		nestH264ObservedAge(now, c.nestLastIDRNS.Load(), idrCount),
		nestH264IDRWait(now, c.nestFirstVideoNS.Load(), c.nestLastIDRNS.Load(), idrCount),
		nestH264ObservedAge(now, c.nestLastSPSNS.Load(), c.nestSPSCount.Load()),
		nestH264ObservedAge(now, c.nestLastPPSNS.Load(), c.nestPPSCount.Load()),
		c.nestVideoRTPPackets.Load(),
		c.nestValidH264Packets.Load(),
		idrCount,
		c.nestSPSCount.Load(),
		c.nestPPSCount.Load(),
		c.nestUnmarshalErrors.Load(),
		idrCount != 0,
		c.nestSPSCount.Load() != 0,
		c.nestPPSCount.Load() != 0,
	)
}

func nestH264ObservedAge(now time.Duration, last int64, count uint64) string {
	if count == 0 {
		return "none"
	}
	return nestH264ElapsedAge(now, time.Duration(last)).String()
}

func nestH264IDRWait(now time.Duration, firstVideo, lastIDR int64, idrCount uint64) string {
	if idrCount == 0 {
		return nestH264ElapsedAge(now, time.Duration(firstVideo)).String()
	}
	return nestH264ElapsedAge(now, time.Duration(lastIDR)).String()
}

func nestH264ElapsedAge(now, last time.Duration) time.Duration {
	age := now - last
	if age < 0 {
		return 0
	}
	return age.Round(time.Millisecond)
}

func (c *Conn) startNestH264Watchdog() {
	// The initial deadline begins at Connected, not at NewConn.
	c.startNestH264WatchdogAt(time.Since(c.nestEpoch))
}

// startNestH264WatchdogAt also lets tests simulate the initial deadline
// without sleeping for 30 seconds.
func (c *Conn) startNestH264WatchdogAt(started time.Duration) {
	c.nestWatchOnce.Do(func() {
		var diagnosticState nestH264DiagnosticState

		go func() {
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()

			for {
				select {
				case <-c.done:
					return

				case <-ticker.C:
					now := time.Since(c.nestEpoch)

					if threshold, ok := c.nextNestH264IDRDiagnostic(now, &diagnosticState); ok {
						c.logNestH264IDRDiagnostic(now, threshold)
					}

					if reason := c.nestH264TimeoutReason(started, now); reason != "" {
						c.failNestH264(reason, nil)
						return
					}
				}
			}
		}()
	})
}

// nestH264TimeoutReason evaluates the Nest video deadline.
// started and now are monotonic elapsed durations from nestEpoch.
func (c *Conn) nestH264TimeoutReason(started, now time.Duration) string {
	if !c.nestVideoSeen.Load() {
		if now-started >= nestH264Timeout {
			return "no_video_initial"
		}
		return ""
	}

	last := time.Duration(c.nestLastVideoNS.Load())
	if now-last >= nestH264Timeout {
		return "stalled"
	}

	return ""
}

func (c *Conn) failNestH264(reason string, readErr error) {
	c.nestFailOnce.Do(func() {
		if c.isDone() {
			return
		}

		var videoAge time.Duration

		if c.nestVideoSeen.Load() {
			videoAge = time.Since(c.nestEpoch) -
				time.Duration(c.nestLastVideoNS.Load())
		}

		// Do not log c.URL, raw Nest errors, tokens or device IDs.
		log.Printf(
			"[nest/h264] recovery conn=%v reason=%s video_age=%s rtp_unmarshal_errors=%d read_error_type=%T",
			c.ID,
			reason,
			videoAge.Round(time.Millisecond),
			c.nestUnmarshalErrors.Load(),
			readErr,
		)

		// The existing WebRTC Start()/Producer.reconnect() owns redial.
		_ = c.Close()
	})
}

func (c *Conn) AddCandidate(candidate string) error {
	// pion uses only candidate value from json/object candidate struct
	return c.pc.AddICECandidate(webrtc.ICECandidateInit{Candidate: candidate})
}

func (c *Conn) getTranseiver(mid string) *webrtc.RTPTransceiver {
	for _, tr := range c.pc.GetTransceivers() {
		if tr.Mid() == mid {
			return tr
		}
	}
	return nil
}

func (c *Conn) getSenderTrack(mid string) *Track {
	if tr := c.getTranseiver(mid); tr != nil {
		if s := tr.Sender(); s != nil {
			if t := s.Track().(*Track); t != nil {
				return t
			}
		}
	}
	return nil
}

func (c *Conn) getMediaCodec(remote *webrtc.TrackRemote) (*core.Media, *core.Codec) {
	for _, tr := range c.pc.GetTransceivers() {
		// search Transeiver for this TrackRemote
		if tr.Receiver() == nil || tr.Receiver().Track() != remote {
			continue
		}

		// search Media for this MID
		for _, media := range c.Medias {
			if media.ID != tr.Mid() || media.Direction != core.DirectionRecvonly {
				continue
			}

			// search codec for this PayloadType
			for _, codec := range media.Codecs {
				if codec.PayloadType != uint8(remote.PayloadType()) {
					continue
				}
				return media, codec
			}
		}
	}

	// fix moment when core.ModePassiveProducer or core.ModeActiveProducer
	// sends new codec with new payload type to same media
	// check GetTrack
	panic(core.Caller())

	return nil, nil
}

func sanitizeIP6(host string) string {
	if strings.IndexByte(host, ':') > 0 {
		return "[" + host + "]"
	}
	return host
}
