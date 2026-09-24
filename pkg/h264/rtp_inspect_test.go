package h264

import (
	"sync"
	"testing"
)

func TestInspectRTPPayload(t *testing.T) {
	tests := []struct {
		name    string
		payload []byte
		want    RTPInfo
	}{
		{
			name: "empty",
		},
		{
			name:    "single non IDR",
			payload: []byte{0x61, 0x01},
			want:    RTPInfo{Valid: true},
		},
		{
			name:    "single IDR",
			payload: []byte{0x65, 0x01},
			want:    RTPInfo{Valid: true, IDR: 1},
		},
		{
			name:    "single SPS",
			payload: []byte{0x67, 0x01},
			want:    RTPInfo{Valid: true, SPS: 1},
		},
		{
			name:    "single PPS",
			payload: []byte{0x68, 0x01},
			want:    RTPInfo{Valid: true, PPS: 1},
		},
		{
			name: "STAP-A SPS PPS IDR",
			payload: []byte{
				0x78,
				0x00, 0x02, 0x67, 0x01,
				0x00, 0x02, 0x68, 0x02,
				0x00, 0x02, 0x65, 0x03,
			},
			want: RTPInfo{
				Valid: true,
				IDR:   1,
				SPS:   1,
				PPS:   1,
			},
		},
		{
			name: "STAP-A multiple PPS",
			payload: []byte{
				0x78,
				0x00, 0x02, 0x68, 0x01,
				0x00, 0x02, 0x68, 0x02,
			},
			want: RTPInfo{Valid: true, PPS: 2},
		},
		{
			name:    "STAP-A empty",
			payload: []byte{0x78},
		},
		{
			name:    "STAP-A truncated length",
			payload: []byte{0x78, 0x00},
		},
		{
			name:    "STAP-A zero length",
			payload: []byte{0x78, 0x00, 0x00},
		},
		{
			name:    "STAP-A truncated NAL",
			payload: []byte{0x78, 0x00, 0x04, 0x67},
		},
		{
			name: "STAP-A invalid nested type",
			payload: []byte{
				0x78,
				0x00, 0x02, 0x78, 0x01,
			},
		},
		{
			name:    "FU-A IDR start",
			payload: []byte{0x7C, 0x85, 0x01},
			want:    RTPInfo{Valid: true, IDR: 1},
		},
		{
			name:    "FU-A IDR middle",
			payload: []byte{0x7C, 0x05, 0x02},
			want:    RTPInfo{Valid: true},
		},
		{
			name:    "FU-A IDR end",
			payload: []byte{0x7C, 0x45, 0x03},
			want:    RTPInfo{Valid: true},
		},
		{
			name:    "FU-A SPS start",
			payload: []byte{0x7C, 0x87, 0x01},
			want:    RTPInfo{Valid: true, SPS: 1},
		},
		{
			name:    "FU-A PPS start",
			payload: []byte{0x7C, 0x88, 0x01},
			want:    RTPInfo{Valid: true, PPS: 1},
		},
		{
			name:    "FU-A too short",
			payload: []byte{0x7C, 0x85},
		},
		{
			name:    "FU-A start and end",
			payload: []byte{0x7C, 0xC5, 0x01},
		},
		{
			name:    "FU-A reserved bit",
			payload: []byte{0x7C, 0xA5, 0x01},
		},
		{
			name:    "unsupported STAP-B",
			payload: []byte{0x79, 0x00, 0x00},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := InspectRTPPayload(tt.payload)
			if got != tt.want {
				t.Fatalf("InspectRTPPayload()=%+v; want=%+v", got, tt.want)
			}
		})
	}
}

func TestInspectRTPPayloadIncompleteFUA(t *testing.T) {
	start := InspectRTPPayload([]byte{0x7C, 0x85, 0x01})

	if !start.Valid || start.IDR != 1 {
		t.Fatalf("start=%+v; want an observed IDR NAL start", start)
	}

	// No end fragment follows. The inspector intentionally has no field
	// claiming that the fragmented NAL or image was completed or decoded.
}

func TestInspectRTPPayloadConcurrent(t *testing.T) {
	payloads := [][]byte{
		{0x61, 0x01},
		{0x65, 0x01},
		{0x67, 0x01},
		{0x68, 0x01},
		{0x7C, 0x85, 0x01},
		{0x7C, 0x05, 0x02},
		{0x7C, 0x45, 0x03},
	}

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		for _, payload := range payloads {
			wg.Add(1)

			go func(payload []byte) {
				defer wg.Done()

				if !InspectRTPPayload(payload).Valid {
					t.Errorf("payload %X reported invalid", payload)
				}
			}(payload)
		}
	}

	wg.Wait()
}
