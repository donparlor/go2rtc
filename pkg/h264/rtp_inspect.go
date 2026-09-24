package h264

import "encoding/binary"

const (
	rtpNALUTypeMask = 0x1F
	rtpFUStartMask  = 0x80
	rtpFUEndMask    = 0x40
	rtpFUReserved   = 0x20
)

// RTPInfo describes H264 NAL observations made directly from one RTP payload.
//
// IDR, SPS and PPS count NAL starts observed in this payload. In particular,
// an IDR reported from a FU-A start does not prove that the complete NAL,
// access unit or decodable image was received.
type RTPInfo struct {
	Valid bool
	IDR   uint16
	SPS   uint16
	PPS   uint16
}

// InspectRTPPayload inspects an RFC 6184 H264 RTP payload without allocating.
//
// It supports single NAL units, STAP-A and FU-A. It does not reconstruct
// fragmented NAL units or prove that an access unit can be decoded.
func InspectRTPPayload(payload []byte) RTPInfo {
	if len(payload) == 0 {
		return RTPInfo{}
	}

	naluType := payload[0] & rtpNALUTypeMask

	switch {
	case naluType > 0 && naluType < stapaNALUType:
		var info RTPInfo
		info.Valid = true
		info.addNALType(naluType)
		return info

	case naluType == stapaNALUType:
		return inspectRTPSTAPA(payload)

	case naluType == fuaNALUType:
		return inspectRTPFUA(payload)

	default:
		return RTPInfo{}
	}
}

func inspectRTPSTAPA(payload []byte) RTPInfo {
	var info RTPInfo

	offset := 1
	nalus := 0

	for offset < len(payload) {
		if len(payload)-offset < 2 {
			return RTPInfo{}
		}

		size := int(binary.BigEndian.Uint16(payload[offset:]))
		offset += 2

		if size == 0 || size > len(payload)-offset {
			return RTPInfo{}
		}

		naluType := payload[offset] & rtpNALUTypeMask
		if naluType == 0 || naluType >= stapaNALUType {
			return RTPInfo{}
		}

		info.addNALType(naluType)
		nalus++
		offset += size
	}

	if nalus == 0 || offset != len(payload) {
		return RTPInfo{}
	}

	info.Valid = true
	return info
}

func inspectRTPFUA(payload []byte) RTPInfo {
	if len(payload) < 3 {
		return RTPInfo{}
	}

	header := payload[1]
	start := header&rtpFUStartMask != 0
	end := header&rtpFUEndMask != 0
	naluType := header & rtpNALUTypeMask

	if header&rtpFUReserved != 0 ||
		(start && end) ||
		naluType == 0 ||
		naluType >= stapaNALUType {
		return RTPInfo{}
	}

	info := RTPInfo{Valid: true}
	if start {
		info.addNALType(naluType)
	}
	return info
}

func (info *RTPInfo) addNALType(naluType byte) {
	switch naluType {
	case NALUTypeIFrame:
		info.IDR++
	case NALUTypeSPS:
		info.SPS++
	case NALUTypePPS:
		info.PPS++
	}
}
