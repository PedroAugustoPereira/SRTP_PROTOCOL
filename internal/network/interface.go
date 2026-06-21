package network

import (
	"SRTP/internal/protocol"
)

type FlowController interface {
	HandlePacket(packet *protocol.SRTPPPacket, session *SRTSession) error
	TransmitFile(session *SRTSession) error
}
