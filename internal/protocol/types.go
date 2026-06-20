package protocol

const (
	SEQBits     = 14
	SYNBits     = 1
	FINBits     = 1
	ACKBits     = 14
	ACKFlagBits = 1
	NACKBits    = 1
	LengthBits  = 8
	CRC32Bits   = 32

	MaxSEQ      = (1 << SEQBits) - 1
	MaxSYN      = (1 << SYNBits) - 1
	MaxFIN      = (1 << FINBits) - 1
	MaxACK      = (1 << ACKBits) - 1
	MaxACKFlags = (1 << ACKFlagBits) - 1
	MaxNACK     = (1 << NACKBits) - 1
	MaxLength   = (1 << LengthBits) - 1
	MaxCRC32    = (1 << CRC32Bits) - 1
)

type SRTPHeader struct {
	SEQ     uint16
	SYN     bool
	FIN     bool
	ACK     uint16
	ACKFlag bool
	NACK    bool
	Length  uint8
	CRC32   uint32
}

type SRTPPPacket struct {
	header  SRTPHeader
	payload []byte
}
