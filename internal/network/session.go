package network

import "net"

type State int

const (
	StateClosed State = iota
	StateSynSent
	StateSynReceived
	StateEstablished
)

type SRTSession struct {
	State       State
	Conn        *net.UDPConn
	RemoteAddr  *net.UDPAddr
	NextSeqNum  uint16
	ExpectedAck uint16
	WindowSize  uint8
}

type Sender struct {
	Session *SRTSession
	File    string
}
