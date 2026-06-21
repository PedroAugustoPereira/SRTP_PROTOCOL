package network

import (
	"SRTP/internal/protocol"
	"net"
	"os"
	"sync"
)

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
	ExpectedSeq uint16
	WindowSize  uint8

	Controller  FlowController
	CurrentFile *os.File
}

type Sender struct {
	Session *SRTSession
	File    string
}

type ClientWorker struct {
	Session   *SRTSession
	File      *os.File
	PacketCh  chan *protocol.SRTPPPacket
	ClientKey string
	Server    *Receiver
}

type Receiver struct {
	Conn     *net.UDPConn
	Sessions map[string]*ClientWorker
	mu       sync.RWMutex
	Mode     string
}
