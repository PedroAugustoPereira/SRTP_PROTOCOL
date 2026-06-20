package network

import (
	"SRTP/internal/protocol"
	"fmt"
	"net"
)

func (s *Sender) Dial(host string, port int) error {
	addr, _ := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", host, port))

	conn, _ := net.DialUDP("udp", nil, addr)

	s.Session.Conn = conn
	s.Session.RemoteAddr = addr

	return s.executeHandShake()
}

func (s *Sender) executeHandShake() error {
	packetSyn := protocol.SRTPPPacket{
		Header: protocol.SRTPHeader{
			SYN:    true,
			Length: 16,
		},
	}

	bufferSyn, err := protocol.EncodeSRTP(&packetSyn)

	if err != nil {
		return fmt.Errorf("Erro ao inciar HandShake: %v", err)
	}

	//Enviamos o Syn do HandShake
	s.Session.Conn.Write(bufferSyn)
	s.Session.State = StateSynSent

	buffer := make([]byte, 9)
	length, _, err := s.Session.Conn.ReadFromUDP(buffer)
	if err != nil {
		return fmt.Errorf("erro ao ler resposta: %v", err)
	}

	responsePacket, err := protocol.DecodeSRTP(buffer[:length])
	if err != nil {
		return fmt.Errorf("erro ao decodar pacote: %v", err)
	}

	// Verificar se a resposata é SYN
	if responsePacket.Header.SYN && responsePacket.Header.ACKFlag {
		// Agora precisamos mandar o ACK final para estabelecer a Conexão
		ACKPacket := protocol.SRTPPPacket{
			Header: protocol.SRTPHeader{
				ACKFlag: true,
			},
		}

		ACKBuffer, _ := protocol.EncodeSRTP(&ACKPacket)
		s.Session.Conn.Write(ACKBuffer)
		s.Session.State = StateEstablished
		fmt.Println("Handshake concluído com sucesso! Estado: Established")
		return nil
	}

	return fmt.Errorf("handshake falhou: resposta inesperada do servidor")
}
