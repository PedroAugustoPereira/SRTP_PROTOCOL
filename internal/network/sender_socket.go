package network

import (
	"SRTP/internal/protocol"
	"fmt"
	"net"
	"os"
	"time"
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
			Length: s.Session.WindowSize,
		},
	}

	bufferSyn, err := protocol.EncodeSRTP(&packetSyn)

	if err != nil {
		return fmt.Errorf("Erro ao inciar HandShake: %v", err)
	}

	buffer := make([]byte, 9)
	handshakeComplete := false

	for !handshakeComplete {
		//Enviamos o Syn do HandShake
		s.Session.Conn.Write(bufferSyn)
		s.Session.State = StateSynSent

		s.Session.Conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		length, _, err := s.Session.Conn.ReadFromUDP(buffer)

		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				fmt.Println("[SENDER] Timeout esperando SYN+ACK... Retransmitindo SYN.")
				continue
			}
			return fmt.Errorf("erro ao ler resposta: %v", err)
		}

		if !protocol.ValidateCRC(buffer[:length]) {
			fmt.Println("[SENDER] SYN+ACK corrompido, retransmitindo SYN...")
			continue
		}

		responsePacket, err := protocol.DecodeSRTP(buffer[:length])
		if err != nil {
			return fmt.Errorf("erro ao decodar pacote: %v", err)
		}

		// Verificar se a resposata é SYN+ACK
		if responsePacket.Header.SYN && responsePacket.Header.ACKFlag {
			// Negociação de janela: min(sender, receiver) conforme enunciado
			receiverWindow := responsePacket.Header.Length
			senderWindow := packetSyn.Header.Length
			if receiverWindow < senderWindow {
				s.Session.WindowSize = receiverWindow
			} else {
				s.Session.WindowSize = senderWindow
			}
			fmt.Printf("[SENDER] Janela negociada com o servidor: %d pacotes\n", s.Session.WindowSize)

			// Agora precisamos mandar o ACK final para estabelecer a Conexão
			ACKPacket := protocol.SRTPPPacket{
				Header: protocol.SRTPHeader{
					ACKFlag: true,
				},
			}

			ACKBuffer, _ := protocol.EncodeSRTP(&ACKPacket)
			s.Session.Conn.Write(ACKBuffer)

			// Limpa o deadline para a fase de transferência
			s.Session.Conn.SetReadDeadline(time.Time{})

			s.Session.State = StateEstablished
			fmt.Println("Handshake concluído com sucesso! Estado: Established")
			handshakeComplete = true
		}
	}

	return nil
}

func (s *Sender) SendFile(filePath string) error {
	if s.Session.State != StateEstablished {
		return fmt.Errorf("impossível enviar arquivo: conexão não estabelecida")
	}

	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("erro ao abrir o arquivo %s: %v", filePath, err)
	}
	defer file.Close()

	s.Session.CurrentFile = file

	fmt.Printf("[SENDER] Iniciando transmissão do arquivo via controlador de fluxo...\n")

	err = s.Session.Controller.TransmitFile(s.Session)
	if err != nil {
		return fmt.Errorf("falha durante a transmissão dos dados: %v", err)
	}

	fmt.Println("[SENDER] Todos os blocos de dados do arquivo foram enviados")
	return nil
}

// Close executa o Two-Way Teardown mandando o FIN e esperando o FIN+ACK
func (s *Sender) Close() error {
	fmt.Println("[SENDER] Iniciando encerramento de sessão (FIN)...")

	FINPacket := protocol.SRTPPPacket{
		Header: protocol.SRTPHeader{
			FIN: true,
		},
	}
	FINBuffer, _ := protocol.EncodeSRTP(&FINPacket)
	ackBuffer := make([]byte, 9)

	confirmedFIN := false
	for !confirmedFIN {
		s.Session.Conn.Write(FINBuffer)

		s.Session.Conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))

		length, _, err := s.Session.Conn.ReadFromUDP(ackBuffer)

		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				fmt.Println("[SENDER] Timeout esperando FIN+ACK... Retransmitindo FIN.")
				continue
			}
			return fmt.Errorf("erro de rede durante o encerramento: %v", err)
		}

		packetRecv, err := protocol.DecodeSRTP(ackBuffer[:length])

		if err == nil && packetRecv.Header.FIN && packetRecv.Header.ACKFlag {
			fmt.Println("[SENDER] FIN+ACK recebido com sucesso do servidor!")
			confirmedFIN = true
		}
	}

	s.Session.State = StateClosed
	s.Session.Conn.Close()
	fmt.Println("[SENDER] Conexão totalmente fechada. Sessão destruída.")
	return nil
}

// SetupACKListener abre a porta P+1 para o Sender escutar as respostas do Receiver
func SetupACKListener(session *SRTSession) (*net.UDPConn, uint16, error) {
	ackPort := session.RemoteAddr.Port + 1
	ackAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", ackPort))
	if err != nil {
		return nil, uint16(ackPort), fmt.Errorf("erro ao resolver porta ACK: %v", err)
	}

	ackConn, err := net.ListenUDP("udp", ackAddr)
	if err != nil {
		return nil, uint16(ackPort), fmt.Errorf("erro ao escutar na porta P+1 (%d): %v", ackPort, err)
	}

	return ackConn, uint16(ackPort), nil
}

func ReadNextPacket(session *SRTSession, seqNum uint16) (*protocol.SRTPPPacket, bool, error) {
	payloadBuffer := make([]byte, 255)
	bytesRead, err := session.CurrentFile.Read(payloadBuffer)

	if err != nil && err.Error() != "EOF" {
		return nil, false, fmt.Errorf("erro lendo arquivo do disco: %v", err)
	}

	packet := &protocol.SRTPPPacket{
		Header: protocol.SRTPHeader{
			SEQ:    seqNum,
			Length: uint8(bytesRead),
		},
		Payload: payloadBuffer[:bytesRead],
	}

	isEOF := bytesRead < 255

	return packet, isEOF, nil
}
