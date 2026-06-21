package network

import (
	"SRTP/internal/protocol"
	"fmt"
	"net"
	"os"
	"time"
)

type StopAndWaitController struct{}

// Função do Receiver
func (c *StopAndWaitController) HandlePacket(packet *protocol.SRTPPPacket, session *SRTSession) error {

	//se vier fora de ordem, nao temos NACK
	if packet.Header.SEQ == session.ExpectedSeq {
		//Inicia uma tranfência de arquivo se ainda nao houver.
		if session.CurrentFile == nil {
			clientFolder := fmt.Sprintf("recebidos/%s_%d", session.RemoteAddr.IP.String(), session.RemoteAddr.Port)
			fileName := fmt.Sprintf("%s/file_%d.bin", clientFolder, time.Now().UnixNano())

			file, err := os.Create(fileName)
			if err != nil {
				return fmt.Errorf("erro ao criar arquivo: %v", err)
			}

			session.CurrentFile = file
			fmt.Println("[SAW] Iniciando nova transferência:", fileName)
		}

		if len(packet.Payload) > 0 {
			session.CurrentFile.Write(packet.Payload)
		}

		ACKPacket := protocol.SRTPPPacket{
			Header: protocol.SRTPHeader{
				ACKFlag: true,
				ACK:     session.ExpectedSeq,
			},
		}

		ACKBuffer, _ := protocol.EncodeSRTP(&ACKPacket)

		localAddr := session.Conn.LocalAddr().(*net.UDPAddr)
		senderACKAddr := &net.UDPAddr{
			IP:   session.RemoteAddr.IP,
			Port: localAddr.Port + 1,
		}

		session.Conn.WriteToUDP(ACKBuffer, senderACKAddr)
		fmt.Printf("[SAW] Pacote %d recebido. ACK enviado para porta %s.\n", packet.Header.SEQ, senderACKAddr)

		if packet.Header.Length < 255 {
			fmt.Println("[SAW] Fim do arquivo detectado! Fechando documento.")
			session.CurrentFile.Close()
			session.CurrentFile = nil // Limpamos a variável. O próximo pacote que chegar vai acionar o passo 2 de novo!
		}

		// Avança a expectativa de pacote
		session.ExpectedSeq = (session.ExpectedSeq + 1) % 16384
	} else {
		fmt.Printf("[SAW] Aviso: Pacote ignorado. Esperava SEQ %d, chegou SEQ %d\n", session.ExpectedSeq, packet.Header.SEQ)
	}

	return nil
}

func (c *StopAndWaitController) TransmitFile(session *SRTSession) error {
	ACKPort := session.RemoteAddr.Port + 1
	ACKAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", ACKPort))

	if err != nil {
		return fmt.Errorf("erro ao resolver porta ACK: %v", err)
	}

	ACKConn, err := net.ListenUDP("udp", ACKAddr)
	if err != nil {
		return fmt.Errorf("erro ao escutar na porta P+1 (%d): %v", ACKPort, err)
	}

	defer ACKConn.Close()
	fmt.Printf("[SAW-SENDER] Preparado para enviar. Escutando ACKs na porta %d...\n", ACKPort)

	payloadBuffer := make([]byte, 255)
	ACKBuffer := make([]byte, 264)

	for {
		bytesReadFile, err := session.CurrentFile.Read(payloadBuffer)
		if err != nil && err.Error() != "EOF" {
			return fmt.Errorf("erro lendo arquivo do disco: %v", err)
		}

		if bytesReadFile == 0 {
			break
		}

		filePacket := protocol.SRTPPPacket{
			Header: protocol.SRTPHeader{
				SEQ:    session.NextSeqNum,
				Length: uint8(bytesReadFile),
			},
			Payload: payloadBuffer[:bytesReadFile],
		}

		packetBuffer, _ := protocol.EncodeSRTP(&filePacket)
		confirmPacket := false

		for !confirmPacket {
			session.Conn.Write(packetBuffer)

			ACKConn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
			length, _, err := ACKConn.ReadFromUDP(ACKBuffer)

			if err != nil {
				// Se deu erro de Timeout, avisa e o loop repete (retransmite)
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					fmt.Printf("[SAW-SENDER] Timeout de 100ms! Retransmitindo SEQ %d...\n", session.NextSeqNum)
					continue
				}
				return fmt.Errorf("erro na rede ao esperar ACK: %v", err)
			}

			if !protocol.ValidateCRC(ACKBuffer[:length]) {
				continue
			}

			ACKPacketRecv, err := protocol.DecodeSRTP(ACKBuffer[:length])

			if err == nil && ACKPacketRecv.Header.ACKFlag {
				// Confere se o ACK é exatamente para o pacote que enviamos
				if ACKPacketRecv.Header.ACK == session.NextSeqNum {
					fmt.Printf("[SAW-SENDER] Sucesso: Recebi o ACK %d\n", ACKPacketRecv.Header.ACK)

					session.NextSeqNum = (session.NextSeqNum + 1) % 16384
					confirmPacket = true
				} else {
					fmt.Printf("[SAW-SENDER] Aviso: Chegou ACK fora de ordem (%d)\n", ACKPacketRecv.Header.ACK)
				}
			}

		}
		// REGRA DE FIM DE STREAM: Se o pacote lido foi menor que 255, era o último!
		if bytesReadFile < 255 {
			fmt.Println("[SAW-SENDER] Último pacote confirmado. Fim do arquivo!")
			break
		}

	}

	return nil
}
