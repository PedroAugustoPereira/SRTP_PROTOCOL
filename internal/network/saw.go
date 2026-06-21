package network

import (
	"SRTP/internal/protocol"
	"fmt"
	"net"
	"time"
)

type StopAndWaitController struct{}

// Função do Receiver
func (c *StopAndWaitController) HandlePacket(packet *protocol.SRTPPPacket, session *SRTSession) error {
	//se vier fora de ordem, nao temos NACK
	if packet.Header.SEQ == session.ExpectedSeq {
		isLast := packet.Header.Length < 255

		err := WritePayloadToFile(session, packet.Payload, isLast, "SAW")
		if err != nil {
			return err
		}

		SendControlPacket(session, session.ExpectedSeq, false)

		Logf("[SAW-RECEIVER] Pacote %d aceito. ACK enviado.\n", packet.Header.SEQ)
		session.ExpectedSeq = (session.ExpectedSeq + 1) % 16384
	} else {
		Logf("[SAW] Aviso: Pacote ignorado. Esperava SEQ %d, chegou SEQ %d\n", session.ExpectedSeq, packet.Header.SEQ)
	}

	return nil
}

func (c *StopAndWaitController) TransmitFile(session *SRTSession) error {
	ACKConn, ACKPort, err := SetupACKListener(session)
	if err != nil {
		return err
	}

	defer ACKConn.Close()
	Logf("[SAW-SENDER] Preparado para enviar. Escutando ACKs na porta %d...\n", ACKPort)

	ACKBuffer := make([]byte, 264)

	for {
		filePacket, isEOF, err := ReadNextPacket(session, session.NextSeqNum)
		if err != nil {
			return err
		}

		// Mesmo com payload vazio (arquivo múltiplo exato de 255 bytes), enviamos
		// o pacote Length=0 como terminador de stream. O receiver precisa dele
		// para finalizar e renomear o arquivo (enunciado: edge case Length=0).
		packetBuffer, _ := protocol.EncodeSRTP(filePacket)
		confirmPacket := false

		for !confirmPacket {
			session.Conn.Write(packetBuffer)

			ACKConn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
			length, _, err := ACKConn.ReadFromUDP(ACKBuffer)

			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					Logf("[SAW-SENDER] Timeout de 100ms! Retransmitindo SEQ %d...\n", session.NextSeqNum)
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
					Logf("[SAW-SENDER] Sucesso: Recebi o ACK %d\n", ACKPacketRecv.Header.ACK)

					session.NextSeqNum = (session.NextSeqNum + 1) % 16384
					confirmPacket = true
				} else {
					Logf("[SAW-SENDER] Aviso: Chegou ACK fora de ordem (%d)\n", ACKPacketRecv.Header.ACK)
				}
			}

		}

		if isEOF {
			Logln("[SAW-SENDER] Último pacote confirmado. Fim do arquivo!")
			break
		}

	}

	return nil
}
