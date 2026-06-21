package network

import (
	"SRTP/internal/protocol"
	"fmt"
	"time"
)

type SelectiveRepeatController struct{}

func (sr *SelectiveRepeatController) HandlePacket(packet *protocol.SRTPPPacket, session *SRTSession) error {
	receivedSEQ := packet.Header.SEQ
	expectedSEQ := session.ExpectedSeq
	windowSize := uint16(session.WindowSize)
	const SEQModule = protocol.MaxSEQ + 1

	// Calcula a distância do SEQ recebido em relação ao esperado (módulo 16384)
	distToSEQ := (receivedSEQ - expectedSEQ + SEQModule) % SEQModule

	// Se o pacote está dentro da janela do receiver, aceita e bufferiza
	if distToSEQ < windowSize {
		// Sempre manda ACK individual para o pacote recebido (SR = ACK individual)
		SendControlPacket(session, receivedSEQ, false)
		Logf("[SR-RECEIVER] ACK individual enviado para SEQ %d\n", receivedSEQ)

		// Armazena no buffer de recepção (lazy init)
		if session.SRRecvBuffer == nil {
			session.SRRecvBuffer = make(map[uint16]*protocol.SRTPPPacket)
		}

		// Só armazena se ainda não temos esse pacote
		if _, alreadyExists := session.SRRecvBuffer[receivedSEQ]; !alreadyExists {
			session.SRRecvBuffer[receivedSEQ] = packet
		}

		// Se recebemos um pacote fora de ordem (lacuna detectada), envia NACK para o pacote faltante
		// la no nosso enunciado diz pra usar NACK para isso.
		if receivedSEQ != expectedSEQ {
			SendControlPacket(session, expectedSEQ, true)
			Logf("[SR-RECEIVER] Lacuna detectada! NACK enviado para SEQ %d (faltante)\n", expectedSEQ)
		}

		// Se é exatamente o pacote esperado, entrega tudo que é contíguo
		if receivedSEQ == expectedSEQ {
			for {
				bufferedPacket, exists := session.SRRecvBuffer[session.ExpectedSeq]
				if !exists {
					break
				}

				isLast := bufferedPacket.Header.Length < 255
				err := WritePayloadToFile(session, bufferedPacket.Payload, isLast, "SR")
				if err != nil {
					return err
				}

				delete(session.SRRecvBuffer, session.ExpectedSeq)
				Logf("[SR-RECEIVER] Pacote %d entregue à aplicação.\n", session.ExpectedSeq)

				session.ExpectedSeq = (session.ExpectedSeq + 1) % SEQModule
			}
		}
	} else {
		Logf("[SR-RECEIVER] Pacote %d fora da janela (esperado=%d, janela=%d). Reenviando ACK.\n", receivedSEQ, expectedSEQ, windowSize)
		SendControlPacket(session, receivedSEQ, false)
	}

	return nil
}

func (sr *SelectiveRepeatController) TransmitFile(session *SRTSession) error {
	ACKConn, ACKPort, err := SetupACKListener(session)
	if err != nil {
		return err
	}
	defer ACKConn.Close()

	Logf("[SR-SENDER] Preparado. Janela de %d pacotes. Escutando ACKs na porta %d...\n", session.WindowSize, ACKPort)

	// Goroutine assíncrona para receber ACKs/NACKs
	ACKChannel := make(chan *protocol.SRTPPPacket, 100)

	go func() {
		buffer := make([]byte, 264)
		for {
			n, _, err := ACKConn.ReadFromUDP(buffer)
			if err != nil {
				return
			}
			if protocol.ValidateCRC(buffer[:n]) {
				packet, _ := protocol.DecodeSRTP(buffer[:n])
				ACKChannel <- packet
			}
		}
	}()

	const SEQModule = protocol.MaxSEQ + 1

	// base = menor SEQ não confirmado
	baseSEQ := session.NextSeqNum

	// próximo SEQ a ser enviado pela primeira vez
	nextSEQNum := session.NextSeqNum

	windowSize := uint16(session.WindowSize)

	// Buffer de pacotes enviados (para possível retransmissão)
	windowBuffer := make(map[uint16]*protocol.SRTPPPacket)

	// Mapa de ACKs recebidos (para saber quais pacotes já foram confirmados)
	ACKedMap := make(map[uint16]bool)

	// Timer individual por pacote enviado
	timers := make(map[uint16]*time.Timer)

	// Canal para notificar timeout de um pacote específico
	timeoutCh := make(chan uint16, 256)

	eof := false

	// Função auxiliar para iniciar timer de um pacote específico
	startTimer := func(targetSEQ uint16) {
		if existingTimer, exists := timers[targetSEQ]; exists {
			existingTimer.Stop()
		}
		timers[targetSEQ] = time.AfterFunc(100*time.Millisecond, func() {
			timeoutCh <- targetSEQ
		})
	}

	// Loop principal
	for !eof || baseSEQ != nextSEQNum {

		// 1. Envia novos pacotes enquanto a janela permitir
		for !eof {
			packetsInFlight := (nextSEQNum - baseSEQ + SEQModule) % SEQModule
			if packetsInFlight >= windowSize {
				break
			}

			filePacket, isEndOfFile, err := ReadNextPacket(session, nextSEQNum)
			if err != nil {
				return err
			}

			if filePacket.Header.Length == 0 && isEndOfFile {
				eof = true
				break
			}

			windowBuffer[nextSEQNum] = filePacket
			SRTPBuffer, _ := protocol.EncodeSRTP(filePacket)
			session.Conn.Write(SRTPBuffer)

			Logf("[SR-SENDER] Enviado SEQ %d\n", nextSEQNum)

			// Inicia timer individual para este pacote
			startTimer(nextSEQNum)

			nextSEQNum = (nextSEQNum + 1) % SEQModule
			eof = isEndOfFile
		}

		// 2. Espera por ACK ou timeout
		select {
		case ACKPacketRecv := <-ACKChannel:
			confirmedSEQ := ACKPacketRecv.Header.ACK

			if ACKPacketRecv.Header.NACK {
				// NACK: retransmite apenas o pacote pedido
				Logf("[SR-SENDER] NACK recebido para SEQ %d. Retransmitindo...\n", confirmedSEQ)
				if packetToResend, exists := windowBuffer[confirmedSEQ]; exists {
					packetBytes, _ := protocol.EncodeSRTP(packetToResend)
					session.Conn.Write(packetBytes)
					startTimer(confirmedSEQ)
				}
			} else {
				// ACK individual: marca como confirmado
				distToACK := (confirmedSEQ - baseSEQ + SEQModule) % SEQModule
				distToWindow := (nextSEQNum - baseSEQ + SEQModule) % SEQModule

				if distToACK < distToWindow {
					ACKedMap[confirmedSEQ] = true
					Logf("[SR-SENDER] ACK recebido para SEQ %d\n", confirmedSEQ)

					// Para o timer desse pacote
					if packetTimer, exists := timers[confirmedSEQ]; exists {
						packetTimer.Stop()
						delete(timers, confirmedSEQ)
					}

					// Desliza a base enquanto a base estiver confirmada
					for ACKedMap[baseSEQ] {
						delete(ACKedMap, baseSEQ)
						delete(windowBuffer, baseSEQ)
						baseSEQ = (baseSEQ + 1) % SEQModule
					}
				}
			}

		case timedOutSEQ := <-timeoutCh:
			// Timeout de um pacote específico — retransmite só ele
			if _, alreadyACKed := ACKedMap[timedOutSEQ]; !alreadyACKed {
				if packetToResend, exists := windowBuffer[timedOutSEQ]; exists {
					Logf("[SR-SENDER] Timeout! Retransmitindo SEQ %d\n", timedOutSEQ)
					packetBytes, _ := protocol.EncodeSRTP(packetToResend)
					session.Conn.Write(packetBytes)
					startTimer(timedOutSEQ)
				}
			}
		}
	}

	// Limpa timers remanescentes
	for _, t := range timers {
		t.Stop()
	}

	fmt.Println("[SR-SENDER] Todos os pacotes confirmados. Transmissão concluída!")
	return nil
}
