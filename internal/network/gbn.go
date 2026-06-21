package network

import (
	"SRTP/internal/protocol"
	"net"
	"time"
)

type GoBackNController struct{}

func (g *GoBackNController) HandlePacket(packet *protocol.SRTPPPacket, session *SRTSession) error {
	Logf(
		"[GBN-RECEIVER] Recebi pacote SEQ=%d\n",
		packet.Header.SEQ,
	)

	if packet.Header.SEQ == session.ExpectedSeq {

		isLast := packet.Header.Length < 255
		err := WritePayloadToFile(session, packet.Payload, isLast, "GBN")
		if err != nil {
			return err
		}

		SendControlPacket(session, session.ExpectedSeq, false)

		Logf("[GBN-RECEIVER] Pacote %d aceito. ACK cumulativo enviado.\n", packet.Header.SEQ)
		session.ExpectedSeq = (session.ExpectedSeq + 1) % 16384
	} else {
		Logf("[GBN-RECEIVER] Descarte! Chegou %d, mas esperava %d. Disparando NACK.\n", packet.Header.SEQ, session.ExpectedSeq)

		SendControlPacket(session, session.ExpectedSeq, true)

		return ErrFlushChannel
	}

	return nil
}

func (g *GoBackNController) TransmitFile(session *SRTSession) error {
	ACKConn, ACKPort, err := SetupACKListener(session)

	if err != nil {
		return err
	}
	defer ACKConn.Close()

	Logf("[GBN-SENDER] Preparado. Janela de %d pacotes. Escutando na porta %d...\n", session.WindowSize, ACKPort)

	// A gente precisa receber os acks do servidor de forma assincrona pq vamos estar enviando pacote toda hora

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

	//Pacote mais antigo que foi enviado, mas ainda nao temos ACK
	baseSEQ := session.NextSeqNum

	//Proximo SEQ disponível para novo pacote
	nextSEQNum := session.NextSeqNum

	//Tamanho da janela de pacotes que podemos enviar
	windowSize := uint16(session.WindowSize)

	// Map com Seq e pacote do SEQ
	windowBuffer := make(map[uint16]*protocol.SRTPPPacket)
	eof := false

	timer := time.NewTimer(100 * time.Millisecond)
	if !timer.Stop() {
		<-timer.C
	}

	const SEQModule = protocol.MaxSEQ + 1

	// A partir daqui vai ser loucurada
	// roda enquanto ainda tem arquivo pra mandar ou ACK pra receber
	for !eof || baseSEQ != nextSEQNum {

		// Vamos enviar os pacotes da janela
		for !eof {
			packetGoing := (nextSEQNum - baseSEQ + SEQModule) % SEQModule

			if packetGoing >= windowSize {
				break
			}

			//Ler proximo pacote do arquivo.
			filePacket, isEndOfFile, err := ReadNextPacket(session, nextSEQNum)
			if err != nil {
				return err
			}

			// Enviamos também o pacote Length=0 (arquivo múltiplo exato de 255
			// bytes) como terminador de stream, para o receiver finalizar o arquivo.
			windowBuffer[nextSEQNum] = filePacket
			SRTPBuffer, _ := protocol.EncodeSRTP(filePacket)
			session.Conn.Write(SRTPBuffer)

			// Se for o priemiro pacote da janela, a gente tem que ligar o cronometro
			if baseSEQ == nextSEQNum {
				timer.Reset(100 * time.Millisecond)
			}

			nextSEQNum = (nextSEQNum + 1) % 16384
			eof = isEndOfFile
		}

		select {
		//TIMEOUT, tem que mandar tudo do ultimo até o nextSEQNUM
		case <-timer.C:
			Logln("[GBN-SENDER] Timeout! Retransmitindo janela inteira...")
			current := baseSEQ

			for current != nextSEQNum {
				packetBytes, _ := protocol.EncodeSRTP(windowBuffer[current])
				session.Conn.Write(packetBytes)
				current = (current + 1) % 16384
			}

			timer.Reset(100 * time.Millisecond)
		//Recebemos ACK
		case ACKPacket := <-ACKChannel:
			confirmedPacket := ACKPacket.Header.ACK

			// Calcula a distância do ACK em relação à base e da janela toda

			// indica a distancia do pacote que recebemos até o ultimo que enviamos que ainda naor recebeu ACK
			// A gente precisa disso pq nao temos garantia do recebimento do ACKs em ordem sequencial pela rede.
			distToACK := (confirmedPacket - baseSEQ + SEQModule) % SEQModule

			//  isso me diz quatos pacotes disparamos que ainda nao receberam ACK
			distToWindow := (nextSEQNum - baseSEQ + SEQModule) % SEQModule

			// Isso aqui evita que a gente reprocesse Acks antigos se veio um ACK mais novo
			if distToACK < distToWindow {
				if ACKPacket.Header.NACK {
					// NACK: Retransmite a partir do pacote pedido
					ResendInterval(session.Conn, windowBuffer, confirmedPacket, nextSEQNum)
					timer.Reset(100 * time.Millisecond)
				} else {
					// ACK: Desliza a base da janela
					baseSEQ = ClearWindow(windowBuffer, baseSEQ, confirmedPacket)

					timer.Stop()
					// Se ainda tem coisa voando, liga o timer de novo
					if baseSEQ != nextSEQNum {
						timer.Reset(100 * time.Millisecond)
					}
				}
			}
		}
	}
	return nil
}

// Essa função vai reenviar pacotes de um até outro, a diferença entre eles
// isolei pq tava complicando demais o fluxo de cima, e me confundindo
// serve para reenviar pacotes desde o ultimo que recebemos ACK até o ultimo que enviamos.
func ResendInterval(conn *net.UDPConn, buffer map[uint16]*protocol.SRTPPPacket, start, end uint16) {
	curr := start
	for curr != end {
		packetBytes, _ := protocol.EncodeSRTP(buffer[curr])
		conn.Write(packetBytes)
		curr = (curr + 1) % (protocol.MaxSEQ + 1)
	}
}

// Função siomples auxiliar só para deletar as janelas que ja tivera, seus pacotes enviados
func ClearWindow(buffer map[uint16]*protocol.SRTPPPacket, base, end uint16) uint16 {
	curr := base
	for {
		delete(buffer, curr)
		if curr == end {
			return (curr + 1) % (protocol.MaxSEQ + 1)
		}
		curr = (curr + 1) % (protocol.MaxSEQ + 1)
	}
}
