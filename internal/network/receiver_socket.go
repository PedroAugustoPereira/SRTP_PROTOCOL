package network

import (
	"SRTP/internal/protocol"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)

var ErrFlushChannel = errors.New("FLUSH_CHANNEL")

func (r *Receiver) Listen(port int) error {
	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", port))

	if err != nil {
		return err
	}

	conn, err := net.ListenUDP("udp", addr)

	if err != nil {
		return err
	}

	r.Conn = conn

	if r.Sessions == nil {
		r.Sessions = make(map[string]*ClientWorker)
	}

	fmt.Printf("Servidor SRTP escutando na porta %d...\n", port)

	buffer := make([]byte, 264)

	// loop de interceptação de pacotes
	for {
		length, remoteAddr, err := r.Conn.ReadFromUDP(buffer)

		if err != nil {
			if r.Stopped {
				fmt.Println("\n[SERVER] Receiver parado pelo usuário.")
				return nil
			}
			continue
		}

		Logf("Recebemos mensagem de %v\n", remoteAddr.IP)

		if !protocol.ValidateCRC((buffer[:length])) {
			Logf("Pacote descartado de %v\n", remoteAddr.IP)
			continue
		}

		clientKey := remoteAddr.String()

		r.mu.RLock()
		worker, exists := r.Sessions[clientKey]
		r.mu.RUnlock()

		if !exists {
			receivedPacket, err := protocol.DecodeSRTP(buffer[:length])

			if err != nil || !receivedPacket.Header.SYN {
				continue
			}

			worker = r.createNewWorker(remoteAddr, receivedPacket, clientKey)
			r.mu.Lock()
			r.Sessions[clientKey] = worker
			r.mu.Unlock()

			go worker.processLoop()
		} else {
			receivedPacket, _ := protocol.DecodeSRTP(buffer[:length])

			select {
			case worker.PacketCh <- receivedPacket:
			default:
				Logln("Aviso: Canal do worker cheio, pacote dropado")
			}
		}
	}
}

func (r *Receiver) createNewWorker(addr *net.UDPAddr, syncPacket *protocol.SRTPPPacket, clientKey string) *ClientWorker {
	senderWindow := syncPacket.Header.Length

	var negotiatedWindow uint8 = 16

	if senderWindow < negotiatedWindow {
		negotiatedWindow = senderWindow
	}

	// 1. Criamos a variável da interface
	var controller FlowController

	// 2. Decidimos qual "cérebro" usar com base na string do modo
	switch r.Mode {
	case "saw":
		controller = &StopAndWaitController{}
	case "gbn":
		controller = &GoBackNController{}
	case "sr":
		controller = &SelectiveRepeatController{}
	default:
		// Se o usuário digitar um modo inválido, o padrão de fallback é o SAW
		fmt.Println("[Aviso] Modo desconhecido. Usando 'saw' como padrão.")
		controller = &StopAndWaitController{}
	}

	session := &SRTSession{
		State:      StateClosed,
		Conn:       r.Conn,
		RemoteAddr: addr,
		WindowSize: negotiatedWindow,
		Controller: controller,
	}

	return &ClientWorker{
		Session:   session,
		PacketCh:  make(chan *protocol.SRTPPPacket, 100),
		ClientKey: clientKey,
		Server:    r,
	}
}

func (w *ClientWorker) processLoop() {
	defer func() {
		w.Server.mu.Lock()
		delete(w.Server.Sessions, w.ClientKey)
		w.Server.mu.Unlock()

		w.Session.State = StateClosed
		fmt.Printf("[SERVER] Sessão de %s totalmente encerrada. Goroutine destruída.\n", w.ClientKey)
	}()

	SYN_ACK_Packet := protocol.SRTPPPacket{
		Header: protocol.SRTPHeader{
			SYN:     true,
			ACKFlag: true,
			Length:  w.Session.WindowSize,
		},
	}

	//Enviamos o SYN do HandShake
	SYN_ACK_Buffer, _ := protocol.EncodeSRTP(&SYN_ACK_Packet)
	w.Session.Conn.WriteToUDP(SYN_ACK_Buffer, w.Session.RemoteAddr)

	//Esperado a ultima mensagem do tree Handshake
	w.Session.State = StateSynReceived

	// Timer de inatividade ou encerramento
	timer := time.NewTimer(5 * time.Second) // Se ficar 5s sem receber nada, morre
	defer timer.Stop()

	for {
		select {
		case receivedPacket := <-w.PacketCh:
			// Reset no timer sempre que receber pacote
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(5 * time.Second)

			// Verifica se o pacote é do HandShake
			switch w.Session.State {
			case StateSynReceived:
				if receivedPacket.Header.ACKFlag {
					w.Session.State = StateEstablished

					clientFolder := fmt.Sprintf("recebidos/%s_%d", w.Session.RemoteAddr.IP.String(), w.Session.RemoteAddr.Port)
					os.MkdirAll(clientFolder, 0755)
					w.Session.ClientFolder = clientFolder

					fmt.Println("Handshake finalizado com " + w.Session.RemoteAddr.String())
				} else if receivedPacket.Header.SYN {
					fmt.Printf("[SERVER] Re-recebido SYN em StateSynReceived. Re-enviando SYN+ACK para %s\n", w.Session.RemoteAddr.IP.String())
					w.Session.Conn.WriteToUDP(SYN_ACK_Buffer, w.Session.RemoteAddr)
				} else {
					// Se não é SYN nem ACK, é um pacote de dados (SEQ 0).
					// Isso significa que o Sender recebeu nosso SYN+ACK, mas o ACK final dele se perdeu.
					// Portanto, consideramos o Handshake concluído!
					fmt.Printf("[SERVER] Pacote de dados recebido durante handshake. Assumindo ACK implícito de %s!\n", w.Session.RemoteAddr.IP.String())
					w.Session.State = StateEstablished

					clientFolder := fmt.Sprintf("recebidos/%s_%d", w.Session.RemoteAddr.IP.String(), w.Session.RemoteAddr.Port)
					os.MkdirAll(clientFolder, 0755)
					w.Session.ClientFolder = clientFolder

					// Como já mudamos o estado, processamos o pacote de dados agora mesmo
					err := w.Session.Controller.HandlePacket(receivedPacket, w.Session)
					if err == ErrFlushChannel {
						fmt.Println("[SERVER] NACK enviado, esvaziando canal de pacotes...")
						draining := true
						for draining {
							select {
							case <-w.PacketCh:
							default:
								draining = false
							}
						}
					} else if err != nil {
						fmt.Println("Erro ao processar pacote pelo controlador de fluxo:", err)
					}
				}
			case StateEstablished:
				if receivedPacket.Header.FIN {
					fmt.Printf("Pacote FIN recebido. Enviando FIN+ACK e entrando em TIME_WAIT... %s\n", w.Session.RemoteAddr.IP.String())

					finAckPacket := protocol.SRTPPPacket{
						Header: protocol.SRTPHeader{
							FIN:     true,
							ACKFlag: true,
						},
					}
					finAckBuffer, _ := protocol.EncodeSRTP(&finAckPacket)
					w.Session.Conn.WriteToUDP(finAckBuffer, w.Session.RemoteAddr)

					w.Session.State = StateTimeWait
					// Reduz o timer para 2 segundos no TimeWait
					timer.Reset(2 * time.Second)
					continue
				}

				err := w.Session.Controller.HandlePacket(receivedPacket, w.Session)

				if err == ErrFlushChannel {
					fmt.Println("[SERVER] NACK enviado, esvaziando canal de pacotes...")
					// Drena todos os pacotes pendentes no canal sem matar a goroutine
					draining := true
					for draining {
						select {
						case <-w.PacketCh:
						default:
							draining = false
						}
					}
				} else if err != nil {
					fmt.Println("Erro ao processar pacote pelo controlador de fluxo:", err)
				}
			case StateTimeWait:
				if receivedPacket.Header.FIN {
					fmt.Printf("Re-recebido FIN em TIME_WAIT. Re-enviando FIN+ACK... %s\n", w.Session.RemoteAddr.IP.String())
					finAckPacket := protocol.SRTPPPacket{
						Header: protocol.SRTPHeader{
							FIN:     true,
							ACKFlag: true,
						},
					}
					finAckBuffer, _ := protocol.EncodeSRTP(&finAckPacket)
					w.Session.Conn.WriteToUDP(finAckBuffer, w.Session.RemoteAddr)
					timer.Reset(2 * time.Second)
				}
			}
		case <-timer.C:
			if w.Session.State == StateTimeWait {
				fmt.Printf("[SERVER] TIME_WAIT de %s concluído. Sessão finalizada.\n", w.ClientKey)
			} else {
				fmt.Printf("[SERVER] Timeout de sessão por inatividade. Encerrando %s\n", w.ClientKey)
			}
			return
		}
	}
}

func WritePayloadToFile(session *SRTSession, payload []byte, isLastPacket bool, modeName string) error {
	if session.CurrentFile == nil {
		// Usa data e hora legível. Inicialmente salva como .tmp para depois renomear
		timestamp := time.Now().Format("2006-01-02_15h04m05s")
		fileName := fmt.Sprintf("%s/file_%s.tmp", session.ClientFolder, timestamp)
		file, err := os.Create(fileName)
		if err != nil {
			return fmt.Errorf("erro ao criar arquivo: %v", err)
		}
		session.CurrentFile = file
		fmt.Printf("[%s-RECEIVER] Iniciando transferência: %s\n", modeName, fileName)
	}

	if len(payload) > 0 {
		_, err := session.CurrentFile.Write(payload)
		if err != nil {
			return fmt.Errorf("erro escrevendo no disco: %v", err)
		}
	}

	if isLastPacket {
		fmt.Printf("[%s-RECEIVER] Fim do stream detectado! Fechando arquivo.\n", modeName)
		tmpFileName := session.CurrentFile.Name()
		session.CurrentFile.Close()
		session.CurrentFile = nil

		// Detectar a extensão lendo os primeiros bytes do arquivo salvo
		extension := ".bin" // default
		f, err := os.Open(tmpFileName)
		if err == nil {
			buffer := make([]byte, 512)
			n, _ := f.Read(buffer)
			f.Close()

			if n > 0 {
				mimeType := http.DetectContentType(buffer[:n])

				// Exemplo: mimeType = "application/pdf" ou "text/plain; charset=utf-8"
				parts := strings.SplitN(mimeType, "/", 2)

				if len(parts) == 2 {
					mimeTypePart := parts[1]

					// Remove possíveis parâmetros extras (ex: "; charset=utf-8")
					mimeTypePart = strings.SplitN(mimeTypePart, ";", 2)[0]
					mimeTypePart = strings.TrimSpace(mimeTypePart)

					// Tratamento de exceções (quando o nome MIME não é igual a extensão)
					if mimeTypePart == "plain" {
						extension = ".txt"
					} else if mimeTypePart == "octet-stream" {
						extension = ".bin" // binário genérico
					} else {
						extension = "." + mimeTypePart // Ex: ".pdf", ".png", ".jpeg", ".zip"
					}
				}
			}
		}

		// Renomear o arquivo com a extensão correta
		finalFileName := tmpFileName[:len(tmpFileName)-4] + extension
		err = os.Rename(tmpFileName, finalFileName)
		if err != nil {
			fmt.Printf("[%s-RECEIVER] Erro ao renomear arquivo para %s: %v\n", modeName, extension, err)
		} else {
			fmt.Printf("[%s-RECEIVER] Arquivo salvo e tipado com sucesso: %s\n", modeName, finalFileName)
		}
	}

	return nil
}

func SendControlPacket(session *SRTSession, ackNum uint16, isNack bool) error {
	localAddr := session.Conn.LocalAddr().(*net.UDPAddr)
	senderControlAddr := &net.UDPAddr{
		IP:   session.RemoteAddr.IP,
		Port: localAddr.Port + 1,
	}

	ctrlPacket := protocol.SRTPPPacket{
		Header: protocol.SRTPHeader{
			ACKFlag: true,
			NACK:    isNack,
			ACK:     ackNum,
		},
	}

	buffer, err := protocol.EncodeSRTP(&ctrlPacket)
	if err != nil {
		return fmt.Errorf("erro ao codificar pacote de controle: %v", err)
	}

	_, err = session.Conn.WriteToUDP(buffer, senderControlAddr)
	return err
}
