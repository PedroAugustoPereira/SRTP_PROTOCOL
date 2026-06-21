package network

import (
	"SRTP/internal/protocol"
	"fmt"
	"net"
	"os"
)

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
			continue
		}

		fmt.Printf("Recebemos mensagem de %v\n", remoteAddr.IP)

		if !protocol.ValidateCRC((buffer[:length])) {
			fmt.Printf("Pacote descartado de %v\n", remoteAddr.IP)
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
				fmt.Println("Aviso: Canal do worker cheio, pacote dropado")
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
		controller = &StopAndWaitController{} // Aqui acontece a "Tipagem Patinho" que conversamos!
	// Futuramente na Parte 2 você vai adicionar:
	// case "gbn":
	// 	controller = &GoBackNController{}
	// case "sr":
	// 	controller = &SelectiveRepeatController{}
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

	for receivedPacket := range w.PacketCh {
		// Verifica se o pacote é do HandShake
		switch w.Session.State {
		case StateSynReceived:
			if receivedPacket.Header.ACKFlag {
				w.Session.State = StateEstablished

				clientFolder := fmt.Sprintf("recebidos/%s_%d", w.Session.RemoteAddr.IP.String(), w.Session.RemoteAddr.Port)
				os.MkdirAll(clientFolder, 0755)

				fmt.Println("Handshake finalizado com " + w.Session.RemoteAddr.String())
			}
		case StateEstablished:
			if receivedPacket.Header.FIN {
				fmt.Printf("Pacote FIN recebido. Iniciando encerramento... %s\n", w.Session.RemoteAddr.IP.String())

				finAckPacket := protocol.SRTPPPacket{
					Header: protocol.SRTPHeader{
						FIN:     true,
						ACKFlag: true,
					},
				}
				finAckBuffer, _ := protocol.EncodeSRTP(&finAckPacket)
				w.Session.Conn.WriteToUDP(finAckBuffer, w.Session.RemoteAddr)
				break
			}

			err := w.Session.Controller.HandlePacket(receivedPacket, w.Session)

			if err != nil {
				fmt.Println("Erro ao processar pacote pelo controlador de fluxo:", err)
			}
		}

	}
}
