package main

import (
	"SRTP/internal/network"
	"flag"
	"fmt"
	"os"
)

func main() {
	// 1. Definição das flags de linha de comando
	listen := flag.Bool("listen", false, "Modo receiver")
	port := flag.Int("port", 0, "Porta de operação")
	host := flag.String("host", "", "IP do host (apenas para sender)")
	file := flag.String("file", "", "Arquivo para transferir (apenas para sender)")
	mode := flag.String("mode", "saw", "Modo do protocolo: saw (Stop-and-Wait), gbn (Go-Back-N) ou sr (Selective Repeat)")

	flag.Parse()

	// 2. Validações de segurança e regras de negócio
	if *port == 0 {
		fmt.Println("Erro: A porta (--port) é obrigatória.")
		os.Exit(1)
	}

	// Trava para evitar que o professor digite um protocolo que não existe
	if *mode != "saw" && *mode != "gbn" && *mode != "sr" {
		fmt.Println("Erro: Modo inválido. Use 'saw', 'gbn' ou 'sr'.")
		os.Exit(1)
	}

	// 3. Separação de Rotas: O programa é um Servidor ou um Cliente?
	if *listen {
		// ==========================================
		// ROTA DO RECEIVER (SERVIDOR)
		// ==========================================
		fmt.Printf("Iniciando em modo Receiver na porta %d com protocolo %s...\n", *port, *mode)

		// O Receiver é uma "fábrica" de conexões.
		// Ele só precisa saber a porta e o modo de operação.
		receiver := network.Receiver{
			Mode: *mode,
		}

		// Trava a execução aqui escutando a rede infinitamente
		err := receiver.Listen(*port)
		if err != nil {
			fmt.Printf("Erro crítico no servidor: %v\n", err)
			os.Exit(1)
		}

	} else {
		// ==========================================
		// ROTA DO SENDER (CLIENTE)
		// ==========================================
		if *host == "" || *file == "" {
			fmt.Println("Erro: Modo sender exige --host e --file.")
			os.Exit(1)
		}

		fmt.Printf("Iniciando Sender conectando a %s:%d, enviando '%s' usando %s\n", *host, *port, *file, *mode)

		// A. Escolhemos qual "Cérebro" (Controlador de Fluxo) o cliente vai usar
		var controller network.FlowController
		switch *mode {
		case "saw":
			controller = &network.StopAndWaitController{}
			// Futuramente na Parte 2:
			// case "gbn":
			// 	controller = &network.GoBackNController{}
			// case "sr":
			// 	controller = &network.SelectiveRepeatController{}
		}

		// B. Montamos a estrutura do Sender injetando o Cérebro na Sessão
		sender := network.Sender{
			File: *file,
			Session: &network.SRTSession{
				State:      network.StateClosed,
				Controller: controller,
			},
		}

		// C. Executamos o Ciclo de Vida da Conexão em Ordem Estrita:

		// Passo 1: Handshake (Conecta e negocia janela)
		err := sender.Dial(*host, *port)
		if err != nil {
			fmt.Printf("Erro na conexão inicial (Handshake): %v\n", err)
			os.Exit(1)
		}

		// Passo 2: Transmissão (Passa a bola para o cérebro fatiar e enviar)
		err = sender.SendFile(*file)
		if err != nil {
			fmt.Printf("Erro durante a transferência de dados: %v\n", err)
			// Perceba que NÃO damos os.Exit(1) aqui.
			// Se o envio falhar no meio, a gente tenta avisar o servidor mandando o FIN.
		}

		// Passo 3: Teardown (Manda o FIN e encerra)
		err = sender.Close()
		if err != nil {
			fmt.Printf("Erro ao fechar conexão (Teardown): %v\n", err)
			os.Exit(1)
		}
	}
}
