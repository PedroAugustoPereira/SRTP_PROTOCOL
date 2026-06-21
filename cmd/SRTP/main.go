package main

import (
	"SRTP/internal/network"
	"flag"
	"fmt"
	"os"
)

func main() {
	listen := flag.Bool("listen", false, "Modo receiver")
	port := flag.Int("port", 0, "Porta de operação")
	host := flag.String("host", "", "IP do host (apenas para sender)")
	file := flag.String("file", "", "Arquivo para transferir (apenas para sender)")

	flag.Parse()

	if *port == 0 {
		fmt.Println("Erro: A porta (--port) é obrigatória.")
		os.Exit(1)
	}

	if *listen {
		// --- MODO RECEIVER ---
		fmt.Printf("Iniciando em modo Receiver na porta %d...\n", *port)

		// Instancia o Receiver vazio (o mapa de sessões é criado lá no Listen)
		receiver := network.Receiver{}

		// Inicia o loop infinito do Maestro
		err := receiver.Listen(*port)
		if err != nil {
			fmt.Printf("Erro crítico no servidor: %v\n", err)
			os.Exit(1)
		}
	} else {
		// --- MODO SENDER ---
		if *host == "" || *file == "" {
			fmt.Println("Erro: Modo sender exige --host e --file.")
			os.Exit(1)
		}

		fmt.Printf("Iniciando Sender: %s:%d, enviando %s\n", *host, *port, *file)

		sender := network.Sender{
			File: *file,
			Session: &network.SRTSession{
				State: network.StateClosed,
			},
		}

		err := sender.Dial(*host, *port)
		if err != nil {
			fmt.Printf("Erro na conexão: %v\n", err)
			os.Exit(1)
		}
	}
}
