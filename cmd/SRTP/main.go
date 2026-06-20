package main

import (
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
	} else {
		// --- MODO SENDER ---
		if *host == "" || *file == "" {
			fmt.Println("Erro: Modo sender exige --host e --file.")
			os.Exit(1)
		}
		fmt.Printf("Iniciando Sender: %s:%d, enviando %s\n", *host, *port, *file)
	}
}
