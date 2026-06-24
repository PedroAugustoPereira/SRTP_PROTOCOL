package main

import (
	"SRTP/internal/network"
	"bufio"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
)

var scanner = bufio.NewScanner(os.Stdin)

func main() {
	listen := flag.Bool("listen", false, "Modo receiver")
	port := flag.Int("port", 0, "Porta de operação")
	host := flag.String("host", "", "IP do host (apenas para sender)")
	file := flag.String("file", "", "Arquivo para transferir (apenas para sender)")
	mode := flag.String("mode", "saw", "Modo do protocolo: saw, gbn ou sr")
	window := flag.Int("window", 16, "Tamanho da janela (GBN e SR) - Padrão 16")
	verbose := flag.Bool("verbose", true, "Imprimir logs detalhados de cada pacote")

	flag.Parse()

	network.Verbose = *verbose

	if *port == 0 {
		fmt.Println("Erro: A porta (--port) é obrigatória.")
		os.Exit(1)
	}

	if *mode != "saw" && *mode != "gbn" && *mode != "sr" {
		fmt.Println("Erro: Modo inválido. Use 'saw', 'gbn' ou 'sr'.")
		os.Exit(1)
	}

	if *listen {
		executeInteractiveReceiver(*port, *mode, *window)
	} else {
		if *host == "" || *file == "" {
			fmt.Println("Erro: Modo sender exige --host e --file.")
			os.Exit(1)
		}
		executeSender(*host, *port, *file, *mode, *window)
	}

	interactiveMenu(port, host, file, mode, window)
}

func interactiveMenu(port *int, host *string, file *string, mode *string, window *int) {
	for {
		fmt.Println()
		fmt.Println("==========================================")
		fmt.Println("         SRTP - Menu Interativo")
		fmt.Println("==========================================")
		fmt.Println("[1] Enviar arquivo (modo Sender)")
		fmt.Println("[2] Receber arquivos (modo Receiver)")
		fmt.Println("[3] Sair")
		fmt.Println("==========================================")
		fmt.Print("Escolha: ")

		switch readLine() {
		case "1":
			senderHost := askValue("Host", *host)
			senderPortStr := askValue("Porta", strconv.Itoa(*port))
			senderPort, err := strconv.Atoi(senderPortStr)
			if err != nil {
				fmt.Println("Porta inválida!")
				continue
			}
			senderFile := askValue("Arquivo", *file)
			senderMode := askValue("Modo (saw/gbn/sr)", *mode)

			if senderMode != "saw" && senderMode != "gbn" && senderMode != "sr" {
				fmt.Println("Modo inválido!")
				continue
			}

			senderWindowStr := askValue("Tamanho da Janela", strconv.Itoa(*window))
			senderWindow, err := strconv.Atoi(senderWindowStr)
			if err != nil {
				senderWindow = 16
			}

			executeSender(senderHost, senderPort, senderFile, senderMode, senderWindow)

		case "2":
			recvPortStr := askValue("Porta", strconv.Itoa(*port))
			recvPort, err := strconv.Atoi(recvPortStr)
			if err != nil {
				fmt.Println("Porta inválida!")
				continue
			}
			recvMode := askValue("Modo (saw/gbn/sr)", *mode)
			if recvMode != "saw" && recvMode != "gbn" && recvMode != "sr" {
				fmt.Println("Modo inválido!")
				continue
			}
			recvWindowStr := askValue("Tamanho da Janela", strconv.Itoa(*window))
			recvWindow, err := strconv.Atoi(recvWindowStr)
			if err != nil {
				recvWindow = 16
			}
			executeInteractiveReceiver(recvPort, recvMode, recvWindow)

		case "3":
			fmt.Println("Encerrando. Até mais!")
			return

		default:
			fmt.Println("Opção inválida.")
		}
	}
}

func executeSender(host string, port int, filePath string, mode string, windowSize int) {
	fmt.Printf("\nSender conectando a %s:%d, arquivo '%s', modo %s, janela %d\n", host, port, filePath, mode, windowSize)

	var controller network.FlowController
	switch mode {
	case "saw":
		controller = &network.StopAndWaitController{}
	case "gbn":
		controller = &network.GoBackNController{}
	case "sr":
		controller = &network.SelectiveRepeatController{}
	}

	sender := network.Sender{
		File: filePath,
		Session: &network.SRTSession{
			State:      network.StateClosed,
			Controller: controller,
			WindowSize: uint8(windowSize),
		},
	}

	err := sender.Dial(host, port)
	if err != nil {
		fmt.Printf("Erro no Handshake: %v\n", err)
		return
	}

	err = sender.SendFile(filePath)
	if err != nil {
		fmt.Printf("Erro na transferência: %v\n", err)
		// Fecha a conexão educadamente para não deixar o receiver travado esperando
		sender.Close()
		return
	}

	err = sender.Close()
	if err != nil {
		fmt.Printf("Erro no Teardown: %v\n", err)
		return
	}

	fmt.Println("\n[SUCESSO] Transferência concluída!")
}

// Roda o receiver numa goroutine e monitora o teclado.
// Se o usuário digitar 'S' + Enter, para o receiver e volta ao menu.
func executeInteractiveReceiver(port int, mode string, windowSize int) {
	fmt.Printf("\nReceiver escutando na porta %d, modo %s...\n", port, mode)
	fmt.Println(">>> Digite [S] + Enter a qualquer momento para parar e trocar para Sender <<<")
	fmt.Println()

	receiver := &network.Receiver{Mode: mode, WindowSize: uint8(windowSize)}

	// Receiver roda numa goroutine separada
	done := make(chan error, 1)
	go func() {
		done <- receiver.Listen(port)
	}()

	// Teclado roda em outra goroutine
	keyChannel := make(chan string, 1)
	go func() {
		for {
			line := readLine()
			if strings.ToUpper(line) == "S" {
				keyChannel <- "S"
				return
			}
		}
	}()

	// Espera: ou o receiver termina sozinho, ou o usuário digita 'S'
	select {
	case err := <-done:
		if err != nil {
			fmt.Printf("Erro no receiver: %v\n", err)
		}
	case <-keyChannel:
		fmt.Println("\nParando o receiver...")
		receiver.Stop()
		<-done // espera o Listen() terminar
	}
}

func readLine() string {
	scanner.Scan()
	return strings.TrimSpace(scanner.Text())
}

func askValue(label string, defaultValue string) string {
	if defaultValue != "" && defaultValue != "0" {
		fmt.Printf("%s [%s]: ", label, defaultValue)
	} else {
		fmt.Printf("%s: ", label)
	}
	response := readLine()
	if response == "" {
		return defaultValue
	}
	return response
}
