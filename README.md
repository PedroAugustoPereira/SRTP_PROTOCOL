# SRTP (Simple Reliable Transport Protocol)

Trabalho 2 da disciplina de Laboratório de Redes de Computadores e Fundamentos de Redes de computadores.
Implementação de um protocolo de transporte confiável sobre UDP com três variantes:
- Stop-and-Wait (SAW)
- Go-Back-N (GBN)
- Selective Repeat (SR)

## Estrutura do Projeto

- `cmd/SRTP/main.go`: Ponto de entrada do executável, com CLI e menu interativo.
- `internal/protocol/`: Definições do cabeçalho SRTP, codificação, decodificação e cálculo de CRC32.
- `internal/network/`: Implementação da lógica de rede (Sender, Receiver, Handshake, Teardown e Controladores GBN, SR, SAW).

## Como Executar

O projeto pode ser executado de três maneiras diferentes:

### 1. Usando o Executável Pré-Compilado (Windows / Linux)
Na raiz do projeto existe o executável `srtp.exe` compilado nativamente para Windows. Para utilizá-lo, basta abrir o terminal (PowerShell ou CMD) e rodar:

```powershell
.\srtp.exe --listen --port 6000 --mode sr
```
*(Nota: Embora o executável atual seja para Windows, o projeto pode ser compilado para Linux a qualquer momento rodando `GOOS=linux go build -o srtp cmd/SRTP/main.go` na raiz do projeto).*

### 2. Usando o Código-Fonte (`go run`)
Caso tenha a linguagem Go instalada (versão 1.18+), você pode executar o código diretamente:
```bash
go run cmd/SRTP/main.go [argumentos]
```

### 3. Usando o Makefile
Se possuir o GNU Make instalado, pode utilizar atalhos para os comandos de execução. Todos os parâmetros aceitam variáveis na linha de comando:
```bash
make server PORT=6000 MODE=sr
make client HOST=127.0.0.1 PORT=6000 FILE=teste.txt MODE=sr VERBOSE=false
```

## Modo Interativo

Para facilitar os testes, o programa foi estendido com um **Menu Interativo**. 
Ao iniciar o Receiver ou após concluir a transmissão no Sender, o programa exibirá um menu onde é possível trocar de papel (de Sender para Receiver ou vice-versa) sem precisar reiniciar a aplicação ou digitar novamente os argumentos da linha de comando.

```text
==========================================
         SRTP — Menu Interativo
==========================================
[1] Enviar arquivo (modo Sender)
[2] Receber arquivos (modo Receiver)
[3] Sair
==========================================
```

## Argumentos de Linha de Comando

| Argumento | Descrição | Exemplo |
|---|---|---|
| `--listen` | Inicia o executável em modo Receiver. | `--listen` |
| `--port` | (Obrigatório) Porta UDP onde o Receiver irá escutar. | `--port 6000` |
| `--host` | IP de destino (Obrigatório para o Sender). | `--host 192.168.0.6` |
| `--file` | Caminho do arquivo a ser enviado (Obrigatório para o Sender). | `--file arquivo.txt` |
| `--mode` | Variante do protocolo: `saw`, `gbn`, ou `sr`. Padrão é `saw`. | `--mode sr` |
| `--window` | Tamanho da janela para GBN e SR. Padrão é `16`. | `--window 4` |
| `--verbose` | Ativa os logs de pacote por pacote no console. Padrão é `true`. Pode ser desativado para acelerar a transferência de arquivos grandes. | `--verbose=false` |

## Exemplos de Execução

Você pode usar qualquer uma das três formas (`.\srtp.exe`, `go run` ou `make`) para rodar os exemplos abaixo. Usaremos o `.\srtp.exe` por ser o mais prático:

**1. Iniciando um Receiver em modo Selective Repeat (SR) na porta 6000:**
```powershell
.\srtp.exe --listen --port 6000 --mode sr
```

**2. Iniciando um Sender para enviar um arquivo para o Receiver acima:**
```powershell
.\srtp.exe --host 127.0.0.1 --port 6000 --file teste.txt --mode sr
```

**3. Testando tamanho de janela personalizado (ex: janela 4 no Go-Back-N):**
```powershell
.\srtp.exe --listen --port 6000 --mode gbn --window 4
.\srtp.exe --host 127.0.0.1 --port 6000 --file teste.txt --mode gbn --window 4
```

**4. Transferindo arquivos gigantes de forma rápida (desativando os logs de console):**
```powershell
.\srtp.exe --host 127.0.0.1 --port 6000 --file arquivo_gigante.mp4 --mode sr --verbose=false
```

## Funcionalidades Adicionais
- **Ordenação Automática:** Os arquivos recebidos são salvos na pasta raiz (ou com prefixo) de forma legível e perfeitamente ordenável (ex: `file_2026-06-21_17h46m19s.txt`).
- **Inferência de Tipo MIME:** O Receiver não altera o protocolo SRTP, mas inspeciona automaticamente a assinatura (*magic bytes*) do arquivo recém-recebido para salvar com a extensão correta (`.txt`, `.jpg`, `.pdf`, `.png`, `.zip`). Caso o tipo não seja identificado, salva como `.bin`.
