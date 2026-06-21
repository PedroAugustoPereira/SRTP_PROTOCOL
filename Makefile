# ==========================================
# Variáveis Padrão (Você pode alterar na hora de rodar)
# ==========================================
PORT ?= 6000
HOST ?= 192.168.0.6
FILE ?= teste.txt
MODE ?= saw
VERBOSE ?= true
MAIN_PATH = cmd/SRTP/main.go

# ==========================================
# Comandos
# ==========================================

# Roda o Receiver (Servidor)
server:
	go run $(MAIN_PATH) --listen --port $(PORT) --mode $(MODE) --verbose=$(VERBOSE)

# Roda o Sender (Cliente)
client:
	go run $(MAIN_PATH) --host $(HOST) --port $(PORT) --file $(FILE) --mode $(MODE) --verbose=$(VERBOSE)

# Gera o executável final (Para quando o projeto estiver pronto)
build:
	go build -o bin/srtp $(MAIN_PATH)
	@echo "Executável gerado na pasta bin/"

# Exibe a ajuda
help:
	@echo "Comandos disponíveis:"
	@echo "  make server  - Roda o receiver na porta padrao"
	@echo "  make client  - Roda o sender enviando o arquivo padrao"
	@echo "  make build   - Compila o binário do projeto"