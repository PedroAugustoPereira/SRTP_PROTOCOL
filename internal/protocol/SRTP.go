package protocol

import (
	"errors"
	"hash/crc32"
)

func EncodeSRTP(packet *SRTPPPacket) ([]byte, error) {
	err := CheckHeader(packet)
	if err != nil {
		return nil, err
	}

	//Montando Header
	//Isso aqui nao foi IA QUE FEZ
	bufferHeader := make([]byte, 9)

	// Syn  - o bool no go nao pode ser passado pra byte (bem esquisito) então fiz assim
	// Não precisa do else pq ja é zero (antes eu tinha posto até entender isso)
	if packet.Header.SYN {
		bufferHeader[0] |= 1 << 7
	}

	// Fin - bit
	if packet.Header.FIN {
		bufferHeader[0] |= 1 << 6
	}

	// SEQ - 14 bits
	bufferHeader[0] |= byte(packet.Header.SEQ >> 8)
	bufferHeader[1] = byte(packet.Header.SEQ)

	// Ack Flag - 1 bit
	if packet.Header.ACKFlag {
		bufferHeader[2] |= 1 << 7
	}

	// Nack - 1 bit
	if packet.Header.NACK {
		bufferHeader[2] |= 1 << 6
	}

	//ACK - 14 bits
	bufferHeader[2] |= byte(packet.Header.ACK >> 8)
	bufferHeader[3] = byte(packet.Header.ACK)

	//Length - 8 bits
	bufferHeader[4] = byte(packet.Header.Length)

	result := append(bufferHeader, packet.Payload...)

	packet.Header.CRC32 = crc32.ChecksumIEEE(result)

	//CRC32 - 32 bits
	result[5] = byte(packet.Header.CRC32 >> 24)
	result[6] = byte(packet.Header.CRC32 >> 16)
	result[7] = byte(packet.Header.CRC32 >> 8)
	result[8] = byte(packet.Header.CRC32)

	return result, nil
}

func DecodeSRTP(buffer []byte) (*SRTPPPacket, error) {
	packet := &SRTPPPacket{}

	if len(buffer) < 9 {
		return nil, errors.New("Tamanho de pacote inválido")
	}

	//SYN
	packet.Header.SYN = ((buffer[0] & 0x80) != 0)
	//FIN
	packet.Header.FIN = ((buffer[0] & 0x40) != 0)
	//SEQ
	packet.Header.SEQ = uint16(buffer[0]&0x3F)<<8 | uint16(buffer[1])
	//ACK FLAG
	packet.Header.ACKFlag = ((buffer[2] & 0x80) != 0)
	//NACK
	packet.Header.NACK = ((buffer[2] & 0x40) != 0)
	//ACK
	packet.Header.ACK = uint16(buffer[2]&0x3F)<<8 | uint16(buffer[3])
	//Length
	packet.Header.Length = uint8(buffer[4])
	//CRC32
	packet.Header.CRC32 = uint32(buffer[5])<<24 | uint32(buffer[6])<<16 | uint32(buffer[7])<<8 | uint32(buffer[8])

	if len(buffer) > 9 {
		packet.Payload = buffer[9 : len(buffer)-1]
	}

	return packet, nil
}

func CheckHeader(packet *SRTPPPacket) error {
	if MaxLength < len(packet.Payload) {
		return errors.New("Tamanho de payload inválido")
	} else if MaxSEQ < packet.Header.SEQ {
		return errors.New("Tamanho de SEQ inválido")
	} else if MaxACK < packet.Header.ACK {
		return errors.New("Tamanho de ACK inválido")
	}

	return nil
}
