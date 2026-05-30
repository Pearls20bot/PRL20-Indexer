package indexer

import (
	"encoding/json"
	"strings"
)

// NFTInfo stores on-chain PRC-721 token ownership.
type NFTInfo struct {
	Collection string `json:"collection"`
	TokenID    int    `json:"token_id"`
	Owner      string `json:"owner"`
	DeployHeight int64 `json:"deploy_height,omitempty"`
}

// NFTKey returns a unique key for collection+tokenID.
func NFTKey(collection string, tokenID int) string {
	return strings.ToLower(collection) + ":" + itoa(tokenID)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [12]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

// NFT721Op — parsed PRC-721 operation.
type NFT721Op struct {
	P          string `json:"p"`
	Op         string `json:"op"`
	Collection string `json:"collection"`
	ID         string `json:"id"`
	To         string `json:"to,omitempty"`
}

// ExtractProtocolPayloads extracts inscription payloads for a given protocol marker.
func ExtractProtocolPayloads(script []byte, protocol string) [][]byte {
	if len(script) < 34 || script[0] != 0x20 || script[33] != 0xac {
		return nil
	}
	protoBytes := []byte(protocol)
	if len(protoBytes) > 75 {
		return nil
	}

	var results [][]byte
	i := 34
	for i+1 < len(script) {
		if script[i] != 0x00 || script[i+1] != 0x63 {
			i++
			continue
		}
		i += 2

		if i+1 >= len(script) {
			break
		}
		protoLen := int(script[i])
		if protoLen <= 0 || i+1+protoLen > len(script) {
			continue
		}
		if string(script[i+1:i+1+protoLen]) != protocol {
			i++
			continue
		}
		i += 1 + protoLen

		// "application/json": 0x10 + 16 bytes
		if i+17 > len(script) || script[i] != 0x10 || string(script[i+1:i+17]) != "application/json" {
			continue
		}
		i += 17

		if i >= len(script) || script[i] != 0x00 {
			continue
		}
		i++

		if i >= len(script) {
			break
		}
		plen, hlen := scriptPushLen(script[i:])
		if plen <= 0 || hlen <= 0 {
			break
		}
		i += hlen
		if i+plen > len(script) {
			break
		}
		payload := make([]byte, plen)
		copy(payload, script[i:i+plen])
		results = append(results, payload)
		i += plen
		if i < len(script) && script[i] == 0x68 {
			i++
		}
	}
	return results
}

// ExtractPRL20Payloads extracts PRL20 payloads from a taproot envelope script.
func ExtractPRL20Payloads(script []byte) [][]byte {
	return ExtractProtocolPayloads(script, "prl-20")
}

// ExtractPRC721Payloads extracts PRC-721 payloads.
func ExtractPRC721Payloads(script []byte) [][]byte {
	return ExtractProtocolPayloads(script, "prc-721")
}

// ParseNFT721Op parses a PRC-721 JSON payload.
func ParseNFT721Op(payload []byte) (*NFT721Op, error) {
	var op NFT721Op
	if err := json.Unmarshal(payload, &op); err != nil {
		return nil, err
	}
	if strings.ToLower(op.P) != "prc-721" {
		return nil, nil
	}
	op.Collection = strings.ToLower(strings.TrimSpace(op.Collection))
	op.Op = strings.ToLower(strings.TrimSpace(op.Op))
	return &op, nil
}

// scriptPushLen decodes the payload length and header size from a script data push.
func scriptPushLen(data []byte) (payloadLen, headerLen int) {
	if len(data) == 0 {
		return 0, 0
	}
	b := data[0]
	switch {
	case b >= 0x01 && b <= 0x4b:
		return int(b), 1
	case b == 0x4c:
		if len(data) < 2 {
			return 0, 0
		}
		return int(data[1]), 2
	case b == 0x4d:
		if len(data) < 3 {
			return 0, 0
		}
		return int(data[1]) | int(data[2])<<8, 3
	default:
		return 0, 0
	}
}
