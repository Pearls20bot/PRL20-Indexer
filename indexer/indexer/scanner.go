package indexer

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"pearlsbot/internal/blockbook"
	"pearlsbot/internal/config"

	"github.com/btcsuite/btcd/wire"
)

// ScanAddress scans an address's transaction history and returns PRL20 balances.
// Checks cache first; on miss — scans transactions in parallel.
func ScanAddress(addr string, timeout time.Duration) (map[string]int64, error) {
	if cached, ok := Cache.Get(addr); ok {
		log.Printf("[IDX] cache HIT %s (age %s)", addr[:10], Cache.Age(addr))
		return cached, nil
	}

	log.Printf("[IDX] cache MISS %s, scanning...", addr[:10])
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	txids, err := blockbook.GetAllTxids(addr, config.MaxScanTx)
	if err != nil {
		return nil, err
	}
	log.Printf("[IDX] address %s: %d txids, starting %d goroutines",
		addr[:10], len(txids), config.ScanWorkers)

	start := time.Now()
	wmap := fetchWitnessesParallel(ctx, txids)
	log.Printf("[IDX] parallel fetch: %.1fs, %d txs with witness",
		time.Since(start).Seconds(), len(wmap))

	balances, ops := processWitnessMap(addr, wmap)
	log.Printf("[IDX] %s: %d operations, balances: %v", addr[:10], ops, balances)

	Cache.Set(addr, balances)
	return balances, nil
}

// fetchWitnessesParallel launches ScanWorkers goroutines to fetch witnesses for all transactions.
func fetchWitnessesParallel(ctx context.Context, txids []string) map[string][][][]byte {
	type result struct {
		txid      string
		witnesses [][][]byte
	}

	jobs := make(chan string, len(txids))
	for _, id := range txids {
		jobs <- id
	}
	close(jobs)

	results := make(chan result, len(txids))
	var wg sync.WaitGroup

	for i := 0; i < config.ScanWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for txid := range jobs {
				select {
				case <-ctx.Done():
					return
				default:
				}
				if wits := FetchWitnesses(txid); len(wits) > 0 {
					results <- result{txid: txid, witnesses: wits}
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	out := make(map[string][][][]byte, len(txids)/8)
	for r := range results {
		out[r.txid] = r.witnesses
	}
	return out
}

// processWitnessMap parses witness data and calculates balances for an address.
func processWitnessMap(addr string, wmap map[string][][][]byte) (map[string]int64, int) {
	balances := make(map[string]int64)
	ops := 0

	for _, witnesses := range wmap {
		for _, wit := range witnesses {
			if len(wit) < 3 {
				continue
			}
			for _, payload := range ExtractPRL20Payloads(wit[1]) {
				var op Op
				if err := json.Unmarshal(payload, &op); err != nil {
					continue
				}
				if strings.ToLower(op.P) != "prl-20" {
					continue
				}
				amt, _ := strconv.ParseInt(op.Amt, 10, 64)
				if amt <= 0 {
					continue
				}
				tick := strings.ToLower(op.Tick)
				switch strings.ToLower(op.Op) {
				case "mint":
					balances[tick] += amt
					ops++
				case "transfer":
					if strings.EqualFold(op.To, addr) {
						balances[tick] += amt
					} else {
						balances[tick] -= amt
					}
					ops++
				}
			}
		}
	}

	return cleanBalances(balances), ops
}

// FetchWitnesses tries three strategies to fetch witnesses for a transaction.
func FetchWitnesses(txid string) [][][]byte {
	return FetchWitnessesCtx(context.Background(), txid)
}

// FetchWitnessesCtx — context-aware version of FetchWitnesses.
func FetchWitnessesCtx(ctx context.Context, txid string) [][][]byte {
	// Strategy 1: /api/v2/rawtx
	if rawHex, err := blockbook.GetRawTxCtx(ctx, txid); err == nil && rawHex != "" {
		if wits := parseWitnessFromHex(rawHex); wits != nil {
			return wits
		}
	}
	if ctx.Err() != nil {
		return nil
	}

	// Strategies 2 and 3: /api/v2/tx
	det, err := blockbook.GetTxDetailCtx(ctx, txid)
	if err != nil {
		return nil
	}

	// Strategy 2: tx.Hex (full raw hex in response body)
	if det.Hex != "" {
		if wits := parseWitnessFromHex(det.Hex); wits != nil {
			return wits
		}
	}

	// Strategy 3: vin[i].Witness as JSON array of hex strings
	var result [][][]byte
	hasWitness := false
	for _, vin := range det.Vin {
		var wit [][]byte
		for _, whex := range vin.Witness {
			if b, err := hex.DecodeString(whex); err == nil {
				wit = append(wit, b)
			}
		}
		result = append(result, wit)
		if len(wit) > 0 {
			hasWitness = true
		}
	}
	if hasWitness {
		return result
	}
	return nil
}

// parseWitnessFromHex parses a raw transaction hex and returns witnesses for all inputs.
func parseWitnessFromHex(rawHex string) [][][]byte {
	txBytes, err := hex.DecodeString(rawHex)
	if err != nil {
		return nil
	}
	var tx wire.MsgTx
	if err := tx.Deserialize(bytes.NewReader(txBytes)); err != nil {
		return nil
	}
	result := make([][][]byte, len(tx.TxIn))
	for i, inp := range tx.TxIn {
		result[i] = [][]byte(inp.Witness)
	}
	return result
}

func cleanBalances(m map[string]int64) map[string]int64 {
	for k, v := range m {
		if v <= 0 {
			delete(m, k)
		}
	}
	return m
}
