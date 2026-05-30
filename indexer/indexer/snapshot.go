package indexer

// snapshot.go — compact JSON snapshot of indexer state for IPC with the bot.
//
// The indexer writes indexer_snap.json after each catchUp.
// The bot reads it in WatchStateFile — no bbolt, no file locks.

import (
	"encoding/json"
	"log"
	"os"
	"sync/atomic"
	"time"
	"unsafe"

	"pearlsbot/internal/config"
)

// Snapshot — serializable indexer state.
type Snapshot struct {
	Height     int64                       `json:"height"`
	BestHeight int64                       `json:"best_height"`
	UpdatedAt  time.Time                   `json:"updated_at"`
	Deploys    map[string]*DeployInfo      `json:"deploys"`
	Balances   map[string]map[string]int64 `json:"balances"`
	NFTs       map[string]*NFTInfo         `json:"nfts,omitempty"`
}

// snapshotPtr — atomic pointer to the latest snapshot (for lock-free reads).
var snapshotPtr unsafe.Pointer

// writeSnapshot creates a snapshot of the current state and atomically writes it to file.
func (g *globalIndexer) writeSnapshot() {
	g.mu.RLock()
	snap := &Snapshot{
		Height:     g.height,
		BestHeight: g.bestHeight,
		UpdatedAt:  time.Now(),
		Deploys:    make(map[string]*DeployInfo, len(g.deploys)),
		Balances:   make(map[string]map[string]int64, len(g.balances)),
	}
	for k, v := range g.deploys {
		cp := *v
		snap.Deploys[k] = &cp
	}
	for addr, toks := range g.balances {
		m := make(map[string]int64, len(toks))
		for t, amt := range toks {
			m[t] = amt
		}
		snap.Balances[addr] = m
	}
	snap.NFTs = make(map[string]*NFTInfo, len(g.nfts))
	for k, v := range g.nfts {
		cp := *v
		snap.NFTs[k] = &cp
	}
	g.mu.RUnlock()

	// Update in-memory pointer (for reads without disk I/O).
	atomic.StorePointer(&snapshotPtr, unsafe.Pointer(snap))

	// Write atomically: tmp file first, then rename.
	data, err := json.Marshal(snap)
	if err != nil {
		log.Printf("[GI] snapshot marshal: %v", err)
		return
	}
	tmp := config.IndexerSnapshot + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		log.Printf("[GI] snapshot write: %v", err)
		return
	}
	if err := os.Rename(tmp, config.IndexerSnapshot); err != nil {
		log.Printf("[GI] snapshot rename: %v", err)
		return
	}
	log.Printf("[GI] 📸 snapshot written: block %d/%d, deploys: %d, addresses: %d",
		snap.Height, snap.BestHeight, len(snap.Deploys), len(snap.Balances))
}

// ReadSnapshot reads the snapshot from file (called by bot).
func ReadSnapshot() (*Snapshot, error) {
	data, err := os.ReadFile(config.IndexerSnapshot)
	if err != nil {
		return nil, err
	}
	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, err
	}
	return &snap, nil
}
