package indexer

// GlobalIndexer — background Pearl blockchain indexer.
//
// State storage: bbolt database indexer.db (not JSON).
// The indexer process (cmd/indexer) opens the DB read-write.
// The bot (main.go) reads a JSON snapshot written by the indexer.
//
// In-memory: full snapshot of balances and deploys for instant queries.

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"pearlsbot/internal/blockbook"
	"pearlsbot/internal/config"
)

// parallelFetch — number of blocks fetched in parallel.
const parallelFetch = 16

// Global — singleton global indexer.
var Global = newGlobalIndexer()

// ---------- DeployInfo ----------

// DeployInfo stores parameters of a deployed PRL20 token.
type DeployInfo struct {
	Tick         string `json:"tick"`
	MaxSupply    int64  `json:"max_supply"`
	MintLimit    int64  `json:"mint_limit"`
	Premint      int64  `json:"premint"`
	Deployer     string `json:"deployer"`
	TotalMinted  int64  `json:"total_minted"`
	DeployHeight int64  `json:"deploy_height"`
}

func (d *DeployInfo) Remaining() int64 {
	if r := d.MaxSupply - d.TotalMinted; r > 0 {
		return r
	}
	return 0
}

func (d *DeployInfo) IsMintable() bool {
	return d.Remaining() > 0 && d.MintLimit > 0
}

// ---------- globalIndexer ----------

type globalIndexer struct {
	mu         sync.RWMutex
	balances   map[string]map[string]int64
	deploys    map[string]*DeployInfo
	nfts       map[string]*NFTInfo
	height     int64
	bestHeight int64

	db *IndexerDB // nil when DB is unavailable (should not happen)
}

func newGlobalIndexer() *globalIndexer {
	return &globalIndexer{
		balances: make(map[string]map[string]int64),
		deploys:  make(map[string]*DeployInfo),
		nfts:     make(map[string]*NFTInfo),
	}
}

// ---------- Indexer process entry point ----------

// Run starts the full indexer loop (for cmd/indexer only).
func (g *globalIndexer) Run(ctx context.Context) {
	dbPath := config.IndexerDB
	if absPath, err2 := filepath.Abs(dbPath); err2 == nil {
		dbPath = absPath
	}
	log.Printf("[GI] opening DB %s…", dbPath)
	db, err := OpenDB(config.IndexerDB, false)
	if err != nil {
		log.Fatalf("[GI] failed to open DB: %v", err)
	}
	log.Printf("[GI] DB opened")
	defer db.Close()
	g.db = db

	log.Printf("[GI] loading state from DB…")
	height, bestHeight, deploys, balances, err := db.LoadAll()
	if err != nil {
		log.Printf("[GI] ❌ LoadAll: %v — starting from scratch", err)
	} else {
		log.Printf("[GI] LoadAll: height=%d bestHeight=%d deploys=%d addresses=%d",
			height, bestHeight, len(deploys), len(balances))
	}
	if height > 0 {
		g.mu.Lock()
		g.height, g.bestHeight = height, bestHeight
		g.deploys, g.balances = deploys, balances
		g.mu.Unlock()
		db.LogStats()
	}

	if g.height == 0 {
		log.Printf("[GI] first run — scanning from block 1")
	} else {
		log.Printf("[GI] resuming from block %d", g.height+1)
	}

	ticker := time.NewTicker(config.IndexerInterval)
	defer ticker.Stop()

	g.catchUp(ctx)
	for {
		select {
		case <-ctx.Done():
			g.mu.RLock()
			h, bh := g.height, g.bestHeight
			g.mu.RUnlock()
			if h > 0 && g.db != nil {
				if err := g.db.SaveHeight(h, bh); err != nil {
					log.Printf("[GI] ⚠️ final SaveHeight: %v", err)
				} else {
					log.Printf("[GI] 💾 saved: block %d/%d", h, bh)
				}
			}
			return
		case <-ticker.C:
			g.catchUp(ctx)
		}
	}
}

// ---------- Bot process entry point ----------

// loadFromSnapshot loads state from JSON snapshot (no bbolt, no file locks).
func (g *globalIndexer) loadFromSnapshot() bool {
	snap, err := ReadSnapshot()
	if err != nil {
		log.Printf("[GI] snapshot not found (%v) — waiting for indexer", err)
		return false
	}
	g.mu.Lock()
	g.height, g.bestHeight = snap.Height, snap.BestHeight
	g.deploys = snap.Deploys
	g.balances = snap.Balances
	if snap.NFTs != nil {
		g.nfts = snap.NFTs
	} else {
		g.nfts = make(map[string]*NFTInfo)
	}
	g.mu.Unlock()
	log.Printf("[GI] bot loaded snapshot: block %d/%d, deploys: %d, addresses: %d (at %s)",
		snap.Height, snap.BestHeight, len(snap.Deploys), len(snap.Balances),
		snap.UpdatedAt.Format("15:04:05"))
	return true
}

// LoadStatePublic loads the indexer snapshot for the bot.
// Returns true if loading succeeded.
func (g *globalIndexer) LoadStatePublic() bool {
	return g.loadFromSnapshot()
}

// WatchStateFile periodically re-reads the JSON snapshot from the indexer.
// Does not require bbolt and has no file locking issues.
func (g *globalIndexer) WatchStateFile(ctx context.Context, initialLoaded bool) {
	var lastHeight int64

	if !initialLoaded {
		retry := time.NewTicker(5 * time.Second)
		defer retry.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-retry.C:
				if g.loadFromSnapshot() {
					g.mu.RLock()
					lastHeight = g.height
					g.mu.RUnlock()
					goto watchLoop
				}
			}
		}
	} else {
		g.mu.RLock()
		lastHeight = g.height
		g.mu.RUnlock()
	}

watchLoop:
	var lastUpdatedAt time.Time
	g.mu.RLock()
	lastHeight = g.height
	g.mu.RUnlock()

	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			snap, err := ReadSnapshot()
			if err != nil {
				continue
			}
			if !snap.UpdatedAt.After(lastUpdatedAt) && snap.Height <= lastHeight {
				continue
			}
			lastUpdatedAt = snap.UpdatedAt
			lastHeight = snap.Height
			g.mu.Lock()
			g.height, g.bestHeight = snap.Height, snap.BestHeight
			g.deploys = snap.Deploys
			g.balances = snap.Balances
			if snap.NFTs != nil {
				g.nfts = snap.NFTs
			}
			g.mu.Unlock()
			log.Printf("[GI] bot updated snapshot: block %d/%d, deploys: %d",
				snap.Height, snap.BestHeight, len(snap.Deploys))
		}
	}
}

// ---------- Block processing ----------

func (g *globalIndexer) catchUp(ctx context.Context) {
	log.Printf("[GI] fetching bestblock…")
	best, err := blockbook.GetBestBlockCtx(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		log.Printf("[GI] ❌ bestblock: %v", err)
		return
	}
	log.Printf("[GI] bestblock = %d", best.Height)

	g.mu.Lock()
	g.bestHeight = best.Height
	from := g.height + 1
	g.mu.Unlock()

	if from > best.Height {
		log.Printf("[GI] already synced (height=%d, best=%d)", from-1, best.Height)
		g.writeSnapshot()
		return
	}

	total := best.Height - from + 1
	log.Printf("[GI] 🔍 start: %d blocks (%d → %d), parallelism: %d", total, from, best.Height, parallelFetch)

	startTime := time.Now()
	var totalOps int64

	type fetchResult struct {
		ops []blockOp
		err error
	}

	for chunkStart := from; chunkStart <= best.Height; {
		select {
		case <-ctx.Done():
			log.Printf("[GI] ⛔ stopped at block %d", chunkStart)
			return
		default:
		}

		chunkEnd := chunkStart + int64(parallelFetch) - 1
		if chunkEnd > best.Height {
			chunkEnd = best.Height
		}
		chunkSize := int(chunkEnd - chunkStart + 1)

		results := make([]fetchResult, chunkSize)
		var wg sync.WaitGroup
		for i := 0; i < chunkSize; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				ops, err := g.fetchBlockData(ctx, chunkStart+int64(idx))
				results[idx] = fetchResult{ops, err}
			}(i)
		}
		wg.Wait()

		abort := false
		chunkHadOps := false
		for i := 0; i < chunkSize; i++ {
			h := chunkStart + int64(i)
			if results[i].err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("[GI] ❌ block %d: %v", h, results[i].err)
				abort = true
				break
			}
			n := g.applyBlockData(results[i].ops, h, best.Height)
			totalOps += int64(n)
			if n > 0 {
				chunkHadOps = true
			}

			elapsed := time.Since(startTime)
			processed := h - from + 1
			remaining := best.Height - h
			var speed float64
			etaStr := "—"
			if elapsed.Seconds() > 0.5 && processed > 0 {
				speed = float64(processed) / elapsed.Seconds()
				if remaining > 0 {
					eta := time.Duration(float64(remaining)/speed) * time.Second
					etaStr = eta.Round(time.Second).String()
				} else {
					etaStr = "done"
				}
			}
			pct := float64(h) / float64(best.Height) * 100
			log.Printf("[GI] block %d/%d (%.1f%%) | %.0f blk/s | ETA: %s | ops: %d | deploys: %d",
				h, best.Height, pct, speed, etaStr, totalOps, len(g.deploys))
		}
		if abort {
			break
		}

		g.mu.RLock()
		curH := g.height
		g.mu.RUnlock()
		if chunkHadOps || curH%500 == 0 {
			g.writeSnapshot()
		}

		chunkStart = chunkEnd + 1
	}

	elapsed := time.Since(startTime)

	g.mu.RLock()
	lastH, lastBH := g.height, g.bestHeight
	g.mu.RUnlock()
	if lastH > 0 {
		if err := g.db.SaveHeight(lastH, lastBH); err != nil {
			log.Printf("[GI] ⚠️ SaveHeight: %v", err)
		}
	}

	g.db.LogStats()
	log.Printf("[GI] ✅ done in %s | ops: %d", elapsed.Round(time.Millisecond), totalOps)

	g.writeSnapshot()
}

// blockOp — one PRL20 operation found in a block.
type blockOp struct {
	payload []byte
	toAddr  string
	height  int64
	proto   string
}

// revealCandidate — transaction candidate for a PRL20 operation.
type revealCandidate struct {
	txid   string
	toAddr string
}

// fetchBlockData downloads a block and returns PRL20 operations without modifying state.
// Witnesses for reveal candidates are fetched in parallel.
func (g *globalIndexer) fetchBlockData(ctx context.Context, height int64) ([]blockOp, error) {
	var candidates []revealCandidate
	for page := int64(1); ; page++ {
		blk, err := blockbook.GetBlockCtx(ctx, height, page)
		if err != nil {
			return nil, fmt.Errorf("GetBlock(%d,%d): %w", height, page, err)
		}
		for _, tx := range blk.Txs {
			if !blockbook.IsRevealCandidate(tx) {
				continue
			}
			var toAddr string
			if len(tx.Vout) > 0 && len(tx.Vout[0].Addresses) > 0 {
				toAddr = tx.Vout[0].Addresses[0]
			}
			if toAddr == "" {
				continue
			}
			candidates = append(candidates, revealCandidate{txid: tx.Txid, toAddr: toAddr})
		}
		if page >= int64(blk.TotalPages) {
			break
		}
	}

	if len(candidates) == 0 {
		return nil, nil
	}

	type witResult struct {
		toAddr    string
		witnesses [][][]byte
	}
	results := make([]witResult, len(candidates))
	var wg sync.WaitGroup
	for i, c := range candidates {
		wg.Add(1)
		go func(idx int, cand revealCandidate) {
			defer wg.Done()
			results[idx] = witResult{
				toAddr:    cand.toAddr,
				witnesses: FetchWitnessesCtx(ctx, cand.txid),
			}
		}(i, c)
	}
	wg.Wait()

	var ops []blockOp
	for _, r := range results {
		for _, wit := range r.witnesses {
			if len(wit) < 3 {
				continue
			}
			payloads := ExtractPRL20Payloads(wit[1])
			if len(payloads) > config.MaxMintsPerTx {
				log.Printf("[GI] ⚠️ tx %s: %d payloads > MaxMintsPerTx(%d), truncating",
					r.toAddr[:min8(len(r.toAddr))], len(payloads), config.MaxMintsPerTx)
				payloads = payloads[:config.MaxMintsPerTx]
			}
			for _, payload := range payloads {
				ops = append(ops, blockOp{payload: payload, toAddr: r.toAddr, height: height, proto: "prl-20"})
			}
			nftPayloads := ExtractPRC721Payloads(wit[1])
			for _, payload := range nftPayloads {
				ops = append(ops, blockOp{payload: payload, toAddr: r.toAddr, height: height, proto: "prc-721"})
			}
		}
	}
	return ops, nil
}

func min8(n int) int {
	if n < 8 {
		return n
	}
	return 8
}

// applyBlockData applies a list of operations to memory and writes a batch to bbolt.
// Must be called strictly sequentially (modifies state).
func (g *globalIndexer) applyBlockData(ops []blockOp, height, bestHeight int64) int {
	type addrTick struct{ addr, tick string }
	dirtyBalances := map[addrTick]struct{}{}
	dirtyDeploys  := map[string]struct{}{} // tickers whose state changed
	opsFound := 0

	for _, op := range ops {
		if op.proto == "prc-721" {
			if g.applyNFT721Op(op.payload, op.toAddr, op.height) {
				opsFound++
			}
			continue
		}
		affected, deploy, dirtyTick := g.applyOpCollect(op.payload, op.toAddr, op.height)
		if affected != nil {
			opsFound++
			for _, at := range affected {
				dirtyBalances[addrTick{at[0], at[1]}] = struct{}{}
			}
			if deploy != nil {
				dirtyDeploys[deploy.Tick] = struct{}{}
			}
			if dirtyTick != "" {
				dirtyDeploys[dirtyTick] = struct{}{}
			}
		}
	}

	g.mu.RLock()
	balDeltas := make([]BalanceDelta, 0, len(dirtyBalances))
	for at := range dirtyBalances {
		var amt int64
		if g.balances[at.addr] != nil {
			amt = g.balances[at.addr][at.tick]
		}
		balDeltas = append(balDeltas, BalanceDelta{Addr: at.addr, Tick: at.tick, Amount: amt})
	}
	var deploySnapshots []*DeployInfo
	for tick := range dirtyDeploys {
		if d := g.deploys[tick]; d != nil {
			cp := *d
			deploySnapshots = append(deploySnapshots, &cp)
		}
	}
	g.mu.RUnlock()

	if err := g.db.ApplyBlock(BlockChanges{
		Height:     height,
		BestHeight: bestHeight,
		Deploys:    deploySnapshots,
		Balances:   balDeltas,
	}); err != nil {
		log.Printf("[GI] ❌ ApplyBlock(%d): %v", height, err)
	} else {
		g.mu.Lock()
		if height > g.height {
			g.height = height
		}
		g.mu.Unlock()
	}
	return opsFound
}

// applyOpCollect applies an operation to memory.
// Returns:
//   - affected  — list of touched [addr, tick] (nil = invalid operation)
//   - newDeploy — new deploy (only for op=deploy)
//   - dirtyTick — ticker whose TotalMinted changed (only for op=mint)
func (g *globalIndexer) applyOpCollect(payload []byte, senderAddr string, height int64) (affected [][2]string, newDeploy *DeployInfo, dirtyTick string) {
	var op Op
	if err := json.Unmarshal(payload, &op); err != nil {
		return nil, nil, ""
	}
	if strings.ToLower(op.P) != "prl-20" {
		return nil, nil, ""
	}
	tick := strings.ToLower(strings.TrimSpace(op.Tick))
	if tick == "" {
		return nil, nil, ""
	}

	switch strings.ToLower(op.Op) {
	case "deploy":
		g.mu.Lock()
		defer g.mu.Unlock()
		if _, exists := g.deploys[tick]; exists {
			return nil, nil, ""
		}
		max, _ := strconv.ParseInt(op.Max, 10, 64)
		lim, _ := strconv.ParseInt(op.Lim, 10, 64)
		pre, _ := strconv.ParseInt(op.Pre, 10, 64)
		if max <= 0 || lim <= 0 {
			return nil, nil, ""
		}
		dep := &DeployInfo{Tick: tick, MaxSupply: max, MintLimit: lim, Premint: pre,
			Deployer: senderAddr, TotalMinted: pre, DeployHeight: height}
		g.deploys[tick] = dep
		if pre > 0 {
			g.addNoLock(senderAddr, tick, pre)
			Cache.Invalidate(senderAddr)
		}
		log.Printf("[GI] DEPLOY %s max=%d lim=%d pre=%d by %s",
			strings.ToUpper(tick), max, lim, pre, short(senderAddr))
		if pre > 0 {
			return [][2]string{{senderAddr, tick}}, dep, tick
		}
		return [][2]string{}, dep, tick

	case "mint":
		amt, _ := strconv.ParseInt(op.Amt, 10, 64)
		if amt <= 0 {
			return nil, nil, ""
		}
		g.mu.Lock()
		defer g.mu.Unlock()
		dep, ok := g.deploys[tick]
		if !ok {
			return nil, nil, ""
		}
		if amt > dep.MintLimit {
			amt = dep.MintLimit
		}
		if dep.TotalMinted+amt > dep.MaxSupply {
			amt = dep.MaxSupply - dep.TotalMinted
		}
		if amt <= 0 {
			return nil, nil, ""
		}
		dep.TotalMinted += amt
		g.addNoLock(senderAddr, tick, amt)
		Cache.Invalidate(senderAddr)
		log.Printf("[GI] MINT %s %s +%d (total %d/%d)",
			short(senderAddr), strings.ToUpper(tick), amt, dep.TotalMinted, dep.MaxSupply)
		return [][2]string{{senderAddr, tick}}, nil, tick

	case "transfer":
		amt, _ := strconv.ParseInt(op.Amt, 10, 64)
		to := strings.TrimSpace(op.To)
		if amt <= 0 || to == "" {
			return nil, nil, ""
		}
		g.mu.Lock()
		defer g.mu.Unlock()
		if _, ok := g.deploys[tick]; !ok {
			return nil, nil, ""
		}
		senderBal := int64(0)
		if g.balances[senderAddr] != nil {
			senderBal = g.balances[senderAddr][tick]
		}
		if amt > senderBal {
			return nil, nil, ""
		}
		g.addNoLock(to, tick, amt)
		Cache.Invalidate(to)
		g.addNoLock(senderAddr, tick, -amt)
		Cache.Invalidate(senderAddr)
		log.Printf("[GI] TRANSFER %s→%s %s %d",
			short(senderAddr), short(to), strings.ToUpper(tick), amt)
		return [][2]string{{to, tick}, {senderAddr, tick}}, nil, ""
	}
	return nil, nil, ""
}

func (g *globalIndexer) addNoLock(addr, tick string, delta int64) {
	if g.balances[addr] == nil {
		g.balances[addr] = make(map[string]int64)
	}
	g.balances[addr][tick] += delta
	if g.balances[addr][tick] <= 0 {
		delete(g.balances[addr], tick)
		if len(g.balances[addr]) == 0 {
			delete(g.balances, addr)
		}
	}
}

func (g *globalIndexer) applyNFT721Op(payload []byte, senderAddr string, height int64) bool {
	nftOp, err := ParseNFT721Op(payload)
	if err != nil || nftOp == nil || nftOp.Collection == "" {
		return false
	}
	tokenID, _ := strconv.Atoi(strings.TrimSpace(nftOp.ID))
	if tokenID < 0 {
		return false
	}
	key := NFTKey(nftOp.Collection, tokenID)

	g.mu.Lock()
	defer g.mu.Unlock()

	switch nftOp.Op {
	case "mint", "deploy":
		if _, exists := g.nfts[key]; exists {
			return false
		}
		g.nfts[key] = &NFTInfo{
			Collection:   nftOp.Collection,
			TokenID:      tokenID,
			Owner:        senderAddr,
			DeployHeight: height,
		}
		log.Printf("[GI] NFT MINT %s #%d by %s", nftOp.Collection, tokenID, short(senderAddr))
		return true
	case "transfer":
		info, ok := g.nfts[key]
		if !ok || info.Owner != senderAddr {
			return false
		}
		to := strings.TrimSpace(nftOp.To)
		if to == "" {
			return false
		}
		info.Owner = to
		log.Printf("[GI] NFT TRANSFER %s #%d %s→%s", nftOp.Collection, tokenID, short(senderAddr), short(to))
		return true
	}
	return false
}

func (g *globalIndexer) GetNFT(collection string, tokenID int) *NFTInfo {
	g.mu.RLock()
	defer g.mu.RUnlock()
	n := g.nfts[NFTKey(collection, tokenID)]
	if n == nil {
		return nil
	}
	cp := *n
	return &cp
}

func (g *globalIndexer) GetNFTsByOwner(addr string) []*NFTInfo {
	g.mu.RLock()
	defer g.mu.RUnlock()
	var out []*NFTInfo
	for _, n := range g.nfts {
		if n.Owner == addr {
			cp := *n
			out = append(out, &cp)
		}
	}
	return out
}

func (g *globalIndexer) GetNFTsByCollection(collection string) []*NFTInfo {
	g.mu.RLock()
	defer g.mu.RUnlock()
	col := strings.ToLower(collection)
	var out []*NFTInfo
	for _, n := range g.nfts {
		if n.Collection == col {
			cp := *n
			out = append(out, &cp)
		}
	}
	return out
}

// ---------- Public API ----------

// HolderRow is one address holding a ticker, with its on-chain token balance.
type HolderRow struct {
	Addr   string `json:"addr"`
	Amount int64  `json:"amount"`
}

// GetTopHolders returns holders of a ticker sorted by balance (desc), truncated
// to limit, together with the total holder count. Balances reflect raw on-chain
// PRL-20 state (escrowed/listed tokens currently sit on the escrow address).
func (g *globalIndexer) GetTopHolders(tick string, limit int) (rows []HolderRow, holderCount int) {
	tick = strings.ToLower(strings.TrimSpace(tick))
	if tick == "" {
		return nil, 0
	}
	g.mu.RLock()
	for addr, toks := range g.balances {
		if amt := toks[tick]; amt > 0 {
			rows = append(rows, HolderRow{Addr: addr, Amount: amt})
		}
	}
	g.mu.RUnlock()
	holderCount = len(rows)
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Amount == rows[j].Amount {
			return rows[i].Addr < rows[j].Addr
		}
		return rows[i].Amount > rows[j].Amount
	})
	if limit > 0 && len(rows) > limit {
		rows = rows[:limit]
	}
	return rows, holderCount
}

func (g *globalIndexer) GetBalance(addr string) map[string]int64 {
	g.mu.RLock()
	defer g.mu.RUnlock()
	toks := g.balances[addr]
	out := make(map[string]int64, len(toks))
	for k, v := range toks {
		out[k] = v
	}
	return out
}

func (g *globalIndexer) CaughtUp() bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.bestHeight > 0 && g.height >= g.bestHeight
}

func (g *globalIndexer) Progress() string {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if g.bestHeight == 0 {
		return "waiting..."
	}
	return fmt.Sprintf("%d/%d (%.1f%%)", g.height, g.bestHeight,
		float64(g.height)/float64(g.bestHeight)*100)
}

func (g *globalIndexer) IsTickerAvailable(tick string) bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	_, exists := g.deploys[strings.ToLower(tick)]
	return !exists
}

func (g *globalIndexer) GetDeploy(tick string) *DeployInfo {
	g.mu.RLock()
	defer g.mu.RUnlock()
	d := g.deploys[strings.ToLower(tick)]
	if d == nil {
		return nil
	}
	cp := *d
	return &cp
}

func (g *globalIndexer) GetAllDeploys() []*DeployInfo {
	g.mu.RLock()
	defer g.mu.RUnlock()
	out := make([]*DeployInfo, 0, len(g.deploys))
	for _, d := range g.deploys {
		cp := *d
		out = append(out, &cp)
	}
	return out
}

func (g *globalIndexer) GetMintableTokens() []*DeployInfo {
	g.mu.RLock()
	defer g.mu.RUnlock()
	var out []*DeployInfo
	for _, d := range g.deploys {
		if d.IsMintable() {
			cp := *d
			out = append(out, &cp)
		}
	}
	return out
}

// ---------- GetPRL20Balances ----------

func GetPRL20Balances(addr string, timeout time.Duration) (map[string]int64, string) {
	if Global.CaughtUp() {
		return Global.GetBalance(addr), "global indexer ✅"
	}
	tokens, err := ScanAddress(addr, timeout)
	if err != nil {
		log.Printf("[IDX] ScanAddress(%s): %v", short(addr), err)
		return map[string]int64{}, "scan error"
	}
	return tokens, fmt.Sprintf("address scan | indexer %s", Global.Progress())
}

func short(s string) string {
	if len(s) > 12 {
		return s[:12]
	}
	return s
}
