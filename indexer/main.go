// Standalone Pearl PRL20 indexer binary.
//
// Run from the bot/ directory:
//
//	go run ./cmd/indexer
//
// The indexer scans blocks and saves state to indexer_snap.json
// (or INDEXER_STATE_FILE from environment).
// The bot reads from the same snapshot file and does not require a restart
// when the indexer is restarted.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"pearlsbot/internal/indexer"
)

func main() {
	log.SetFlags(log.LstdFlags)
	log.Println("[indexer] started as a separate process")

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	indexer.Global.Run(ctx)

	log.Println("[indexer] stopped")
}
