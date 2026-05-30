// db.go — bbolt wrapper for storing PRL20 indexer state.
//
// Database structure:
//   - Bucket "meta":
//       "height"      → uint64 big-endian
//       "best_height" → uint64 big-endian
//   - Bucket "deploys":
//       tick (string) → JSON(DeployInfo)
//   - Bucket "balances":
//       "addr:tick"   → int64 big-endian
//
// The indexer opens the DB read-write.
// The bot opens it read-only (safe cross-process).
package indexer

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"go.etcd.io/bbolt"
)

var (
	bktMeta     = []byte("meta")
	bktDeploys  = []byte("deploys")
	bktBalances = []byte("balances")

	keyHeight     = []byte("height")
	keyBestHeight = []byte("best_height")
)

// IndexerDB — wrapper around bbolt.DB.
type IndexerDB struct {
	db *bbolt.DB
}

// OpenDB opens (or creates) the indexer database.
// readOnly=true: safe to open concurrently with the writer process (bot).
func OpenDB(path string, readOnly bool) (*IndexerDB, error) {
	timeout := 10 * time.Second // enough to wait for indexer write transaction
	if !readOnly {
		timeout = 3 * time.Second // writer should not wait long
	}
	opts := &bbolt.Options{
		ReadOnly: readOnly,
		Timeout:  timeout,
	}
	db, err := bbolt.Open(path, 0600, opts)
	if err != nil {
		return nil, fmt.Errorf("open bbolt %s: %w", path, err)
	}
	if !readOnly {
		// Create buckets on first run.
		err = db.Update(func(tx *bbolt.Tx) error {
			for _, name := range [][]byte{bktMeta, bktDeploys, bktBalances} {
				if _, err := tx.CreateBucketIfNotExists(name); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return &IndexerDB{db: db}, nil
}

// Close closes the database.
func (d *IndexerDB) Close() error {
	return d.db.Close()
}

// ---------- Write (indexer only) ----------

// SaveMeta saves the current block height.
func (d *IndexerDB) SaveMeta(height, bestHeight int64) error {
	return d.db.Update(func(tx *bbolt.Tx) error {
		meta := tx.Bucket(bktMeta)
		meta.Put(keyHeight, i64tob(height))
		meta.Put(keyBestHeight, i64tob(bestHeight))
		return nil
	})
}

// BlockChanges — set of changes for one block.
type BlockChanges struct {
	Height     int64
	BestHeight int64
	Deploys    []*DeployInfo
	Balances   []BalanceDelta
}

// BalanceDelta — balance change for a specific address/ticker.
type BalanceDelta struct {
	Addr   string
	Tick   string
	Amount int64 // final value (not a delta)
}

// heightCommitEvery — how often to commit height to bbolt for empty blocks.
// Real changes (deploys/balances) are always written immediately.
const heightCommitEvery = 200

// ApplyBlock writes block changes to bbolt.
// If there are no PRL20 changes — writes only height every heightCommitEvery blocks.
func (d *IndexerDB) ApplyBlock(changes BlockChanges) error {
	hasChanges := len(changes.Deploys) > 0 || len(changes.Balances) > 0
	if !hasChanges && changes.Height%heightCommitEvery != 0 {
		return nil
	}
	return d.db.Update(func(tx *bbolt.Tx) error {
		meta := tx.Bucket(bktMeta)
		meta.Put(keyHeight, i64tob(changes.Height))
		meta.Put(keyBestHeight, i64tob(changes.BestHeight))

		dep := tx.Bucket(bktDeploys)
		for _, info := range changes.Deploys {
			data, _ := json.Marshal(info)
			dep.Put([]byte(info.Tick), data)
		}

		bal := tx.Bucket(bktBalances)
		for _, bd := range changes.Balances {
			key := []byte(bd.Addr + ":" + bd.Tick)
			if bd.Amount <= 0 {
				bal.Delete(key)
			} else {
				bal.Put(key, i64tob(bd.Amount))
			}
		}
		return nil
	})
}

// SaveHeight forcefully writes the current height (called on shutdown).
func (d *IndexerDB) SaveHeight(height, bestHeight int64) error {
	return d.db.Update(func(tx *bbolt.Tx) error {
		meta := tx.Bucket(bktMeta)
		meta.Put(keyHeight, i64tob(height))
		meta.Put(keyBestHeight, i64tob(bestHeight))
		return nil
	})
}

// ---------- Read ----------

// LoadAll loads the full state from the database.
func (d *IndexerDB) LoadAll() (height, bestHeight int64, deploys map[string]*DeployInfo, balances map[string]map[string]int64, err error) {
	deploys = make(map[string]*DeployInfo)
	balances = make(map[string]map[string]int64)

	err = d.db.View(func(tx *bbolt.Tx) error {
		if meta := tx.Bucket(bktMeta); meta != nil {
			if v := meta.Get(keyHeight); v != nil {
				height = btoi64(v)
			}
			if v := meta.Get(keyBestHeight); v != nil {
				bestHeight = btoi64(v)
			}
		}

		if dep := tx.Bucket(bktDeploys); dep != nil {
			dep.ForEach(func(k, v []byte) error { //nolint:errcheck
				var info DeployInfo
				if jsonErr := json.Unmarshal(v, &info); jsonErr == nil {
					deploys[string(k)] = &info
				}
				return nil
			})
		}

		if bal := tx.Bucket(bktBalances); bal != nil {
			bal.ForEach(func(k, v []byte) error { //nolint:errcheck
				key := string(k)
				idx := strings.LastIndex(key, ":")
				if idx < 0 {
					return nil
				}
				addr, tick := key[:idx], key[idx+1:]
				if balances[addr] == nil {
					balances[addr] = make(map[string]int64)
				}
				balances[addr][tick] = btoi64(v)
				return nil
			})
		}
		return nil
	})
	return
}

// GetBalance returns balances for a specific address directly from the DB (without loading into memory).
func (d *IndexerDB) GetBalance(addr string) map[string]int64 {
	result := make(map[string]int64)
	prefix := []byte(addr + ":")
	_ = d.db.View(func(tx *bbolt.Tx) error {
		bal := tx.Bucket(bktBalances)
		if bal == nil {
			return nil
		}
		c := bal.Cursor()
		for k, v := c.Seek(prefix); k != nil && strings.HasPrefix(string(k), string(prefix)); k, v = c.Next() {
			tick := string(k[len(prefix):])
			result[tick] = btoi64(v)
		}
		return nil
	})
	return result
}

// ---------- Utilities ----------

func i64tob(v int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

func btoi64(b []byte) int64 {
	if len(b) < 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(b))
}

// LogStats prints database statistics to the log.
func (d *IndexerDB) LogStats() {
	_ = d.db.View(func(tx *bbolt.Tx) error {
		if bal := tx.Bucket(bktBalances); bal != nil {
			s := bal.Stats()
			log.Printf("[DB] balances: %d records, ~%d KB", s.KeyN, s.BranchInuse/1024+s.LeafInuse/1024)
		}
		if dep := tx.Bucket(bktDeploys); dep != nil {
			s := dep.Stats()
			log.Printf("[DB] deploys:  %d tokens", s.KeyN)
		}
		return nil
	})
}
