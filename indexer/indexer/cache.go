package indexer

import (
	"sync"
	"time"

	"pearlsbot/internal/config"
)

type cacheEntry struct {
	tokens    map[string]int64
	fetchedAt time.Time
}

type balanceCache struct {
	mu   sync.RWMutex
	data map[string]*cacheEntry
}

// Cache — global per-address PRL20 balance cache.
var Cache = &balanceCache{data: make(map[string]*cacheEntry)}

func (c *balanceCache) Get(addr string) (map[string]int64, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	e, ok := c.data[addr]
	if !ok || time.Since(e.fetchedAt) > config.CacheTTL {
		return nil, false
	}
	out := make(map[string]int64, len(e.tokens))
	for k, v := range e.tokens {
		out[k] = v
	}
	return out, true
}

func (c *balanceCache) Set(addr string, tokens map[string]int64) {
	cp := make(map[string]int64, len(tokens))
	for k, v := range tokens {
		cp[k] = v
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data[addr] = &cacheEntry{tokens: cp, fetchedAt: time.Now()}
}

// Age returns a string with the age of the cache entry, or "".
func (c *balanceCache) Age(addr string) string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	e, ok := c.data[addr]
	if !ok {
		return ""
	}
	d := time.Since(e.fetchedAt).Round(time.Second)
	if d < time.Minute {
		return "just now"
	}
	return d.String() + " ago"
}

// Invalidate removes an entry from the cache.
func (c *balanceCache) Invalidate(addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.data, addr)
}
