// Package indexer — PRL20 indexer: envelope script parsing, cache, scanner, global indexer.
package indexer

// Op — parsed PRL20 operation from a JSON payload.
type Op struct {
	P    string `json:"p"`
	Op   string `json:"op"`
	Tick string `json:"tick"`
	Amt  string `json:"amt,omitempty"`
	To   string `json:"to,omitempty"`
	Max  string `json:"max,omitempty"` // deploy: max supply
	Lim  string `json:"lim,omitempty"` // deploy: mint limit
	Pre  string `json:"pre,omitempty"` // deploy: premint
}
