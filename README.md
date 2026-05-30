## Pearl Indexer

A standalone background service that indexes the **Pearl** UTXO chain and reconstructs off-chain state for inscription protocols. It connects to **Blockbook**, walks new blocks in parallel, parses **PRL-20** (`deploy`, `mint`, `transfer`) and **PRC-721** operations from taproot inscriptions, and maintains:

- per-address **PRL-20 balances** (`address → ticker → amount`)
- **deploy metadata** (max supply, mint limit, total minted, mintable status)
- **NFT ownership** (PRC-721)
