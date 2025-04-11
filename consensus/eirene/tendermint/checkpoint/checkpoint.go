package checkpoint

import (
	"math/big"

	"github.com/zenanetwork/go-zenanet/common"
)

// Checkpoint defines a response object type of eirene checkpoint
type Checkpoint struct {
	Proposer   common.Address `json:"proposer"`
	StartBlock *big.Int       `json:"start_block"`
	EndBlock   *big.Int       `json:"end_block"`
	RootHash   common.Hash    `json:"root_hash"`
	EireneChainID string         `json:"eirene_chain_id"`
	Timestamp  uint64         `json:"timestamp"`
}

type CheckpointResponse struct {
	Height string     `json:"height"`
	Result Checkpoint `json:"result"`
}

type CheckpointCount struct {
	Result int64 `json:"result"`
}

type CheckpointCountResponse struct {
	Height string          `json:"height"`
	Result CheckpointCount `json:"result"`
}
