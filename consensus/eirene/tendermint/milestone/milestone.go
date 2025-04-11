package milestone

import (
	"math/big"

	"github.com/zenanetwork/go-zenanet/common"
)

// milestone defines a response object type of eirene milestone
type Milestone struct {
	Proposer   common.Address `json:"proposer"`
	StartBlock *big.Int       `json:"start_block"`
	EndBlock   *big.Int       `json:"end_block"`
	Hash       common.Hash    `json:"hash"`
	EireneChainID string         `json:"eirene_chain_id"`
	Timestamp  uint64         `json:"timestamp"`
}

type MilestoneResponse struct {
	Height string    `json:"height"`
	Result Milestone `json:"result"`
}

type MilestoneCount struct {
	Count int64 `json:"count"`
}

type MilestoneCountResponse struct {
	Height string         `json:"height"`
	Result MilestoneCount `json:"result"`
}

type MilestoneLastNoAck struct {
	Result string `json:"result"`
}

type MilestoneLastNoAckResponse struct {
	Height string             `json:"height"`
	Result MilestoneLastNoAck `json:"result"`
}

type MilestoneNoAck struct {
	Result bool `json:"result"`
}

type MilestoneNoAckResponse struct {
	Height string         `json:"height"`
	Result MilestoneNoAck `json:"result"`
}

type MilestoneID struct {
	Result bool `json:"result"`
}

type MilestoneIDResponse struct {
	Height string      `json:"height"`
	Result MilestoneID `json:"result"`
}
