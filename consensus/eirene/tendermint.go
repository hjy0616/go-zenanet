package eirene

import (
	"context"
	"time"

	"github.com/zenanetwork/go-zenanet/consensus/eirene/clerk"
	"github.com/zenanetwork/go-zenanet/consensus/eirene/tendermint/checkpoint"
	"github.com/zenanetwork/go-zenanet/consensus/eirene/tendermint/milestone"
	"github.com/zenanetwork/go-zenanet/consensus/eirene/tendermint/span"
	"github.com/zenanetwork/go-zenanet/consensus/eirene/valset"

	"github.com/tendermint/tendermint/abci/types"
)

// BlockID는 Tendermint 블록 ID를 나타냅니다.
type BlockID struct {
	Hash        []byte `json:"hash"`
	PartsHeader struct {
		Total int    `json:"total"`
		Hash  []byte `json:"hash"`
	} `json:"parts"`
}

// BlockHeader는 Tendermint 블록 헤더를 나타냅니다.
type BlockHeader struct {
	Version struct {
		Block uint64 `json:"block"`
		App   uint64 `json:"app"`
	} `json:"version"`
	ChainID     string    `json:"chain_id"`
	Height      int64     `json:"height"`
	Time        time.Time `json:"time"`
	LastBlockID BlockID   `json:"last_block_id"`
	LastCommit  struct {
		Height  int64   `json:"height"`
		Round   int     `json:"round"`
		BlockID BlockID `json:"block_id"`
	} `json:"last_commit_info"`
	DataHash           []byte `json:"data_hash"`
	ValidatorsHash     []byte `json:"validators_hash"`
	NextValidatorsHash []byte `json:"next_validators_hash"`
	ConsensusHash      []byte `json:"consensus_hash"`
	AppHash            []byte `json:"app_hash"`
	LastResultsHash    []byte `json:"last_results_hash"`
	EvidenceHash       []byte `json:"evidence_hash"`
	ProposerAddress    []byte `json:"proposer_address"`
}

// BlockInfo는 Tendermint 블록 정보를 나타냅니다.
type BlockInfo struct {
	BlockID BlockID     `json:"block_id"`
	Header  BlockHeader `json:"header"`
	Data    struct {
		Txs [][]byte `json:"txs"`
	} `json:"data"`
}

//go:generate mockgen -destination=../../tests/eirene/mocks/ITendermintClient.go -package=mocks . ITendermintClient
type ITendermintClient interface {
	StateSyncEvents(ctx context.Context, fromID uint64, to int64) ([]*clerk.EventRecordWithTime, error)
	Span(ctx context.Context, spanID uint64) (*span.TendermintSpan, error)
	FetchCheckpoint(ctx context.Context, number int64) (*checkpoint.Checkpoint, error)
	FetchCheckpointCount(ctx context.Context) (int64, error)
	FetchMilestone(ctx context.Context) (*milestone.Milestone, error)
	FetchMilestoneCount(ctx context.Context) (int64, error)
	FetchNoAckMilestone(ctx context.Context, milestoneID string) error // Fetch the bool value whether milestone corresponding to the given id failed in the Tendermint
	FetchLastNoAckMilestone(ctx context.Context) (string, error)       // Fetch latest failed milestone id
	FetchMilestoneID(ctx context.Context, milestoneID string) error    // Fetch the bool value whether milestone corresponding to the given id is in process in Tendermint

	InitChain(ctx context.Context, req types.RequestInitChain) (*types.ResponseInitChain, error)
	BeginBlock(ctx context.Context, req types.RequestBeginBlock) (*types.ResponseBeginBlock, error)
	CheckTx(ctx context.Context, req types.RequestCheckTx) (*types.ResponseCheckTx, error)
	DeliverTx(ctx context.Context, req types.RequestDeliverTx) (*types.ResponseDeliverTx, error)
	EndBlock(ctx context.Context, req types.RequestEndBlock) (*types.ResponseEndBlock, error)
	Commit(ctx context.Context) (*types.ResponseCommit, error)

	GetValidators(ctx context.Context) ([]*valset.Validator, error)
	GetCurrentValidatorSet(ctx context.Context) (*valset.ValidatorSet, error)

	// 블록 정보 조회 메서드 추가
	BlockInfo(ctx context.Context, height *int64) (*BlockInfo, error)

	Connect() error
	IsConnected() bool
	Close()
}
