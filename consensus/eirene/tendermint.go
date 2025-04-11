package eirene

import (
	"context"

	"github.com/zenanetwork/go-zenanet/consensus/eirene/clerk"
	"github.com/zenanetwork/go-zenanet/consensus/eirene/tendermint/checkpoint"
	"github.com/zenanetwork/go-zenanet/consensus/eirene/tendermint/milestone"
	"github.com/zenanetwork/go-zenanet/consensus/eirene/tendermint/span"
	"github.com/zenanetwork/go-zenanet/consensus/eirene/valset"

	"github.com/tendermint/tendermint/abci/types"
)

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

	Connect() error
	IsConnected() bool
	Close()
}
