package tendermintapp

import (
	"context"
	"math/big"

	"github.com/zenanetwork/go-zenanet/consensus/eirene/tendermint/checkpoint"
	"github.com/zenanetwork/go-zenanet/log"

	hmTypes "github.com/maticnetwork/tendermint/types"
)

func (h *TendermintAppClient) FetchCheckpointCount(_ context.Context) (int64, error) {
	log.Info("Fetching checkpoint count")

	res := h.hApp.CheckpointKeeper.GetACKCount(h.NewContext())

	log.Info("Fetched checkpoint count")

	return int64(res), nil
}

func (h *TendermintAppClient) FetchCheckpoint(_ context.Context, number int64) (*checkpoint.Checkpoint, error) {
	log.Info("Fetching checkpoint", "number", number)

	res, err := h.hApp.CheckpointKeeper.GetCheckpointByNumber(h.NewContext(), uint64(number))
	if err != nil {
		return nil, err
	}

	log.Info("Fetched checkpoint", "number", number)

	return toEireneCheckpoint(res), nil
}

func toEireneCheckpoint(hdCheckpoint hmTypes.Checkpoint) *checkpoint.Checkpoint {
	return &checkpoint.Checkpoint{
		Proposer:   hdCheckpoint.Proposer.EthAddress(),
		StartBlock: big.NewInt(int64(hdCheckpoint.StartBlock)),
		EndBlock:   big.NewInt(int64(hdCheckpoint.EndBlock)),
		RootHash:   hdCheckpoint.RootHash.EthHash(),
		EireneChainID: hdCheckpoint.EireneChainID,
		Timestamp:  hdCheckpoint.TimeStamp,
	}
}
