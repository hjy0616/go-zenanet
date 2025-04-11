package tendermintgrpc

import (
	"context"

	"github.com/zenanetwork/go-zenanet/consensus/eirene/tendermint/span"
	"github.com/zenanetwork/go-zenanet/consensus/eirene/valset"
	"github.com/zenanetwork/go-zenanet/log"

	proto "github.com/maticnetwork/polyproto/tendermint"
	protoutils "github.com/maticnetwork/polyproto/utils"
)

func (h *TendermintGRPCClient) Span(ctx context.Context, spanID uint64) (*span.TendermintSpan, error) {
	req := &proto.SpanRequest{
		ID: spanID,
	}

	log.Info("Fetching span", "spanID", spanID)

	res, err := h.client.Span(ctx, req)
	if err != nil {
		return nil, err
	}

	log.Info("Fetched span", "spanID", spanID)

	return parseSpan(res.Result), nil
}

func parseSpan(protoSpan *proto.Span) *span.TendermintSpan {
	resp := &span.TendermintSpan{
		Span: span.Span{
			ID:         protoSpan.ID,
			StartBlock: protoSpan.StartBlock,
			EndBlock:   protoSpan.EndBlock,
		},
		ValidatorSet:      valset.ValidatorSet{},
		SelectedProducers: []valset.Validator{},
		ChainID:           protoSpan.ChainID,
	}

	for _, validator := range protoSpan.ValidatorSet.Validators {
		resp.ValidatorSet.Validators = append(resp.ValidatorSet.Validators, parseValidator(validator))
	}

	resp.ValidatorSet.Proposer = parseValidator(protoSpan.ValidatorSet.Proposer)

	for _, validator := range protoSpan.SelectedProducers {
		resp.SelectedProducers = append(resp.SelectedProducers, *parseValidator(validator))
	}

	return resp
}

func parseValidator(validator *proto.Validator) *valset.Validator {
	return &valset.Validator{
		ID:               validator.ID,
		Address:          protoutils.ConvertH160toAddress(validator.Address),
		VotingPower:      validator.VotingPower,
		ProposerPriority: validator.ProposerPriority,
	}
}
