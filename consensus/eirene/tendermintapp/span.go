package tendermintapp

import (
	"context"

	hmTypes "github.com/maticnetwork/tendermint/types"

	"github.com/zenanetwork/go-zenanet/consensus/eirene/tendermint/span"
	"github.com/zenanetwork/go-zenanet/consensus/eirene/valset"
	"github.com/zenanetwork/go-zenanet/log"
)

func (h *TendermintAppClient) Span(ctx context.Context, spanID uint64) (*span.TendermintSpan, error) {
	log.Info("Fetching span", "spanID", spanID)

	res, err := h.hApp.EireneKeeper.GetSpan(h.NewContext(), spanID)
	if err != nil {
		return nil, err
	}

	log.Info("Fetched span", "spanID", spanID)

	return toSpan(res), nil
}

func toSpan(hdSpan *hmTypes.Span) *span.TendermintSpan {
	return &span.TendermintSpan{
		Span: span.Span{
			ID:         hdSpan.ID,
			StartBlock: hdSpan.StartBlock,
			EndBlock:   hdSpan.EndBlock,
		},
		ValidatorSet:      toValidatorSet(hdSpan.ValidatorSet),
		SelectedProducers: toValidators(hdSpan.SelectedProducers),
		ChainID:           hdSpan.ChainID,
	}
}

func toValidatorSet(vs hmTypes.ValidatorSet) valset.ValidatorSet {
	return valset.ValidatorSet{
		Validators: toValidatorsRef(vs.Validators),
		Proposer:   toValidatorRef(vs.Proposer),
	}
}

func toValidators(vs []hmTypes.Validator) []valset.Validator {
	newVS := make([]valset.Validator, len(vs))

	for i, v := range vs {
		newVS[i] = toValidator(v)
	}

	return newVS
}

func toValidatorsRef(vs []*hmTypes.Validator) []*valset.Validator {
	newVS := make([]*valset.Validator, len(vs))

	for i, v := range vs {
		if v == nil {
			continue
		}

		newVS[i] = toValidatorRef(v)
	}

	return newVS
}

func toValidatorRef(v *hmTypes.Validator) *valset.Validator {
	return &valset.Validator{
		ID:               v.ID.Uint64(),
		Address:          v.Signer.EthAddress(),
		VotingPower:      v.VotingPower,
		ProposerPriority: v.ProposerPriority,
	}
}

func toValidator(v hmTypes.Validator) valset.Validator {
	return valset.Validator{
		ID:               v.ID.Uint64(),
		Address:          v.Signer.EthAddress(),
		VotingPower:      v.VotingPower,
		ProposerPriority: v.ProposerPriority,
	}
}
