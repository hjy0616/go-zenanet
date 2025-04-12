package span

import (
	"github.com/zenanetwork/go-zenanet/consensus/eirene/valset"
)

// Span Eirene represents a current eirene span
type Span struct {
	ID         uint64 `json:"span_id" yaml:"span_id"`
	StartBlock uint64 `json:"start_block" yaml:"start_block"`
	EndBlock   uint64 `json:"end_block" yaml:"end_block"`
}

// TendermintSpan represents span from tendermint APIs
type TendermintSpan struct {
	Span
	ValidatorSet      valset.ValidatorSet `json:"validator_set" yaml:"validator_set"`
	SelectedProducers []valset.Validator  `json:"selected_producers" yaml:"selected_producers"`
	ChainID           string              `json:"eirene_chain_id" yaml:"eirene_chain_id"`
}

// Copy creates a deep copy of the TendermintSpan
func (t *TendermintSpan) Copy() *TendermintSpan {
	if t == nil {
		return nil
	}

	span := TendermintSpan{
		Span: Span{
			ID:         t.ID,
			StartBlock: t.StartBlock,
			EndBlock:   t.EndBlock,
		},
		ChainID: t.ChainID,
	}

	// Copy ValidatorSet
	if len(t.ValidatorSet.Validators) > 0 {
		validators := make([]*valset.Validator, len(t.ValidatorSet.Validators))
		for i, val := range t.ValidatorSet.Validators {
			validators[i] = val.Copy()
		}
		valSet := valset.NewValidatorSet(validators)
		span.ValidatorSet = *valSet
	}

	// Copy SelectedProducers
	if len(t.SelectedProducers) > 0 {
		producers := make([]valset.Validator, len(t.SelectedProducers))
		for i, val := range t.SelectedProducers {
			producers[i] = *val.Copy()
		}
		span.SelectedProducers = producers
	}

	return &span
}

// UpdateValidatorSet updates the validator set of the span with the new validator set
func (t *TendermintSpan) UpdateValidatorSet(newSet *valset.ValidatorSet) {
	if t == nil || newSet == nil {
		return
	}

	t.ValidatorSet = *newSet

	// Update selected producers list to match validator set
	t.SyncSelectedProducers()
}

// SyncSelectedProducers synchronizes the selected producers with the validator set
func (t *TendermintSpan) SyncSelectedProducers() {
	if t == nil || len(t.ValidatorSet.Validators) == 0 {
		return
	}

	// Reset selected producers
	t.SelectedProducers = make([]valset.Validator, len(t.ValidatorSet.Validators))

	// Copy validators to selected producers
	for i, val := range t.ValidatorSet.Validators {
		t.SelectedProducers[i] = *val.Copy()
	}
}

// IsValid checks if the span is valid
func (t *TendermintSpan) IsValid() bool {
	if t == nil {
		return false
	}

	// Check basic fields
	if t.EndBlock <= t.StartBlock {
		return false
	}

	// Ensure validator set is not empty
	if len(t.ValidatorSet.Validators) == 0 {
		return false
	}

	// Ensure selected producers match validators
	if len(t.SelectedProducers) == 0 {
		return false
	}

	return true
}
