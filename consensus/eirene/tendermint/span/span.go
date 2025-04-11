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
