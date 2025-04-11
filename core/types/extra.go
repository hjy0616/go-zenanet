// Package types contains data types related to Ethereum consensus.
package types

import (
	"github.com/zenanetwork/go-zenanet/params"
)

// Constants for header extra-data manipulation
const (
	// ExtraVanityLength is the fixed number of bytes reserved for the validator vanity
	ExtraVanityLength = 32
	// ExtraSealLength is the fixed number of bytes reserved for the validator seal
	ExtraSealLength = 65
)

// BlockExtraData contains the extra data for the block header
type BlockExtraData struct {
	ValidatorBytes []byte
	TxDependency   [][]byte
}

// Header.Extra = extraVanity + validatorBytes + extraSeal
func (h *Header) GetValidatorBytes(config *params.ChainConfig) []byte {
	if config.IsCancun(h.Number, 0) {
		if len(h.Extra) < ExtraVanityLength+ExtraSealLength {
			return []byte{}
		}
		validatorBytes := h.Extra[ExtraVanityLength : len(h.Extra)-ExtraSealLength]
		return validatorBytes
	} else {
		if len(h.Extra) < ExtraVanityLength+ExtraSealLength {
			return []byte{}
		}
		validatorBytes := h.Extra[ExtraVanityLength : len(h.Extra)-ExtraSealLength]
		return validatorBytes
	}
}
