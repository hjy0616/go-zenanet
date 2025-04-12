package span

import (
	"context"
	"encoding/hex"
	"math"
	"math/big"

	"github.com/zenanetwork/go-zenanet/common"
	"github.com/zenanetwork/go-zenanet/common/hexutil"
	"github.com/zenanetwork/go-zenanet/consensus/eirene/abi"
	"github.com/zenanetwork/go-zenanet/consensus/eirene/api"
	"github.com/zenanetwork/go-zenanet/consensus/eirene/statefull"
	"github.com/zenanetwork/go-zenanet/consensus/eirene/valset"
	"github.com/zenanetwork/go-zenanet/core"
	"github.com/zenanetwork/go-zenanet/core/state"
	"github.com/zenanetwork/go-zenanet/core/types"
	"github.com/zenanetwork/go-zenanet/internal/ethapi"
	"github.com/zenanetwork/go-zenanet/log"
	"github.com/zenanetwork/go-zenanet/params"
	"github.com/zenanetwork/go-zenanet/rlp"
	"github.com/zenanetwork/go-zenanet/rpc"
)

type ChainSpanner struct {
	ethAPI                   api.Caller
	validatorSet             abi.ABI
	chainConfig              *params.ChainConfig
	validatorContractAddress common.Address
}

// validator response on ValidatorSet contract
type contractValidator struct {
	Id     *big.Int
	Power  *big.Int
	Signer common.Address
}

func NewChainSpanner(ethAPI api.Caller, validatorSet abi.ABI, chainConfig *params.ChainConfig, validatorContractAddress common.Address) *ChainSpanner {
	return &ChainSpanner{
		ethAPI:                   ethAPI,
		validatorSet:             validatorSet,
		chainConfig:              chainConfig,
		validatorContractAddress: validatorContractAddress,
	}
}

// GetCurrentSpan get current span from contract
func (c *ChainSpanner) GetCurrentSpan(ctx context.Context, headerHash common.Hash) (*Span, error) {
	// block
	blockNr := rpc.BlockNumberOrHashWithHash(headerHash, false)

	// method
	const method = "getCurrentSpan"

	data, err := c.validatorSet.Pack(method)
	if err != nil {
		log.Error("Unable to pack tx for getCurrentSpan", "error", err)

		return nil, err
	}

	msgData := (hexutil.Bytes)(data)
	toAddress := c.validatorContractAddress
	gas := (hexutil.Uint64)(uint64(math.MaxUint64 / 2))

	// todo: would we like to have a timeout here?
	result, err := c.ethAPI.Call(ctx, ethapi.TransactionArgs{
		Gas:  &gas,
		To:   &toAddress,
		Data: &msgData,
	}, &blockNr, nil, nil)
	if err != nil {
		return nil, err
	}

	// span result
	ret := new(struct {
		Number     *big.Int
		StartBlock *big.Int
		EndBlock   *big.Int
	})

	if err := c.validatorSet.UnpackIntoInterface(ret, method, result); err != nil {
		return nil, err
	}

	// create new span
	span := Span{
		ID:         ret.Number.Uint64(),
		StartBlock: ret.StartBlock.Uint64(),
		EndBlock:   ret.EndBlock.Uint64(),
	}

	return &span, nil
}

// GetCurrentValidators get current validators
func (c *ChainSpanner) GetCurrentValidatorsByBlockNrOrHash(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash, blockNumber uint64) ([]*valset.Validator, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	toAddress := c.validatorContractAddress
	gas := (hexutil.Uint64)(uint64(math.MaxUint64 / 2))

	valz, err := c.tryGetEireneValidatorsWithId(ctx, blockNrOrHash, blockNumber, toAddress, gas)
	if err != nil {
		return nil, err
	}

	return valz, nil
}

// tryGetEireneValidatorsWithId Try to get eirene validators with Id from ValidatorSet contract by querying each element on mapping(uint256 => Validator[]) public producers
// If fails then returns GetEireneValidators without id
func (c *ChainSpanner) tryGetEireneValidatorsWithId(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash, blockNumber uint64, toAddress common.Address, gas hexutil.Uint64) ([]*valset.Validator, error) {
	firstEndBlock, err := c.getFirstEndBlock(ctx, blockNrOrHash, toAddress, gas)
	if err != nil {
		return nil, err
	}
	var spanNumber *big.Int
	if big.NewInt(int64(blockNumber)).Cmp(firstEndBlock) <= 0 {
		spanNumber = big.NewInt(0)
	} else {
		spanNumber, err = c.getSpanByBlock(ctx, blockNrOrHash, blockNumber, toAddress, gas)
		if err != nil {
			return nil, err
		}
	}

	eireneValidatorsWithoutId, err := c.getEireneValidatorsWithoutId(ctx, blockNrOrHash, blockNumber, toAddress, gas)
	if err != nil {
		return nil, err
	}

	producersCount := len(eireneValidatorsWithoutId)

	valz := make([]*valset.Validator, producersCount)

	for i := 0; i < producersCount; i++ {
		p, err := c.getProducersBySpanAndIndexMethod(ctx, blockNrOrHash, toAddress, gas, spanNumber, i)
		// if fails, return validators without id
		if err != nil {
			return eireneValidatorsWithoutId, nil
		}

		valz[i] = &valset.Validator{
			ID:          p.Id.Uint64(),
			Address:     p.Signer,
			VotingPower: p.Power.Int64(),
		}
	}

	return valz, nil
}

func (c *ChainSpanner) getSpanByBlock(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash, blockNumber uint64, toAddress common.Address, gas hexutil.Uint64) (*big.Int, error) {
	const getSpanByBlockMethod = "getSpanByBlock"
	spanData, err := c.validatorSet.Pack(getSpanByBlockMethod, big.NewInt(0).SetUint64(blockNumber))
	if err != nil {
		log.Error("Unable to pack tx for getSpanByBlock", "error", err)
		return nil, err
	}

	spanMsgData := (hexutil.Bytes)(spanData)

	spanResult, err := c.ethAPI.Call(ctx, ethapi.TransactionArgs{
		Gas:  &gas,
		To:   &toAddress,
		Data: &spanMsgData,
	}, &blockNrOrHash, nil, nil)
	if err != nil {
		return nil, err
	}

	var spanNumber *big.Int
	if err := c.validatorSet.UnpackIntoInterface(&spanNumber, getSpanByBlockMethod, spanResult); err != nil {
		return nil, err
	}
	return spanNumber, nil
}

func (c *ChainSpanner) getProducersBySpanAndIndexMethod(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash, toAddress common.Address, gas hexutil.Uint64, spanNumber *big.Int, index int) (*contractValidator, error) {
	const getProducersBySpanAndIndexMethod = "producers"
	producerData, err := c.validatorSet.Pack(getProducersBySpanAndIndexMethod, spanNumber, big.NewInt(int64(index)))
	if err != nil {
		log.Error("Unable to pack tx for producers", "error", err)
		return nil, err
	}

	producerMsgData := (hexutil.Bytes)(producerData)

	result, err := c.ethAPI.Call(ctx, ethapi.TransactionArgs{
		Gas:  &gas,
		To:   &toAddress,
		Data: &producerMsgData,
	}, &blockNrOrHash, nil, nil)
	if err != nil {
		return nil, err
	}

	var producer contractValidator
	if err := c.validatorSet.UnpackIntoInterface(&producer, getProducersBySpanAndIndexMethod, result); err != nil {
		return nil, err
	}
	return &producer, nil
}

func (c *ChainSpanner) getFirstEndBlock(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash, toAddress common.Address, gas hexutil.Uint64) (*big.Int, error) {
	const getFirstEndBlockMethod = "FIRST_END_BLOCK"
	firstEndBlockData, err := c.validatorSet.Pack(getFirstEndBlockMethod)
	if err != nil {
		log.Error("Unable to pack tx for getFirstEndBlock", "error", err)
		return nil, err
	}

	firstEndBlockMsgData := (hexutil.Bytes)(firstEndBlockData)

	firstEndBlockResult, err := c.ethAPI.Call(ctx, ethapi.TransactionArgs{
		Gas:  &gas,
		To:   &toAddress,
		Data: &firstEndBlockMsgData,
	}, &blockNrOrHash, nil, nil)
	if err != nil {
		return nil, err
	}

	var firstEndBlockNumber *big.Int
	if err := c.validatorSet.UnpackIntoInterface(&firstEndBlockNumber, getFirstEndBlockMethod, firstEndBlockResult); err != nil {
		return nil, err
	}
	return firstEndBlockNumber, nil
}

func (c *ChainSpanner) getEireneValidatorsWithoutId(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash, blockNumber uint64, toAddress common.Address, gas hexutil.Uint64) ([]*valset.Validator, error) {
	// method
	const method = "getEireneValidators"

	data, err := c.validatorSet.Pack(method, big.NewInt(0).SetUint64(blockNumber))
	if err != nil {
		log.Error("Unable to pack tx for getValidator", "error", err)
		return nil, err
	}

	// call
	msgData := (hexutil.Bytes)(data)

	result, err := c.ethAPI.Call(ctx, ethapi.TransactionArgs{
		Gas:  &gas,
		To:   &toAddress,
		Data: &msgData,
	}, &blockNrOrHash, nil, nil)
	if err != nil {
		return nil, err
	}

	var (
		ret0 = new([]common.Address)
		ret1 = new([]*big.Int)
	)

	out := &[]interface{}{
		ret0,
		ret1,
	}

	if err := c.validatorSet.UnpackIntoInterface(out, method, result); err != nil {
		return nil, err
	}

	valz := make([]*valset.Validator, len(*ret0))
	for i, a := range *ret0 {
		valz[i] = &valset.Validator{
			Address:     a,
			VotingPower: (*ret1)[i].Int64(),
		}
	}

	return valz, nil
}

func (c *ChainSpanner) GetCurrentValidatorsByHash(ctx context.Context, headerHash common.Hash, blockNumber uint64) ([]*valset.Validator, error) {
	blockNr := rpc.BlockNumberOrHashWithHash(headerHash, false)

	return c.GetCurrentValidatorsByBlockNrOrHash(ctx, blockNr, blockNumber)
}

const method = "commitSpan"

func (c *ChainSpanner) CommitSpan(ctx context.Context, tendermintSpan TendermintSpan, state *state.StateDB, header *types.Header, chainContext core.ChainContext) error {
	// get validators bytes
	validators := make([]valset.MinimalVal, 0, len(tendermintSpan.ValidatorSet.Validators))
	for _, val := range tendermintSpan.ValidatorSet.Validators {
		validators = append(validators, val.MinimalVal())
	}

	validatorBytes, err := rlp.EncodeToBytes(validators)
	if err != nil {
		return err
	}

	// get producers bytes
	producers := make([]valset.MinimalVal, 0, len(tendermintSpan.SelectedProducers))
	for _, val := range tendermintSpan.SelectedProducers {
		producers = append(producers, val.MinimalVal())
	}

	producerBytes, err := rlp.EncodeToBytes(producers)
	if err != nil {
		return err
	}

	log.Info("✅ Committing new span",
		"id", tendermintSpan.ID,
		"startBlock", tendermintSpan.StartBlock,
		"endBlock", tendermintSpan.EndBlock,
		"validatorBytes", hex.EncodeToString(validatorBytes),
		"producerBytes", hex.EncodeToString(producerBytes),
	)

	data, err := c.validatorSet.Pack(method,
		big.NewInt(0).SetUint64(tendermintSpan.ID),
		big.NewInt(0).SetUint64(tendermintSpan.StartBlock),
		big.NewInt(0).SetUint64(tendermintSpan.EndBlock),
		validatorBytes,
		producerBytes,
	)
	if err != nil {
		log.Error("Unable to pack tx for commitSpan", "error", err)

		return err
	}

	// get system message
	msg := statefull.GetSystemMessage(c.validatorContractAddress, data)

	// apply message
	_, err = statefull.ApplyMessage(ctx, msg, state, header, c.chainConfig, chainContext)

	return err
}

// CommitSpanFromTendermint commits a span directly from Tendermint validators
// This is a specialized version of CommitSpan that skips the contract call
// and directly uses Tendermint validators
func (c *ChainSpanner) CommitSpanFromTendermint(
	ctx context.Context,
	span *Span,
	tendermintValidators []*valset.Validator,
	chainID string,
	state *state.StateDB,
	header *types.Header,
	chainContext core.ChainContext,
) error {
	if span == nil {
		return nil
	}

	log.Info("Committing span from Tendermint",
		"span", span.ID,
		"startBlock", span.StartBlock,
		"endBlock", span.EndBlock,
		"validators", len(tendermintValidators))

	// Create validator set from Tendermint validators
	validatorSet := valset.NewValidatorSet(tendermintValidators)

	// Create tendermint span
	tendermintSpan := TendermintSpan{
		Span:              *span,
		ValidatorSet:      *validatorSet,
		SelectedProducers: make([]valset.Validator, len(tendermintValidators)),
		ChainID:           chainID,
	}

	// Copy validators to selected producers
	for i, val := range tendermintValidators {
		tendermintSpan.SelectedProducers[i] = *val.Copy()
	}

	// Commit the span
	return c.CommitSpan(ctx, tendermintSpan, state, header, chainContext)
}

// SyncValidatorsWithTendermint synchronizes validators with Tendermint
// This method is used to ensure the validator set in the contract
// matches the validators in Tendermint
func (c *ChainSpanner) SyncValidatorsWithTendermint(
	ctx context.Context,
	tendermintValidators []*valset.Validator,
	currentSpan *Span,
	state *state.StateDB,
	header *types.Header,
	chainContext core.ChainContext,
) error {
	if len(tendermintValidators) == 0 {
		log.Warn("Empty Tendermint validator set, skipping sync")
		return nil
	}

	log.Info("Syncing validators with Tendermint",
		"validators", len(tendermintValidators),
		"block", header.Number.Uint64())

	// If we don't have a current span, we need to create one
	if currentSpan == nil {
		// Create a new span
		newSpan := &Span{
			ID:         1, // First span
			StartBlock: header.Number.Uint64(),
			EndBlock:   header.Number.Uint64() + 100*c.chainConfig.Eirene.CalculateSprint(header.Number.Uint64()),
		}

		// Commit the span with Tendermint validators
		return c.CommitSpanFromTendermint(
			ctx,
			newSpan,
			tendermintValidators,
			c.chainConfig.ChainID.String(),
			state,
			header,
			chainContext,
		)
	}

	// Get current validators from contract
	blockNr := rpc.BlockNumberOrHashWithHash(header.ParentHash, false)
	currentValidators, err := c.GetCurrentValidatorsByBlockNrOrHash(ctx, blockNr, header.Number.Uint64())
	if err != nil {
		log.Error("Failed to get current validators", "error", err)
		return err
	}

	// Check if validators need to be updated
	needsUpdate := c.compareValidatorSets(currentValidators, tendermintValidators)
	if !needsUpdate {
		log.Debug("Validator sets match, no update needed")
		return nil
	}

	log.Info("Validator sets differ, updating with Tendermint validators",
		"current", len(currentValidators),
		"tendermint", len(tendermintValidators))

	// If current span is about to end, we'll create a new one
	if header.Number.Uint64()+c.chainConfig.Eirene.CalculateSprint(header.Number.Uint64()) >= currentSpan.EndBlock {
		// Create a new span that starts after current one ends
		newSpan := &Span{
			ID:         currentSpan.ID + 1,
			StartBlock: currentSpan.EndBlock + 1,
			EndBlock:   currentSpan.EndBlock + 100*c.chainConfig.Eirene.CalculateSprint(header.Number.Uint64()),
		}

		// Commit the span with Tendermint validators
		return c.CommitSpanFromTendermint(
			ctx,
			newSpan,
			tendermintValidators,
			c.chainConfig.ChainID.String(),
			state,
			header,
			chainContext,
		)
	}

	// Update the current span with new validators
	// This would typically be done by a contract call, but for now we'll just
	// create a new TendermintSpan and commit it with the same span ID and blocks
	return c.CommitSpanFromTendermint(
		ctx,
		currentSpan,
		tendermintValidators,
		c.chainConfig.ChainID.String(),
		state,
		header,
		chainContext,
	)
}

// compareValidatorSets checks if the validator sets are different
// Returns true if they need to be updated
func (c *ChainSpanner) compareValidatorSets(current, tendermint []*valset.Validator) bool {
	if len(current) != len(tendermint) {
		return true
	}

	// Create maps for quick lookup
	currentMap := make(map[common.Address]*valset.Validator)
	for _, val := range current {
		currentMap[val.Address] = val
	}

	// Check if any validator is missing or has different power
	for _, tmVal := range tendermint {
		curVal, exists := currentMap[tmVal.Address]
		if !exists || curVal.VotingPower != tmVal.VotingPower {
			return true
		}
	}

	return false
}
