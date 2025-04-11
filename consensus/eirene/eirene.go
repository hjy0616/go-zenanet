package eirene

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/holiman/uint256"
	"golang.org/x/crypto/sha3"

	"github.com/zenanetwork/go-zenanet/accounts"
	"github.com/zenanetwork/go-zenanet/common"
	balance_tracing "github.com/zenanetwork/go-zenanet/core/tracing"
	"github.com/zenanetwork/go-zenanet/core/vm"

	"github.com/zenanetwork/go-zenanet/consensus"
	"github.com/zenanetwork/go-zenanet/consensus/eirene/api"
	"github.com/zenanetwork/go-zenanet/consensus/eirene/clerk"
	"github.com/zenanetwork/go-zenanet/consensus/eirene/statefull"
	"github.com/zenanetwork/go-zenanet/consensus/eirene/tendermint/span"
	"github.com/zenanetwork/go-zenanet/consensus/eirene/valset"
	"github.com/zenanetwork/go-zenanet/consensus/misc"
	"github.com/zenanetwork/go-zenanet/consensus/misc/eip1559"
	"github.com/zenanetwork/go-zenanet/core"
	"github.com/zenanetwork/go-zenanet/core/state"
	"github.com/zenanetwork/go-zenanet/core/types"
	"github.com/zenanetwork/go-zenanet/crypto"
	"github.com/zenanetwork/go-zenanet/ethdb"
	"github.com/zenanetwork/go-zenanet/log"
	"github.com/zenanetwork/go-zenanet/params"
	"github.com/zenanetwork/go-zenanet/rlp"
	"github.com/zenanetwork/go-zenanet/rpc"
	"github.com/zenanetwork/go-zenanet/trie"
)

const (
	checkpointInterval = 1024 // Number of blocks after which to save the vote snapshot to the database
	inmemorySnapshots  = 128  // Number of recent vote snapshots to keep in memory
	inmemorySignatures = 4096 // Number of recent block signatures to keep in memory
)

// Eirene protocol constants.
var (
	defaultSprintLength = map[string]uint64{
		"0": 64,
	} // Default number of blocks after which to checkpoint and reset the pending votes

	uncleHash = types.CalcUncleHash(nil) // Always Keccak256(RLP([])) as uncles are meaningless outside of PoW.

	validatorHeaderBytesLength = common.AddressLength + 20 // address + power
)

// Various error messages to mark blocks invalid. These should be private to
// prevent engine specific errors from being referenced in the remainder of the
// codebase, inherently breaking if the engine is swapped out. Please put common
// error types into the consensus package.
var (
	// errUnknownBlock is returned when the list of signers is requested for a block
	// that is not part of the local blockchain.
	errUnknownBlock = errors.New("unknown block")

	// errMissingVanity is returned if a block's extra-data section is shorter than
	// 32 bytes, which is required to store the signer vanity.
	errMissingVanity = errors.New("extra-data 32 byte vanity prefix missing")

	// errMissingSignature is returned if a block's extra-data section doesn't seem
	// to contain a 65 byte secp256k1 signature.
	errMissingSignature = errors.New("extra-data 65 byte signature suffix missing")

	// errExtraValidators is returned if non-sprint-end block contain validator data in
	// their extra-data fields.
	errExtraValidators = errors.New("non-sprint-end block contains extra validator list")

	// errInvalidSpanValidators is returned if a block contains an
	// invalid list of validators (i.e. non divisible by 40 bytes).
	errInvalidSpanValidators = errors.New("invalid validator list on sprint end block")

	// errInvalidMixDigest is returned if a block's mix digest is non-zero.
	errInvalidMixDigest = errors.New("non-zero mix digest")

	// errInvalidUncleHash is returned if a block contains an non-empty uncle list.
	errInvalidUncleHash = errors.New("non empty uncle hash")

	// errInvalidDifficulty is returned if the difficulty of a block neither 1 or 2.
	errInvalidDifficulty = errors.New("invalid difficulty")

	// ErrInvalidTimestamp is returned if the timestamp of a block is lower than
	// the previous block's timestamp + the minimum block period.
	ErrInvalidTimestamp = errors.New("invalid timestamp")

	// errOutOfRangeChain is returned if an authorization list is attempted to
	// be modified via out-of-range or non-contiguous headers.
	errOutOfRangeChain = errors.New("out of range or non-contiguous chain")

	errUncleDetected     = errors.New("uncles not allowed")
	errUnknownValidators = errors.New("unknown validators")
)

// SignerFn is a signer callback function to request a header to be signed by a
// backing account.
type SignerFn func(accounts.Account, string, []byte) ([]byte, error)

// ecrecover extracts the Ethereum account address from a signed header.
func ecrecover(header *types.Header, sigcache *lru.ARCCache, c *params.EireneConfig) (common.Address, error) {
	// If the signature's already cached, return that
	hash := header.Hash()
	if address, known := sigcache.Get(hash); known {
		return address.(common.Address), nil
	}
	// Retrieve the signature from the header extra-data
	if len(header.Extra) < types.ExtraSealLength {
		return common.Address{}, errMissingSignature
	}

	signature := header.Extra[len(header.Extra)-types.ExtraSealLength:]

	// Recover the public key and the Ethereum address
	pubkey, err := crypto.Ecrecover(SealHash(header, c).Bytes(), signature)
	if err != nil {
		return common.Address{}, err
	}

	var signer common.Address

	copy(signer[:], crypto.Keccak256(pubkey[1:])[12:])

	sigcache.Add(hash, signer)

	return signer, nil
}

// SealHash returns the hash of a block prior to it being sealed.
func SealHash(header *types.Header, c *params.EireneConfig) (hash common.Hash) {
	hasher := sha3.NewLegacyKeccak256()
	encodeSigHeader(hasher, header, c)
	hasher.Sum(hash[:0])

	return hash
}

func encodeSigHeader(w io.Writer, header *types.Header, c *params.EireneConfig) {
	enc := []interface{}{
		header.ParentHash,
		header.UncleHash,
		header.Coinbase,
		header.Root,
		header.TxHash,
		header.ReceiptHash,
		header.Bloom,
		header.Difficulty,
		header.Number,
		header.GasLimit,
		header.GasUsed,
		header.Time,
		header.Extra[:len(header.Extra)-65], // Yes, this will panic if extra is too short
		header.MixDigest,
		header.Nonce,
	}

	if c.IsJaipur(header.Number) {
		if header.BaseFee != nil {
			enc = append(enc, header.BaseFee)
		}
	}

	if err := rlp.Encode(w, enc); err != nil {
		panic("can't encode: " + err.Error())
	}
}

// CalcProducerDelay is the block delay algorithm based on block time, period, producerDelay and turn-ness of a signer
func CalcProducerDelay(number uint64, succession int, c *params.EireneConfig) uint64 {
	// When the block is the first block of the sprint, it is expected to be delayed by `producerDelay`.
	// That is to allow time for block propagation in the last sprint
	delay := c.CalculatePeriod(number)
	if number%c.CalculateSprint(number) == 0 {
		delay = c.CalculateProducerDelay(number)
	}

	if succession > 0 {
		delay += uint64(succession) * c.CalculateBackupMultiplier(number)
	}

	return delay
}

// EireneRLP returns the rlp bytes which needs to be signed for the eirene
// sealing. The RLP to sign consists of the entire header apart from the 65 byte signature
// contained at the end of the extra data.
//
// Note, the method requires the extra data to be at least 65 bytes, otherwise it
// panics. This is done to avoid accidentally using both forms (signature present
// or not), which could be abused to produce different hashes for the same header.
func EireneRLP(header *types.Header, c *params.EireneConfig) []byte {
	b := new(bytes.Buffer)
	encodeSigHeader(b, header, c)

	return b.Bytes()
}

// Eirene is the matic-eirene consensus engine
type Eirene struct {
	chainConfig *params.ChainConfig  // Chain config
	config      *params.EireneConfig // Consensus engine configuration parameters for eirene consensus
	db          ethdb.Database       // Database to store and retrieve snapshot checkpoints

	recents    *lru.ARCCache // Snapshots for recent block to speed up reorgs
	signatures *lru.ARCCache // Signatures of recent blocks to speed up mining

	authorizedSigner atomic.Pointer[signer] // Ethereum address and sign function of the signing key

	ethAPI                 api.Caller
	spanner                Spanner
	GenesisContractsClient GenesisContract
	TendermintClient       ITendermintClient

	// The fields below are for testing only
	fakeDiff      bool // Skip difficulty verifications
	devFakeAuthor bool

	closeOnce sync.Once
}

type signer struct {
	signer common.Address // Ethereum address of the signing key
	signFn SignerFn       // Signer function to authorize hashes with
}

// New creates a Eirene consensus engine.
func New(
	chainConfig *params.ChainConfig,
	db ethdb.Database,
	ethAPI api.Caller,
	spanner Spanner,
	tendermintClient ITendermintClient,
	genesisContracts GenesisContract,
	devFakeAuthor bool,
) *Eirene {
	// Allocate the snapshot caches and create the engine
	recents, _ := lru.NewARC(inmemorySnapshots)
	signatures, _ := lru.NewARC(inmemorySignatures)

	c := &Eirene{
		chainConfig:            chainConfig,
		config:                 chainConfig.Eirene,
		db:                     db,
		recents:                recents,
		signatures:             signatures,
		ethAPI:                 ethAPI,
		spanner:                spanner,
		GenesisContractsClient: genesisContracts,
		TendermintClient:       tendermintClient,
		devFakeAuthor:          devFakeAuthor,
	}

	// Ensure Tendermint client is connected
	if tendermintClient != nil && !tendermintClient.IsConnected() {
		log.Info("Connecting to Tendermint...")
		if err := tendermintClient.Connect(); err != nil {
			log.Error("Failed to connect to Tendermint", "err", err)
		}
	}

	return c
}

// Author implements consensus.Engine, returning the Ethereum address recovered
// from the signature in the header's extra-data section.
func (c *Eirene) Author(header *types.Header) (common.Address, error) {
	return ecrecover(header, c.signatures, c.config)
}

// VerifyHeader checks whether a header conforms to the consensus rules of the eirene engine
func (c *Eirene) VerifyHeader(chain consensus.ChainHeaderReader, header *types.Header) error {
	return c.verifyHeader(chain, header, nil)
}

func (c *Eirene) GetSpanner() Spanner {
	return c.spanner
}

func (c *Eirene) SetSpanner(spanner Spanner) {
	c.spanner = spanner
}

// VerifyHeaders is similar to VerifyHeader, but verifies a batch of headers. The
// method returns a quit channel to aeirenet the operations and a results channel to
// retrieve the async verifications (the order is that of the input slice).
func (c *Eirene) VerifyHeaders(chain consensus.ChainHeaderReader, headers []*types.Header) (chan<- struct{}, <-chan error) {
	aeirenet := make(chan struct{})
	results := make(chan error, len(headers))

	go func() {
		for i, header := range headers {
			err := c.verifyHeader(chain, header, headers[:i])

			select {
			case <-aeirenet:
				return
			case results <- err:
			}
		}
	}()

	return aeirenet, results
}

// verifyHeader checks whether a header conforms to the consensus rules of the Eirene engine.
// See YoloV1 paper section 5.2.
// The verification of eirene header can be done in two ways
// 1. basic (all normal checks and signature check using validator set from the header itself)
// 2. bor (basic + validator set verification from totem), used for side chains i.e. erigon
func (c *Eirene) verifyHeader(chain consensus.ChainHeaderReader, header *types.Header, parents []*types.Header) error {
	// Ensure that the header is sealed properly
	if header.Number == nil {
		return errUnknownBlock
	}

	number := header.Number.Uint64()

	// If the header is a checkpoint block, verify the signer list
	// The signer list from the block header is verified using the sprint during sprint change
	// Block must be a multiple of sprint
	if number > 0 && IsSprintStart(number, c.config.CalculateSprint(number)) && len(header.Extra) >= types.ExtraSealLength {
		// 헤더의 Extra 데이터에서 검증자 정보 검증
		signersBytes := len(header.GetValidatorBytes(c.chainConfig))
		if signersBytes%validatorHeaderBytesLength != 0 {
			log.Warn("Invalid validator set", "number", number, "signersBytes", signersBytes)
			return errInvalidSpanValidators
		}
	}

	// All basic checks passed, verify cascading fields
	return c.verifyCascadingFields(chain, header, parents)
}

// validateHeaderExtraField validates that the extra-data contains both the vanity and signature.
// header.Extra = header.Vanity + header.ProducerBytes (optional) + header.Seal
func validateHeaderExtraField(extraBytes []byte) error {
	if len(extraBytes) < types.ExtraVanityLength {
		return errMissingVanity
	}

	if len(extraBytes) < types.ExtraVanityLength+types.ExtraSealLength {
		return errMissingSignature
	}

	return nil
}

// verifyCascadingFields verifies all the header fields that are not standalone,
// rather depend on a batch of previous headers. The caller may optionally pass
// in a batch of parents (ascending order) to avoid looking those up from the
// database. This is useful for concurrently verifying a batch of new headers.
func (c *Eirene) verifyCascadingFields(chain consensus.ChainHeaderReader, header *types.Header, parents []*types.Header) error {
	// The genesis block is the always valid dead-end
	number := header.Number.Uint64()

	if number == 0 {
		return nil
	}

	// Ensure that the block's timestamp isn't too close to it's parent
	var parent *types.Header

	if len(parents) > 0 {
		parent = parents[len(parents)-1]
	} else {
		parent = chain.GetHeader(header.ParentHash, number-1)
	}

	if parent == nil || parent.Number.Uint64() != number-1 || parent.Hash() != header.ParentHash {
		return consensus.ErrUnknownAncestor
	}

	// Verify that the gasUsed is <= gasLimit
	if header.GasUsed > header.GasLimit {
		return fmt.Errorf("invalid gasUsed: have %d, gasLimit %d", header.GasUsed, header.GasLimit)
	}

	if !chain.Config().IsLondon(header.Number) {
		// Verify BaseFee not present before EIP-1559 fork.
		if header.BaseFee != nil {
			return fmt.Errorf("invalid baseFee before fork: have %d, want <nil>", header.BaseFee)
		}

		if err := misc.VerifyGaslimit(parent.GasLimit, header.GasLimit); err != nil {
			return err
		}
	} else if err := eip1559.VerifyEIP1559Header(chain.Config(), parent, header); err != nil {
		// Verify the header's EIP-1559 attributes.
		return err
	}

	if parent.Time+c.config.CalculatePeriod(number) > header.Time {
		return ErrInvalidTimestamp
	}

	// Retrieve the snapshot needed to verify this header and cache it
	snap, err := c.snapshot(chain, number-1, header.ParentHash, parents)
	if err != nil {
		return err
	}

	// Verify the validator list match the local contract
	if IsSprintStart(number+1, c.config.CalculateSprint(number)) {
		newValidators, err := c.spanner.GetCurrentValidatorsByBlockNrOrHash(context.Background(), rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber), number+1)

		if err != nil {
			return err
		}

		sort.Sort(valset.ValidatorsByAddress(newValidators))

		headerVals, err := valset.ParseValidators(header.GetValidatorBytes(c.chainConfig))
		if err != nil {
			return err
		}

		if len(newValidators) != len(headerVals) {
			log.Warn("Invalid validator set", "block number", number, "newValidators", newValidators, "headerVals", headerVals)
			return errInvalidSpanValidators
		}

		for i, val := range newValidators {
			if !bytes.Equal(val.HeaderBytes(), headerVals[i].HeaderBytes()) {
				log.Warn("Invalid validator set", "block number", number, "index", i, "local validator", val, "header validator", headerVals[i])
				return errInvalidSpanValidators
			}
		}
	}

	// verify the validator list in the last sprint block
	if IsSprintStart(number, c.config.CalculateSprint(number)) {
		parentValidatorBytes := parent.GetValidatorBytes(c.chainConfig)
		validatorsBytes := make([]byte, len(snap.ValidatorSet.Validators)*validatorHeaderBytesLength)

		currentValidators := snap.ValidatorSet.Copy().Validators
		// sort validator by address
		sort.Sort(valset.ValidatorsByAddress(currentValidators))

		for i, validator := range currentValidators {
			copy(validatorsBytes[i*validatorHeaderBytesLength:], validator.HeaderBytes())
		}
		// len(header.Extra) >= extraVanity+extraSeal has already been validated in validateHeaderExtraField, so this won't result in a panic
		if !bytes.Equal(parentValidatorBytes, validatorsBytes) {
			return &MismatchingValidatorsError{number - 1, validatorsBytes, parentValidatorBytes}
		}
	}

	// All basic checks passed, verify the seal and return
	return c.verifySeal(chain, header, parents)
}

// snapshot retrieves the authorization snapshot at a given point in time.
// nolint: gocognit
func (c *Eirene) snapshot(chain consensus.ChainHeaderReader, number uint64, hash common.Hash, parents []*types.Header) (*Snapshot, error) {
	// Search for a snapshot in memory or on disk for checkpoints
	signer := common.BytesToAddress(c.authorizedSigner.Load().signer.Bytes())
	if c.devFakeAuthor && signer.String() != "0x0000000000000000000000000000000000000000" {
		log.Info("👨‍💻Using DevFakeAuthor", "signer", signer)

		val := valset.NewValidator(signer, 1000)
		validatorset := valset.NewValidatorSet([]*valset.Validator{val})

		snapshot := newSnapshot(c.chainConfig, c.signatures, number, hash, validatorset.Validators)

		return snapshot, nil
	}

	var snap *Snapshot

	headers := make([]*types.Header, 0, 16)

	//nolint:govet
	for snap == nil {
		// If an in-memory snapshot was found, use that
		if s, ok := c.recents.Get(hash); ok {
			snap = s.(*Snapshot)

			break
		}

		// If an on-disk checkpoint snapshot can be found, use that
		if number%checkpointInterval == 0 {
			if s, err := loadSnapshot(c.chainConfig, c.config, c.signatures, c.db, hash); err == nil {
				log.Trace("Loaded snapshot from disk", "number", number, "hash", hash)

				snap = s

				break
			}
		}

		// If we're at the genesis, snapshot the initial state. Alternatively if we're
		// at a checkpoint block without a parent (light client CHT), or we have piled
		// up more headers than allowed to be reorged (chain reinit from a freezer),
		// consider the checkpoint trusted and snapshot it.

		// TODO fix this
		// nolint:nestif
		if number == 0 {
			checkpoint := chain.GetHeaderByNumber(number)
			if checkpoint != nil {
				// get checkpoint data
				hash := checkpoint.Hash()

				// get validators and current span
				validators, err := c.spanner.GetCurrentValidatorsByHash(context.Background(), hash, number+1)
				if err != nil {
					return nil, err
				}

				// new snap shot
				snap = newSnapshot(c.chainConfig, c.signatures, number, hash, validators)
				if err := snap.store(c.db); err != nil {
					return nil, err
				}

				log.Info("Stored checkpoint snapshot to disk", "number", number, "hash", hash)

				break
			}
		}

		// No snapshot for this header, gather the header and move backward
		var header *types.Header
		if len(parents) > 0 {
			// If we have explicit parents, pick from there (enforced)
			header = parents[len(parents)-1]
			if header.Hash() != hash || header.Number.Uint64() != number {
				return nil, consensus.ErrUnknownAncestor
			}

			parents = parents[:len(parents)-1]
		} else {
			// No explicit parents (or no more left), reach out to the database
			header = chain.GetHeader(hash, number)
			if header == nil {
				return nil, consensus.ErrUnknownAncestor
			}
		}

		headers = append(headers, header)
		number, hash = number-1, header.ParentHash
	}

	// check if snapshot is nil
	if snap == nil {
		return nil, fmt.Errorf("unknown error while retrieving snapshot at block number %v", number)
	}

	// Previous snapshot found, apply any pending headers on top of it
	for i := 0; i < len(headers)/2; i++ {
		headers[i], headers[len(headers)-1-i] = headers[len(headers)-1-i], headers[i]
	}

	snap, err := snap.apply(headers, c)
	if err != nil {
		return nil, err
	}

	c.recents.Add(snap.Hash, snap)

	// If we've generated a new checkpoint snapshot, save to disk
	if snap.Number%checkpointInterval == 0 && len(headers) > 0 {
		if err = snap.store(c.db); err != nil {
			return nil, err
		}

		log.Trace("Stored snapshot to disk", "number", snap.Number, "hash", snap.Hash)
	}

	return snap, err
}

// VerifyUncles implements consensus.Engine, always returning an error for any
// uncles as this consensus mechanism doesn't permit uncles.
func (c *Eirene) VerifyUncles(_ consensus.ChainReader, block *types.Block) error {
	if len(block.Uncles()) > 0 {
		return errUncleDetected
	}

	return nil
}

// VerifySeal implements consensus.Engine, checking whether the signature contained
// in the header satisfies the consensus protocol requirements.
func (c *Eirene) VerifySeal(chain consensus.ChainHeaderReader, header *types.Header) error {
	return c.verifySeal(chain, header, nil)
}

// verifySeal checks whether the signature contained in the header satisfies the
// consensus protocol requirements. The method accepts an optional list of parent
// headers that aren't yet part of the local blockchain to generate the snapshots
// from.
func (c *Eirene) verifySeal(chain consensus.ChainHeaderReader, header *types.Header, parents []*types.Header) error {
	// Verifying the genesis block is not supported
	number := header.Number.Uint64()
	if number == 0 {
		return errUnknownBlock
	}
	// Retrieve the snapshot needed to verify this header and cache it
	snap, err := c.snapshot(chain, number-1, header.ParentHash, parents)
	if err != nil {
		return err
	}

	// Resolve the authorization key and check against signers
	signer, err := ecrecover(header, c.signatures, c.config)
	if err != nil {
		return err
	}

	if !snap.ValidatorSet.HasAddress(signer) {
		// Check the UnauthorizedSignerError.Error() msg to see why we pass number-1
		return &UnauthorizedSignerError{number - 1, signer.Bytes()}
	}

	succession, err := snap.GetSignerSuccessionNumber(signer)
	if err != nil {
		return err
	}

	var parent *types.Header
	if len(parents) > 0 { // if parents is nil, len(parents) is zero
		parent = parents[len(parents)-1]
	} else if number > 0 {
		parent = chain.GetHeader(header.ParentHash, number-1)
	}

	if IsBlockOnTime(parent, header, number, succession, c.config) {
		return &BlockTooSoonError{number, succession}
	}

	// Ensure that the difficulty corresponds to the turn-ness of the signer
	if !c.fakeDiff {
		difficulty := Difficulty(snap.ValidatorSet, signer)
		if header.Difficulty.Uint64() != difficulty {
			return &WrongDifficultyError{number, difficulty, header.Difficulty.Uint64(), signer.Bytes()}
		}
	}

	return nil
}

func IsBlockOnTime(parent *types.Header, header *types.Header, number uint64, succession int, cfg *params.EireneConfig) bool {
	return parent != nil && header.Time < parent.Time+CalcProducerDelay(number, succession, cfg)
}

// Prepare implements consensus.Engine, preparing all the consensus fields of the
// header for running the transactions on top.
func (c *Eirene) Prepare(chain consensus.ChainHeaderReader, header *types.Header) error {
	// If the block isn't a checkpoint, cast a random vote (good enough for now)
	header.Coinbase = common.Address{}
	header.Nonce = types.BlockNonce{}

	number := header.Number.Uint64()
	// Assemble the validator snapshot to check which votes make sense
	snap, err := c.snapshot(chain, number-1, header.ParentHash, nil)
	if err != nil {
		return err
	}

	currentSigner := *c.authorizedSigner.Load()

	// Set the correct difficulty
	header.Difficulty = new(big.Int).SetUint64(Difficulty(snap.ValidatorSet, currentSigner.signer))

	// Ensure the extra data has all it's components
	if len(header.Extra) < types.ExtraVanityLength {
		header.Extra = append(header.Extra, bytes.Repeat([]byte{0x00}, types.ExtraVanityLength-len(header.Extra))...)
	}

	header.Extra = header.Extra[:types.ExtraVanityLength]

	// get validator set if number
	if IsSprintStart(number+1, c.config.CalculateSprint(number)) {
		newValidators, err := c.spanner.GetCurrentValidatorsByHash(context.Background(), header.ParentHash, number+1)
		if err != nil {
			return errUnknownValidators
		}

		// sort validator by address
		sort.Sort(valset.ValidatorsByAddress(newValidators))

		if c.chainConfig.IsCancun(header.Number) {
			var tempValidatorBytes []byte

			for _, validator := range newValidators {
				tempValidatorBytes = append(tempValidatorBytes, validator.HeaderBytes()...)
			}

			blockExtraData := &types.BlockExtraData{
				ValidatorBytes: tempValidatorBytes,
				TxDependency:   nil,
			}

			blockExtraDataBytes, err := rlp.EncodeToBytes(blockExtraData)
			if err != nil {
				log.Error("error while encoding block extra data: %v", err)
				return fmt.Errorf("error while encoding block extra data: %v", err)
			}

			header.Extra = append(header.Extra, blockExtraDataBytes...)
		} else {
			for _, validator := range newValidators {
				header.Extra = append(header.Extra, validator.HeaderBytes()...)
			}
		}
	} else if c.chainConfig.IsCancun(header.Number) {
		blockExtraData := &types.BlockExtraData{
			ValidatorBytes: nil,
			TxDependency:   nil,
		}

		blockExtraDataBytes, err := rlp.EncodeToBytes(blockExtraData)
		if err != nil {
			log.Error("error while encoding block extra data: %v", err)
			return fmt.Errorf("error while encoding block extra data: %v", err)
		}

		header.Extra = append(header.Extra, blockExtraDataBytes...)
	}

	// add extra seal space
	header.Extra = append(header.Extra, make([]byte, types.ExtraSealLength)...)

	// Mix digest is reserved for now, set to empty
	header.MixDigest = common.Hash{}

	// Ensure the timestamp has the correct delay
	parent := chain.GetHeader(header.ParentHash, number-1)
	if parent == nil {
		return consensus.ErrUnknownAncestor
	}

	var succession int
	// if signer is not empty
	if currentSigner.signer != (common.Address{}) {
		succession, err = snap.GetSignerSuccessionNumber(currentSigner.signer)
		if err != nil {
			return err
		}
	}

	header.Time = parent.Time + CalcProducerDelay(number, succession, c.config)
	if header.Time < uint64(time.Now().Unix()) {
		header.Time = uint64(time.Now().Unix())
	}

	return nil
}

// Finalize implements consensus.Engine, ensuring no uncles are set, nor block
// rewards given.
func (c *Eirene) Finalize(chain consensus.ChainHeaderReader, header *types.Header, state vm.StateDB, body *types.Body) {
	headerNumber := header.Number.Uint64()
	if body.Withdrawals != nil || header.WithdrawalsHash != nil {
		return
	}

	stateDB, ok := state.(*state.StateDB)
	if !ok {
		log.Error("Failed to convert state to *state.StateDB")
		return
	}

	var (
		stateSyncData []*types.StateSyncData
		err           error
	)

	if IsSprintStart(headerNumber, c.config.CalculateSprint(headerNumber)) {
		start := time.Now()
		cx := statefull.ChainContext{Chain: chain, Eirene: c}
		// check and commit span
		if err := c.checkAndCommitSpan(stateDB, header, cx); err != nil {
			log.Error("Error while committing span", "error", err)
			return
		}

		if c.TendermintClient != nil && c.TendermintClient.IsConnected() {
			// Tendermint에 블록 완성 상태 통지
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			log.Info("Notifying Tendermint about block finalization",
				"number", headerNumber,
				"hash", header.Hash())

			// Tendermint에서 현재 검증자 세트 가져오기
			validators, valErr := c.TendermintClient.GetCurrentValidatorSet(ctx)
			if valErr != nil {
				log.Warn("Failed to get validator set from Tendermint", "error", valErr)
			} else {
				log.Debug("Current Tendermint validator set", "count", len(validators.Validators))
			}

			// commit states
			stateSyncData, err = c.CommitStates(stateDB, header, cx)
			if err != nil {
				log.Error("Error while committing states", "error", err)
				return
			}
		}

		stateDB.AddEireneConsensusTime(time.Since(start))
	}

	if err = c.changeContractCodeIfNeeded(headerNumber, stateDB); err != nil {
		log.Error("Error changing contract code", "error", err)
		return
	}

	// No block rewards in PoA, so the state remains as is and uncles are dropped
	header.Root = stateDB.IntermediateRoot(chain.Config().IsEIP158(header.Number))
	header.UncleHash = types.CalcUncleHash(nil)

	// Set state sync data to blockchain
	if bc, ok := chain.(*core.BlockChain); ok {
		if err := bc.AddStateSyncData(stateSyncData); err != nil {
			log.Error("Failed to add state sync data", "error", err)
		}
	}
}

func decodeGenesisAlloc(i interface{}) (types.GenesisAlloc, error) {
	var alloc types.GenesisAlloc

	b, err := json.Marshal(i)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(b, &alloc); err != nil {
		return nil, err
	}

	return alloc, nil
}

func (c *Eirene) changeContractCodeIfNeeded(headerNumber uint64, state *state.StateDB) error {
	for blockNumber, genesisAlloc := range c.config.BlockAlloc {
		if blockNumber == strconv.FormatUint(headerNumber, 10) {
			allocs, err := decodeGenesisAlloc(genesisAlloc)
			if err != nil {
				return fmt.Errorf("failed to decode genesis alloc: %w", err)
			}

			for addr, account := range allocs {
				log.Info("change contract code", "address", addr)
				state.SetCode(addr, account.Code)

				if state.GetBalance(addr).Cmp(uint256.NewInt(0)) == 0 {
					// todo: @anshalshukla - check tracing reason
					state.SetBalance(addr, uint256.NewInt(account.Balance.Uint64()), balance_tracing.BalanceChangeUnspecified)
				}
			}
		}
	}

	return nil
}

// FinalizeAndAssemble implements consensus.Engine, ensuring no uncles are set,
// nor block rewards given, and returns the final block.
func (c *Eirene) FinalizeAndAssemble(chain consensus.ChainHeaderReader, header *types.Header, state vm.StateDB, body *types.Body, receipts []*types.Receipt) (*types.Block, error) {
	headerNumber := header.Number.Uint64()
	if body.Withdrawals != nil || header.WithdrawalsHash != nil {
		return nil, consensus.ErrUnexpectedWithdrawals
	}

	stateDB, ok := state.(*state.StateDB)
	if !ok {
		return nil, errors.New("failed to convert state to *state.StateDB")
	}

	var (
		stateSyncData []*types.StateSyncData
		err           error
	)

	if IsSprintStart(headerNumber, c.config.CalculateSprint(headerNumber)) {
		cx := statefull.ChainContext{Chain: chain, Eirene: c}

		// check and commit span
		if err = c.checkAndCommitSpan(stateDB, header, cx); err != nil {
			log.Error("Error while committing span", "error", err)
			return nil, err
		}

		if c.TendermintClient != nil && c.TendermintClient.IsConnected() {
			// Tendermint에 새로운 검증자 세트 정보 전송
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			validators, valErr := c.GetCurrentValidators(ctx, header.Hash(), headerNumber)
			if valErr != nil {
				log.Warn("Failed to get validators for Tendermint", "error", valErr)
			} else {
				log.Info("Sending validator set to Tendermint", "validators", len(validators))
				// 여기서 필요한 경우 Tendermint에 검증자 정보를 전송할 수 있음
			}

			// commit states
			stateSyncData, err = c.CommitStates(stateDB, header, cx)
			if err != nil {
				log.Error("Error while committing states", "error", err)
				return nil, err
			}
		}
	}

	if err = c.changeContractCodeIfNeeded(headerNumber, stateDB); err != nil {
		log.Error("Error changing contract code", "error", err)
		return nil, err
	}

	// No block rewards in PoA, so the state remains as it is
	header.Root = stateDB.IntermediateRoot(chain.Config().IsEIP158(header.Number))

	// Uncles are dropped
	header.UncleHash = types.CalcUncleHash(nil)

	// Assemble block
	block := types.NewBlock(header, body, receipts, trie.NewStackTrie(nil))

	// 블록 최종화 전 Tendermint와 상태 확인
	if c.TendermintClient != nil && c.TendermintClient.IsConnected() {
		log.Debug("Verifying block with Tendermint before finalization",
			"number", headerNumber,
			"hash", header.Hash().Hex(),
			"transactions", len(body.Transactions))

		// 추가 검증이 필요한 경우 Tendermint API를 호출할 수 있음
	}

	// set state sync
	if bc, ok := chain.(*core.BlockChain); ok {
		if err := bc.AddStateSyncData(stateSyncData); err != nil {
			log.Error("Failed to add state sync data", "error", err)
		}
	}

	// return the final block for sealing
	return block, nil
}

// Authorize injects a private key into the consensus engine to mint new blocks
// with.
func (c *Eirene) Authorize(currentSigner common.Address, signFn SignerFn) {
	c.authorizedSigner.Store(&signer{
		signer: currentSigner,
		signFn: signFn,
	})
}

// Seal implements consensus.Engine, attempting to create a sealed block using
// the local signing credentials.
func (c *Eirene) Seal(chain consensus.ChainHeaderReader, block *types.Block, results chan<- *types.Block, stop <-chan struct{}) error {
	header := block.Header()

	// Sealing the genesis block is not supported
	number := header.Number.Uint64()
	if number == 0 {
		return errUnknownBlock
	}

	// For 0-period chains, refuse to seal empty blocks (no reward but would spin sealing)
	if c.config.CalculatePeriod(number) == 0 && len(block.Transactions()) == 0 {
		log.Info("Sealing paused, waiting for transactions")
		return nil
	}

	// Don't hold the signer fields for the entire sealing procedure
	authorizedSigner := c.authorizedSigner.Load()

	if authorizedSigner == nil {
		log.Error("Signer not authorized for sealing")
		return errUnauthorizedSigner
	}

	currentSigner := authorizedSigner.signer
	signFn := authorizedSigner.signFn

	// Sweet, the protocol permits us to sign the block, wait for our time
	delay := time.Unix(int64(header.Time), 0).Sub(time.Now()) // nolint: gosimple
	if delay < 0 {
		delay = 0
	}

	// Sign all the things!
	sealHash := SealHash(header, c.config)
	hashBytes := sealHash.Bytes()
	sighash, err := signFn(accounts.Account{Address: currentSigner}, accounts.MimetypeEirene, hashBytes)

	if err != nil {
		return err
	}

	copy(header.Extra[len(header.Extra)-types.ExtraSealLength:], sighash)

	// Check if we're authorized to sign the header
	if err := c.checkAuthorization(chain, header, currentSigner); err != nil {
		log.Warn("Block sealing failed", "err", err)
		return err
	}

	// Tendermint 확인 및 동기화
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Tendermint에게 블록 전송
	if c.TendermintClient != nil && c.TendermintClient.IsConnected() {
		// TODO: DeliverTx 호출로 블록 정보 전송
		log.Info("Sending block to Tendermint", "number", number, "hash", header.Hash().Hex())
	}

	// 블록 생성 완료 및 결과 반환
	go func() {
		select {
		case <-stop:
			return
		case results <- block.WithSeal(header):
		}
	}()

	return nil
}

// 새로 추가: Tendermint 권한 확인 함수
func (c *Eirene) checkAuthorization(chain consensus.ChainHeaderReader, header *types.Header, signer common.Address) error {
	// Tendermint 검증자 확인
	if c.TendermintClient != nil && c.TendermintClient.IsConnected() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		validators, err := c.TendermintClient.GetValidators(ctx)
		if err != nil {
			return fmt.Errorf("failed to get validators from Tendermint: %w", err)
		}

		// 검증자 집합에 현재 서명자가 있는지 확인
		isValidator := false
		for _, validator := range validators {
			if validator.Address == signer {
				isValidator = true
				break
			}
		}

		if !isValidator {
			return errUnauthorizedSigner
		}
	}

	return nil
}

// CalcDifficulty is the difficulty adjustment algorithm. It returns the difficulty
// that a new block should have based on the previous blocks in the chain and the
// current signer.
func (c *Eirene) CalcDifficulty(chain consensus.ChainHeaderReader, _ uint64, parent *types.Header) *big.Int {
	snap, err := c.snapshot(chain, parent.Number.Uint64(), parent.Hash(), nil)
	if err != nil {
		return nil
	}

	return new(big.Int).SetUint64(Difficulty(snap.ValidatorSet, c.authorizedSigner.Load().signer))
}

// SealHash returns the hash of a block prior to it being sealed.
func (c *Eirene) SealHash(header *types.Header) common.Hash {
	return SealHash(header, c.config)
}

// APIs implements consensus.Engine, returning the user facing RPC API to allow
// controlling the signer voting.
func (c *Eirene) APIs(chain consensus.ChainHeaderReader) []rpc.API {
	return []rpc.API{{
		Namespace: "eirene",
		Version:   "1.0",
		Service:   &API{chain: chain, eirene: c},
		Public:    false,
	}}
}

// Close implements consensus.Engine. It's a noop for eirene as there are no background threads.
func (c *Eirene) Close() error {
	c.closeOnce.Do(func() {
		if c.TendermintClient != nil {
			c.TendermintClient.Close()
		}
	})

	return nil
}

func (c *Eirene) checkAndCommitSpan(
	state *state.StateDB,
	header *types.Header,
	chain core.ChainContext,
) error {
	var ctx = context.Background()
	headerNumber := header.Number.Uint64()

	span, err := c.spanner.GetCurrentSpan(ctx, header.ParentHash)
	if err != nil {
		return err
	}

	if c.needToCommitSpan(span, headerNumber) {
		return c.FetchAndCommitSpan(ctx, span.ID+1, state, header, chain)
	}

	return nil
}

func (c *Eirene) needToCommitSpan(currentSpan *span.Span, headerNumber uint64) bool {
	// if span is nil
	if currentSpan == nil {
		return false
	}

	// check span is not set initially
	if currentSpan.EndBlock == 0 {
		return true
	}

	// if current block is first block of last sprint in current span
	if currentSpan.EndBlock > c.config.CalculateSprint(headerNumber) && currentSpan.EndBlock-c.config.CalculateSprint(headerNumber)+1 == headerNumber {
		return true
	}

	return false
}

func (c *Eirene) FetchAndCommitSpan(
	ctx context.Context,
	newSpanID uint64,
	state *state.StateDB,
	header *types.Header,
	chain core.ChainContext,
) error {
	var tendermintSpan span.TendermintSpan

	if c.TendermintClient == nil {
		// fixme: move to a new mock or fake and remove c.TendermintClient completely
		s, err := c.getNextTendermintSpanForTest(ctx, newSpanID, header, chain)
		if err != nil {
			return err
		}

		tendermintSpan = *s
	} else {
		response, err := c.TendermintClient.Span(ctx, newSpanID)
		if err != nil {
			return err
		}

		tendermintSpan = *response
	}

	// check if chain id matches with Tendermint span
	if tendermintSpan.ChainID != c.chainConfig.ChainID.String() {
		return fmt.Errorf(
			"chain id proposed span, %s, and eirene chain id, %s, doesn't match",
			tendermintSpan.ChainID,
			c.chainConfig.ChainID,
		)
	}

	return c.spanner.CommitSpan(ctx, tendermintSpan, state, header, chain)
}

// CommitStates commit states
func (c *Eirene) CommitStates(
	state *state.StateDB,
	header *types.Header,
	chain statefull.ChainContext,
) ([]*types.StateSyncData, error) {
	fetchStart := time.Now()
	number := header.Number.Uint64()

	var (
		lastStateIDBig *big.Int
		from           uint64
		toTimestamp    int64
		err            error
	)

	// Fetch the LastStateId from contract via current state instance
	lastStateIDBig, err = c.GenesisContractsClient.LastStateId(state.Copy(), number-1, header.ParentHash)
	if err != nil {
		return nil, err
	}

	// Tendermint에서는 이전 블록 타임스탬프를 toTimestamp로 사용
	// Tendermint의 블록 생성 주기에 따라 적절한 값을 선택해야 함
	// 이전 스프린트의 마지막 블록 시간으로 설정
	stateSyncDelay := c.config.CalculateStateSyncDelay(number)
	toTimestamp = int64(header.Time - stateSyncDelay)

	lastStateID := lastStateIDBig.Uint64()
	from = lastStateID + 1

	log.Info(
		"Fetching state updates from Tendermint",
		"fromID", from,
		"toTimestamp", toTimestamp)

	// Tendermint ABCI 클라이언트를 통해 상태 동기화 이벤트 조회
	eventRecords, err := c.TendermintClient.StateSyncEvents(context.Background(), from, toTimestamp)
	if err != nil {
		log.Error("Error occurred when fetching state sync events", "fromID", from, "toTimestamp", toTimestamp, "err", err)
		// 에러가 발생해도 계속 진행, 이벤트를 가져오지 못한 경우 빈 목록으로 처리
		eventRecords = []*clerk.EventRecordWithTime{}
	}

	// 테스트 환경에서 이벤트 수 조정이 필요한 경우
	if c.config.OverrideStateSyncRecords != nil {
		if val, ok := c.config.OverrideStateSyncRecords[strconv.FormatUint(number, 10)]; ok {
			if val < len(eventRecords) {
				eventRecords = eventRecords[0:val]
			}
		}
	}

	fetchTime := time.Since(fetchStart)
	processStart := time.Now()
	totalGas := 0 // 상태 동기화당 가스 제한
	chainID := c.chainConfig.ChainID.String()
	stateSyncs := make([]*types.StateSyncData, 0, len(eventRecords))
	toTime := time.Unix(toTimestamp, 0)

	var gasUsed uint64

	for _, eventRecord := range eventRecords {
		if eventRecord.ID <= lastStateID {
			log.Debug("Skipping already processed state sync event", "eventID", eventRecord.ID, "lastStateID", lastStateID)
			continue
		}

		// 이벤트 레코드 검증
		if err = validateEventRecord(eventRecord, number, toTime, lastStateID, chainID); err != nil {
			log.Error("Invalid event record", "block", number, "toTimestamp", toTimestamp, "stateID", lastStateID+1, "error", err.Error())
			break
		}

		// 상태 동기화 데이터 생성
		stateData := types.StateSyncData{
			ID:       eventRecord.ID,
			Contract: eventRecord.Contract,
			Data:     hex.EncodeToString(eventRecord.Data),
			TxHash:   eventRecord.TxHash,
		}

		stateSyncs = append(stateSyncs, &stateData)

		// 상태 동기화 실행
		// 이 호출은 이벤트를 발생시켜야 함
		// 수신자 주소가 컨트랙트가 아닌 경우, 실행과 이벤트 발생이 스킵됨
		// https://github.com/maticnetwork/genesis-contracts/blob/master/contracts/StateReceiver.sol#L27
		gasUsed, err = c.GenesisContractsClient.CommitState(eventRecord, state, header, chain)
		if err != nil {
			return nil, err
		}

		totalGas += int(gasUsed)
		lastStateID++
	}

	processTime := time.Since(processStart)

	log.Info("StateSyncData processed",
		"gas", totalGas,
		"block", number,
		"lastStateID", lastStateID,
		"total records", len(eventRecords),
		"fetch time (ms)", int(fetchTime.Milliseconds()),
		"process time (ms)", int(processTime.Milliseconds()))

	return stateSyncs, nil
}

func validateEventRecord(eventRecord *clerk.EventRecordWithTime, number uint64, to time.Time, lastStateID uint64, chainID string) error {
	// event id should be sequential and event.Time should lie in the range [from, to)
	if lastStateID+1 != eventRecord.ID || eventRecord.ChainID != chainID || !eventRecord.Time.Before(to) {
		return &InvalidStateReceivedError{number, lastStateID, &to, eventRecord}
	}

	return nil
}

func (c *Eirene) SetTendermintClient(h ITendermintClient) {
	c.TendermintClient = h
}

func (c *Eirene) GetCurrentValidators(ctx context.Context, headerHash common.Hash, blockNumber uint64) ([]*valset.Validator, error) {
	return c.spanner.GetCurrentValidatorsByHash(ctx, headerHash, blockNumber)
}

//
// Private methods
//

func (c *Eirene) getNextTendermintSpanForTest(
	ctx context.Context,
	newSpanID uint64,
	header *types.Header,
	chain core.ChainContext,
) (*span.TendermintSpan, error) {
	headerNumber := header.Number.Uint64()

	spanEirene, err := c.spanner.GetCurrentSpan(ctx, header.ParentHash)
	if err != nil {
		return nil, err
	}

	// get local chain context object
	localContext := chain.(statefull.ChainContext)
	// Retrieve the snapshot needed to verify this header and cache it
	snap, err := c.snapshot(localContext.Chain, headerNumber-1, header.ParentHash, nil)
	if err != nil {
		return nil, err
	}

	// new span
	spanEirene.ID = newSpanID
	if spanEirene.EndBlock == 0 {
		spanEirene.StartBlock = 256
	} else {
		spanEirene.StartBlock = spanEirene.EndBlock + 1
	}

	spanEirene.EndBlock = spanEirene.StartBlock + (100 * c.config.CalculateSprint(headerNumber)) - 1

	selectedProducers := make([]valset.Validator, len(snap.ValidatorSet.Validators))
	for i, v := range snap.ValidatorSet.Validators {
		selectedProducers[i] = *v
	}

	tendermintSpan := &span.TendermintSpan{
		Span:              *spanEirene,
		ValidatorSet:      *snap.ValidatorSet,
		SelectedProducers: selectedProducers,
		ChainID:           c.chainConfig.ChainID.String(),
	}

	return tendermintSpan, nil
}

func validatorContains(a []*valset.Validator, x *valset.Validator) (*valset.Validator, bool) {
	for _, n := range a {
		if n.Address == x.Address {
			return n, true
		}
	}

	return nil, false
}

func getUpdatedValidatorSet(oldValidatorSet *valset.ValidatorSet, newVals []*valset.Validator) *valset.ValidatorSet {
	v := oldValidatorSet
	oldVals := v.Validators

	changes := make([]*valset.Validator, 0, len(oldVals))

	for _, ov := range oldVals {
		if f, ok := validatorContains(newVals, ov); ok {
			ov.VotingPower = f.VotingPower
		} else {
			ov.VotingPower = 0
		}

		changes = append(changes, ov)
	}

	for _, nv := range newVals {
		if _, ok := validatorContains(changes, nv); !ok {
			changes = append(changes, nv)
		}
	}

	if err := v.UpdateWithChangeSet(changes); err != nil {
		changesStr := ""
		for _, change := range changes {
			changesStr += fmt.Sprintf("Address: %s, VotingPower: %d\n", change.Address, change.VotingPower)
		}
		log.Warn("Changes in validator set", "changes", changesStr)
		log.Error("Error while updating change set", "error", err)
	}

	return v
}

func IsSprintStart(number, sprint uint64) bool {
	return number%sprint == 0
}
