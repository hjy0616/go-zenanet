package tendermint

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/zenanetwork/go-zenanet/common"
	"github.com/zenanetwork/go-zenanet/consensus/eirene"
	"github.com/zenanetwork/go-zenanet/consensus/eirene/valset"
	"github.com/zenanetwork/go-zenanet/log"
)

var (
	// ErrValidatorNotFound is returned when a validator is not found in the current validator set
	ErrValidatorNotFound = errors.New("validator not found in current validator set")
	// ErrEmptyValidatorSet is returned when the validator set is empty
	ErrEmptyValidatorSet = errors.New("empty validator set")
	// ErrInvalidSpan is returned when span data is invalid
	ErrInvalidSpan = errors.New("invalid span data")
)

const (
	// DefaultSyncInterval is the default interval for validator syncing
	DefaultSyncInterval = 30 * time.Second
	// DefaultRetryInterval is the default interval for retrying failed sync operations
	DefaultRetryInterval = 5 * time.Second
	// MaxSyncRetries is the maximum number of sync retries before giving up
	MaxSyncRetries = 5
)

// ValidatorSyncer is responsible for synchronizing validator information between Tendermint and Eirene
type ValidatorSyncer struct {
	client         eirene.ITendermintClient
	currentSet     *valset.ValidatorSet
	currentSpanID  uint64
	mu             sync.RWMutex
	stopCh         chan struct{}
	syncInterval   time.Duration
	lastUpdateTime time.Time
}

// NewValidatorSyncer creates a new validator syncer with the given client
func NewValidatorSyncer(client eirene.ITendermintClient) *ValidatorSyncer {
	return &ValidatorSyncer{
		client:       client,
		stopCh:       make(chan struct{}),
		syncInterval: DefaultSyncInterval,
	}
}

// Start begins the validator synchronization process
func (vs *ValidatorSyncer) Start() error {
	// 초기 검증자 세트 로드
	if err := vs.initialSync(); err != nil {
		return fmt.Errorf("failed to perform initial validator sync: %w", err)
	}

	// 주기적인 동기화 고루틴 시작
	go vs.syncLoop()

	log.Info("Validator syncer started")
	return nil
}

// Stop halts the validator synchronization process
func (vs *ValidatorSyncer) Stop() {
	select {
	case <-vs.stopCh:
		// Already stopped
	default:
		close(vs.stopCh)
		log.Info("Validator syncer stopped")
	}
}

// initialSync performs the initial synchronization of validator information
func (vs *ValidatorSyncer) initialSync() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 현재 검증자 세트 가져오기
	validatorSet, err := vs.client.GetCurrentValidatorSet(ctx)
	if err != nil {
		return fmt.Errorf("failed to get current validator set: %w", err)
	}

	if validatorSet.IsNilOrEmpty() {
		return ErrEmptyValidatorSet
	}

	// 현재 스팬 ID 초기화 (스팬 1부터 시작한다고 가정)
	// 실제로는 체인 상태나 DB에서 복구할 수 있음
	vs.currentSpanID = 1

	// 스팬 정보 조회 시도
	span, err := vs.client.Span(ctx, vs.currentSpanID)
	if err == nil && span != nil {
		// 스팬이 존재하면 해당 ID 사용
		vs.currentSpanID = span.ID
		log.Info("Initialized validator syncer with existing span", "spanID", vs.currentSpanID)
	} else {
		log.Info("No existing span found, starting with default span ID", "spanID", vs.currentSpanID)
	}

	vs.mu.Lock()
	vs.currentSet = validatorSet
	vs.lastUpdateTime = time.Now()
	vs.mu.Unlock()

	log.Info("Initial validator sync completed",
		"validators", len(validatorSet.Validators),
		"totalPower", validatorSet.TotalVotingPower())

	return nil
}

// syncLoop runs a continuous loop to synchronize validator information
func (vs *ValidatorSyncer) syncLoop() {
	ticker := time.NewTicker(vs.syncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := vs.syncValidators(); err != nil {
				log.Error("Failed to sync validators", "error", err)
			}
		case <-vs.stopCh:
			return
		}
	}
}

// syncValidators synchronizes the current validator set with Tendermint
func (vs *ValidatorSyncer) syncValidators() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 현재 Tendermint 검증자 세트 가져오기
	newValidatorSet, err := vs.client.GetCurrentValidatorSet(ctx)
	if err != nil {
		return fmt.Errorf("failed to get current validator set: %w", err)
	}

	if newValidatorSet.IsNilOrEmpty() {
		return ErrEmptyValidatorSet
	}

	vs.mu.Lock()
	defer vs.mu.Unlock()

	// 변경 사항 확인
	if vs.validatorSetChanged(newValidatorSet) {
		log.Info("Validator set changed",
			"oldCount", vs.currentSet.Size(),
			"newCount", newValidatorSet.Size(),
			"oldPower", vs.currentSet.TotalVotingPower(),
			"newPower", newValidatorSet.TotalVotingPower())

		// 검증자 세트 업데이트
		vs.currentSet = newValidatorSet
		vs.lastUpdateTime = time.Now()
	}

	return nil
}

// validatorSetChanged checks if the validator set has changed
func (vs *ValidatorSyncer) validatorSetChanged(newSet *valset.ValidatorSet) bool {
	if vs.currentSet == nil {
		return true
	}

	if vs.currentSet.Size() != newSet.Size() {
		return true
	}

	if vs.currentSet.TotalVotingPower() != newSet.TotalVotingPower() {
		return true
	}

	// 각 검증자의 투표력 변화 확인
	for _, newVal := range newSet.Validators {
		found := false
		for _, currentVal := range vs.currentSet.Validators {
			if newVal.Address == currentVal.Address {
				found = true
				if newVal.VotingPower != currentVal.VotingPower {
					return true
				}
				break
			}
		}
		if !found {
			return true
		}
	}

	return false
}

// GetCurrentValidators returns the current validator set
func (vs *ValidatorSyncer) GetCurrentValidators() []*valset.Validator {
	vs.mu.RLock()
	defer vs.mu.RUnlock()

	if vs.currentSet == nil || len(vs.currentSet.Validators) == 0 {
		return []*valset.Validator{}
	}

	// 복사본 반환
	validators := make([]*valset.Validator, len(vs.currentSet.Validators))
	for i, val := range vs.currentSet.Validators {
		validators[i] = val.Copy()
	}

	return validators
}

// GetCurrentValidatorSet returns a copy of the current validator set
func (vs *ValidatorSyncer) GetCurrentValidatorSet() *valset.ValidatorSet {
	vs.mu.RLock()
	defer vs.mu.RUnlock()

	if vs.currentSet == nil {
		return valset.NewValidatorSet([]*valset.Validator{})
	}

	return vs.currentSet.Copy()
}

// GetValidator returns a validator by address
func (vs *ValidatorSyncer) GetValidator(address common.Address) (*valset.Validator, error) {
	vs.mu.RLock()
	defer vs.mu.RUnlock()

	if vs.currentSet == nil {
		return nil, ErrEmptyValidatorSet
	}

	_, val := vs.currentSet.GetByAddress(address)
	if val == nil {
		return nil, ErrValidatorNotFound
	}

	return val.Copy(), nil
}

// GetCurrentSpanID returns the current span ID
func (vs *ValidatorSyncer) GetCurrentSpanID() uint64 {
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	return vs.currentSpanID
}

// HandleValidatorUpdate processes validator updates from Tendermint
func (vs *ValidatorSyncer) HandleValidatorUpdate(ctx context.Context, validators []*valset.Validator) error {
	if len(validators) == 0 {
		return nil
	}

	vs.mu.Lock()
	defer vs.mu.Unlock()

	// 현재 검증자 세트가 없으면 새로 생성
	if vs.currentSet == nil {
		vs.currentSet = valset.NewValidatorSet(validators)
		vs.lastUpdateTime = time.Now()
		log.Info("Created new validator set", "validators", len(validators))
		return nil
	}

	// 변경 사항 적용
	if err := vs.currentSet.UpdateWithChangeSet(validators); err != nil {
		return fmt.Errorf("failed to update validator set: %w", err)
	}

	vs.lastUpdateTime = time.Now()
	log.Info("Updated validator set",
		"validators", len(vs.currentSet.Validators),
		"totalPower", vs.currentSet.TotalVotingPower())

	return nil
}

// UpdateSpanID updates the current span ID
func (vs *ValidatorSyncer) UpdateSpanID(spanID uint64) {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	// GetProposer()를 사용하여 현재 제안자 가져오기
	vs.currentSet.Proposer = vs.currentSet.GetProposer()
	vs.currentSpanID = spanID
	log.Info("Updated span ID", "spanID", spanID)
}

// ForceSync forces an immediate synchronization with Tendermint
func (vs *ValidatorSyncer) ForceSync() error {
	return vs.syncValidators()
}

// LastUpdateTime returns the time of the last validator set update
func (vs *ValidatorSyncer) LastUpdateTime() time.Time {
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	return vs.lastUpdateTime
}
