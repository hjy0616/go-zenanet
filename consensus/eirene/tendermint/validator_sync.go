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
				if newVal.VotingPower != currentVal.VotingPower || newVal.ProposerPriority != currentVal.ProposerPriority {
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

	if vs.currentSet == nil || vs.currentSet.IsNilOrEmpty() {
		return nil
	}

	// 원본 보호를 위해 복사본 반환
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
		return nil
	}

	return vs.currentSet.Copy()
}

// GetValidator returns a validator by address
func (vs *ValidatorSyncer) GetValidator(address common.Address) (*valset.Validator, error) {
	vs.mu.RLock()
	defer vs.mu.RUnlock()

	if vs.currentSet == nil || vs.currentSet.IsNilOrEmpty() {
		return nil, ErrEmptyValidatorSet
	}

	for _, val := range vs.currentSet.Validators {
		if val.Address == address {
			return val.Copy(), nil
		}
	}

	return nil, ErrValidatorNotFound
}

// GetCurrentSpanID returns the current span ID
func (vs *ValidatorSyncer) GetCurrentSpanID() uint64 {
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	return vs.currentSpanID
}

// HandleValidatorUpdate processes validator updates and applies them to the local validator set
func (vs *ValidatorSyncer) HandleValidatorUpdate(ctx context.Context, validators []*valset.Validator) error {
	if len(validators) == 0 {
		return errors.New("empty validator update received")
	}

	vs.mu.Lock()
	defer vs.mu.Unlock()

	// 현재 세트가 없으면 새로 생성
	if vs.currentSet == nil || vs.currentSet.IsNilOrEmpty() {
		vs.currentSet = valset.NewValidatorSet(validators)
		vs.lastUpdateTime = time.Now()
		log.Info("Initial validator set created from update",
			"validators", len(validators),
			"totalPower", vs.currentSet.TotalVotingPower())
		return nil
	}

	// 검증자 ID 유지를 위한 맵 구성
	idMap := make(map[common.Address]uint64)
	for _, val := range vs.currentSet.Validators {
		if val.ID != 0 {
			idMap[val.Address] = val.ID
		}
	}

	// 새 검증자에게 ID 할당 (기존 ID 유지)
	for _, val := range validators {
		if id, exists := idMap[val.Address]; exists && val.ID == 0 {
			val.ID = id
		}
	}

	// 검증자 세트 업데이트
	oldSet := vs.currentSet.Copy()
	newSet := getUpdatedValidatorSet(oldSet, validators)

	// 변경 사항 로깅
	if vs.validatorSetChanged(newSet) {
		addedCount, removedCount, changedCount := countValidatorChanges(oldSet, newSet)

		log.Info("Validator set updated",
			"added", addedCount,
			"removed", removedCount,
			"changed", changedCount,
			"totalCount", newSet.Size(),
			"totalPower", newSet.TotalVotingPower())

		vs.currentSet = newSet
		vs.lastUpdateTime = time.Now()
	}

	return nil
}

// countValidatorChanges 함수는 이전 검증자 세트와 새 검증자 세트 간의 변경 사항을 계산합니다.
func countValidatorChanges(oldSet, newSet *valset.ValidatorSet) (added, removed, changed int) {
	if oldSet == nil || oldSet.IsNilOrEmpty() {
		return len(newSet.Validators), 0, 0
	}

	// 주소 기반 맵 생성
	oldMap := make(map[common.Address]*valset.Validator)
	for _, val := range oldSet.Validators {
		oldMap[val.Address] = val
	}

	newMap := make(map[common.Address]*valset.Validator)
	for _, val := range newSet.Validators {
		newMap[val.Address] = val
	}

	// 추가/변경된 검증자 계산
	for addr, newVal := range newMap {
		oldVal, exists := oldMap[addr]
		if !exists {
			added++
		} else if newVal.VotingPower != oldVal.VotingPower {
			changed++
		}
	}

	// 제거된 검증자 계산
	for addr := range oldMap {
		if _, exists := newMap[addr]; !exists {
			removed++
		}
	}

	return added, removed, changed
}

// UpdateSpanID updates the current span ID
func (vs *ValidatorSyncer) UpdateSpanID(spanID uint64) {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	if spanID > vs.currentSpanID {
		log.Info("Updating span ID", "old", vs.currentSpanID, "new", spanID)
		vs.currentSpanID = spanID
	}
}

// ForceSync triggers an immediate validator synchronization
func (vs *ValidatorSyncer) ForceSync() error {
	return vs.syncValidators()
}

// LastUpdateTime returns the timestamp of the last validator set update
func (vs *ValidatorSyncer) LastUpdateTime() time.Time {
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	return vs.lastUpdateTime
}

// UpdateValidatorPriorities 메서드는 검증자 우선순위를 업데이트합니다.
// 이는 블록 생성 순서에 영향을 줍니다.
func (vs *ValidatorSyncer) UpdateValidatorPriorities(times int) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	if vs.currentSet == nil || vs.currentSet.IsNilOrEmpty() {
		return ErrEmptyValidatorSet
	}

	// 우선순위 업데이트 (Tendermint 알고리즘과 일치)
	vs.currentSet.IncrementProposerPriority(times)
	log.Debug("Updated validator priorities",
		"times", times,
		"newProposer", vs.currentSet.GetProposer().Address.Hex())

	return nil
}

// GetProposer 메서드는 현재 블록 제안자를 반환합니다.
func (vs *ValidatorSyncer) GetProposer() *valset.Validator {
	vs.mu.RLock()
	defer vs.mu.RUnlock()

	if vs.currentSet == nil || vs.currentSet.IsNilOrEmpty() {
		return nil
	}

	return vs.currentSet.GetProposer().Copy()
}

// IsValidator 메서드는 주어진 주소가 현재 검증자 세트에 포함되어 있는지 확인합니다.
func (vs *ValidatorSyncer) IsValidator(address common.Address) bool {
	vs.mu.RLock()
	defer vs.mu.RUnlock()

	if vs.currentSet == nil || vs.currentSet.IsNilOrEmpty() {
		return false
	}

	return vs.currentSet.HasAddress(address)
}

// IsProposer 메서드는 주어진 주소가 현재 블록 제안자인지 확인합니다.
func (vs *ValidatorSyncer) IsProposer(address common.Address) bool {
	vs.mu.RLock()
	defer vs.mu.RUnlock()

	if vs.currentSet == nil || vs.currentSet.IsNilOrEmpty() {
		return false
	}

	proposer := vs.currentSet.GetProposer()
	return proposer != nil && proposer.Address == address
}

// getUpdatedValidatorSet 함수는 이전 검증자 세트와 새 검증자 목록을 기반으로 업데이트된 검증자 세트를 반환합니다.
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

// validatorContains 함수는 주어진 validator가 목록에 포함되어 있는지 확인합니다.
func validatorContains(a []*valset.Validator, x *valset.Validator) (*valset.Validator, bool) {
	for _, n := range a {
		if n.Address == x.Address {
			return n, true
		}
	}

	return nil, false
}
