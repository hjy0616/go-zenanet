package tendermint

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/zenanetwork/go-zenanet/common"
	"github.com/zenanetwork/go-zenanet/consensus/eirene"
	"github.com/zenanetwork/go-zenanet/consensus/eirene/tendermint/span"
	"github.com/zenanetwork/go-zenanet/consensus/eirene/valset"
	"github.com/zenanetwork/go-zenanet/log"
)

var (
	// ErrSpanNotFound is returned when a span is not found
	ErrSpanNotFound = errors.New("span not found")
	// ErrInvalidSpanRange is returned when span start block is greater than or equal to end block
	ErrInvalidSpanRange = errors.New("invalid span range: start block must be less than end block")
	// ErrSpanAlreadyExists is returned when a span with the given ID already exists
	ErrSpanAlreadyExists = errors.New("span already exists with given ID")
	// ErrNoSelectedProducers is returned when no producers are selected for a span
	ErrNoSelectedProducers = errors.New("no selected producers for span")
)

// SpanManager manages spans for validator rotation and block production scheduling
type SpanManager struct {
	client        eirene.ITendermintClient
	validatorSync *ValidatorSyncer
	currentSpan   *span.TendermintSpan
	mu            sync.RWMutex
	stopCh        chan struct{}

	// 스팬 설정 값
	spanDuration      uint64 // 한 스팬의 블록 수
	producerCount     int    // 각 스팬에 선택할 블록 생성자 수
	blockCycleTimeout time.Duration
}

// NewSpanManager creates a new span manager
func NewSpanManager(client eirene.ITendermintClient, validatorSync *ValidatorSyncer, options ...SpanOption) *SpanManager {
	sm := &SpanManager{
		client:            client,
		validatorSync:     validatorSync,
		stopCh:            make(chan struct{}),
		spanDuration:      100, // 기본값: 100 블록마다 스팬 변경
		producerCount:     10,  // 기본값: 10명의 블록 생성자 선택
		blockCycleTimeout: 30 * time.Second,
	}

	// 옵션 적용
	for _, option := range options {
		option(sm)
	}

	return sm
}

// SpanOption configures the SpanManager
type SpanOption func(*SpanManager)

// WithSpanDuration sets the number of blocks per span
func WithSpanDuration(blocks uint64) SpanOption {
	return func(sm *SpanManager) {
		if blocks > 0 {
			sm.spanDuration = blocks
		}
	}
}

// WithProducerCount sets the number of block producers to select per span
func WithProducerCount(count int) SpanOption {
	return func(sm *SpanManager) {
		if count > 0 {
			sm.producerCount = count
		}
	}
}

// WithBlockCycleTimeout sets the timeout for waiting for a block cycle
func WithBlockCycleTimeout(timeout time.Duration) SpanOption {
	return func(sm *SpanManager) {
		if timeout > 0 {
			sm.blockCycleTimeout = timeout
		}
	}
}

// Start initializes the span manager and starts monitoring for span changes
func (sm *SpanManager) Start() error {
	// 초기 스팬 로드
	if err := sm.initializeCurrentSpan(); err != nil {
		log.Error("Failed to initialize current span", "error", err)
		// 실패하더라도 계속 진행 (초기 시작 시 스팬이 없을 수 있음)
	}

	log.Info("Span manager started",
		"spanDuration", sm.spanDuration,
		"producerCount", sm.producerCount)
	return nil
}

// Stop halts the span manager
func (sm *SpanManager) Stop() {
	select {
	case <-sm.stopCh:
		// Already stopped
	default:
		close(sm.stopCh)
		log.Info("Span manager stopped")
	}
}

// initializeCurrentSpan loads the current span from Tendermint
func (sm *SpanManager) initializeCurrentSpan() error {
	spanID := sm.validatorSync.GetCurrentSpanID()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Tendermint에서 스팬 정보 로드
	tendermintSpan, err := sm.client.Span(ctx, spanID)
	if err != nil {
		// 스팬을 찾을 수 없는 경우 새 스팬 생성
		if errors.Is(err, ErrSpanNotFound) {
			return sm.createNewSpan(ctx, spanID, 0)
		}
		return fmt.Errorf("failed to get span from Tendermint: %w", err)
	}

	sm.mu.Lock()
	sm.currentSpan = tendermintSpan
	sm.mu.Unlock()

	log.Info("Initialized current span",
		"spanID", tendermintSpan.ID,
		"startBlock", tendermintSpan.StartBlock,
		"endBlock", tendermintSpan.EndBlock,
		"validators", tendermintSpan.ValidatorSet.Size(),
		"producers", len(tendermintSpan.SelectedProducers))

	return nil
}

// GetCurrentSpan returns a copy of the current span
func (sm *SpanManager) GetCurrentSpan() *span.TendermintSpan {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	if sm.currentSpan == nil {
		return nil
	}

	// 반환할 때는 복사본 생성
	return &span.TendermintSpan{
		Span: span.Span{
			ID:         sm.currentSpan.ID,
			StartBlock: sm.currentSpan.StartBlock,
			EndBlock:   sm.currentSpan.EndBlock,
		},
		ValidatorSet:      *sm.currentSpan.ValidatorSet.Copy(),
		SelectedProducers: copyValidatorList(sm.currentSpan.SelectedProducers),
		ChainID:           sm.currentSpan.ChainID,
	}
}

// copyValidatorList creates a deep copy of a validator list
func copyValidatorList(validators []valset.Validator) []valset.Validator {
	if validators == nil {
		return nil
	}

	copied := make([]valset.Validator, len(validators))
	for i, v := range validators {
		copied[i] = *v.Copy()
	}
	return copied
}

// ShouldUpdateSpan checks if it's time to update to a new span based on the current block number
func (sm *SpanManager) ShouldUpdateSpan(blockNum uint64) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	if sm.currentSpan == nil {
		return true
	}

	// 현재 블록이 현재 스팬의 종료 블록 이상이면 새 스팬이 필요
	return blockNum >= sm.currentSpan.EndBlock
}

// UpdateToNextSpan updates to the next span
func (sm *SpanManager) UpdateToNextSpan(ctx context.Context, currentBlockNum uint64) (*span.TendermintSpan, error) {
	nextSpanID := sm.validatorSync.GetCurrentSpanID() + 1
	startBlock := currentBlockNum + 1

	return sm.createNewSpan(ctx, nextSpanID, startBlock)
}

// createNewSpan creates a new span and sets it as the current span
func (sm *SpanManager) createNewSpan(ctx context.Context, spanID uint64, startBlock uint64) (*span.TendermintSpan, error) {
	validatorSet := sm.validatorSync.GetCurrentValidatorSet()
	if validatorSet.IsNilOrEmpty() {
		return nil, ErrEmptyValidatorSet
	}

	// 스팬 기간 계산
	endBlock := startBlock + sm.spanDuration - 1

	// 블록 생성자 선택
	selectedProducers, err := sm.selectBlockProducers(validatorSet)
	if err != nil {
		return nil, fmt.Errorf("failed to select block producers: %w", err)
	}

	// 새 스팬 생성
	newSpan := &span.TendermintSpan{
		Span: span.Span{
			ID:         spanID,
			StartBlock: startBlock,
			EndBlock:   endBlock,
		},
		ValidatorSet:      *validatorSet,
		SelectedProducers: selectedProducers,
		ChainID:           "eirene", // 체인 ID 설정
	}

	sm.mu.Lock()
	sm.currentSpan = newSpan
	sm.mu.Unlock()

	// 검증자 동기화 모듈에도 스팬 ID 업데이트
	sm.validatorSync.UpdateSpanID(spanID)

	log.Info("Created new span",
		"spanID", spanID,
		"startBlock", startBlock,
		"endBlock", endBlock,
		"validators", validatorSet.Size(),
		"producers", len(selectedProducers))

	return newSpan, nil
}

// selectBlockProducers selects block producers for the span based on voting power
func (sm *SpanManager) selectBlockProducers(validatorSet *valset.ValidatorSet) ([]valset.Validator, error) {
	if validatorSet.IsNilOrEmpty() {
		return nil, ErrEmptyValidatorSet
	}

	validators := validatorSet.Validators
	numValidators := len(validators)

	// 프로듀서 수는 검증자 수를 초과할 수 없음
	producerCount := sm.producerCount
	if producerCount > numValidators {
		producerCount = numValidators
	}

	// 검증자 복사 및 투표력 기준 정렬
	sortedValidators := make([]*valset.Validator, numValidators)
	for i, v := range validators {
		sortedValidators[i] = v.Copy()
	}

	// 투표력 기준 내림차순 정렬
	sort.Slice(sortedValidators, func(i, j int) bool {
		return sortedValidators[i].VotingPower > sortedValidators[j].VotingPower
	})

	// 상위 producerCount 개의 검증자 선택
	selectedProducers := make([]valset.Validator, producerCount)
	for i := 0; i < producerCount; i++ {
		selectedProducers[i] = *sortedValidators[i]
	}

	// 선택된 프로듀서가 없으면 오류 반환
	if len(selectedProducers) == 0 {
		return nil, ErrNoSelectedProducers
	}

	return selectedProducers, nil
}

// GetProducerForBlock determines which producer should produce the given block
func (sm *SpanManager) GetProducerForBlock(blockNum uint64) (*valset.Validator, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	if sm.currentSpan == nil {
		return nil, ErrSpanNotFound
	}

	producers := sm.currentSpan.SelectedProducers
	if len(producers) == 0 {
		return nil, ErrNoSelectedProducers
	}

	// 블록 번호를 프로듀서 수로 나눈 나머지를 인덱스로 사용
	// 이렇게 하면 라운드 로빈 방식으로 프로듀서가 할당됨
	producerIndex := int(blockNum % uint64(len(producers)))

	// 복사본 반환
	producer := producers[producerIndex]
	return producer.Copy(), nil
}

// ShouldProduceBlockAtTime checks if the current validator should produce a block at the given time
func (sm *SpanManager) ShouldProduceBlockAtTime(blockNum uint64, validatorAddress common.Address) (bool, error) {
	producer, err := sm.GetProducerForBlock(blockNum)
	if err != nil {
		return false, err
	}

	// 현재 검증자가 해당 블록의 프로듀서인지 확인
	return producer.Address == validatorAddress, nil
}

// IsValidatorInCurrentSpan checks if the given validator is part of the current span
func (sm *SpanManager) IsValidatorInCurrentSpan(validatorAddress common.Address) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	if sm.currentSpan == nil || sm.currentSpan.ValidatorSet.IsNilOrEmpty() {
		return false
	}

	return sm.currentSpan.ValidatorSet.HasAddress(validatorAddress)
}

// GetValidatorsBySpanID fetches validators for a specific span
func (sm *SpanManager) GetValidatorsBySpanID(ctx context.Context, spanID uint64) ([]*valset.Validator, error) {
	// 현재 스팬이면 메모리에서 바로 반환
	sm.mu.RLock()
	if sm.currentSpan != nil && sm.currentSpan.ID == spanID {
		validators := sm.currentSpan.ValidatorSet.Validators
		result := make([]*valset.Validator, len(validators))
		for i, v := range validators {
			result[i] = v.Copy()
		}
		sm.mu.RUnlock()
		return result, nil
	}
	sm.mu.RUnlock()

	// 요청된 스팬이 현재 스팬이 아니면 Tendermint에서 조회
	tendermintSpan, err := sm.client.Span(ctx, spanID)
	if err != nil {
		return nil, fmt.Errorf("failed to get span from Tendermint: %w", err)
	}

	validators := tendermintSpan.ValidatorSet.Validators
	result := make([]*valset.Validator, len(validators))
	for i, v := range validators {
		result[i] = v.Copy()
	}

	return result, nil
}
