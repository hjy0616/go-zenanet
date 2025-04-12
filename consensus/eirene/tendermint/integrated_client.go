package tendermint

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/tendermint/tendermint/abci/types"

	"github.com/zenanetwork/go-zenanet/consensus/eirene/clerk"
	"github.com/zenanetwork/go-zenanet/consensus/eirene/tendermint/checkpoint"
	"github.com/zenanetwork/go-zenanet/consensus/eirene/tendermint/milestone"
	"github.com/zenanetwork/go-zenanet/consensus/eirene/tendermint/span"
	"github.com/zenanetwork/go-zenanet/consensus/eirene/valset"
	"github.com/zenanetwork/go-zenanet/log"
)

// IntegratedTendermintClient combines the functionality of both HTTP and ABCI clients
type IntegratedTendermintClient struct {
	httpClient *TendermintClient     // 기존 HTTP 기반 클라이언트
	abciClient *TendermintABCIClient // 새로운 ABCI 클라이언트

	httpMutex sync.RWMutex // HTTP 클라이언트 뮤텍스
	abciMutex sync.RWMutex // ABCI 클라이언트 뮤텍스

	closeCh chan struct{} // 종료 채널
}

// NewIntegratedTendermintClient creates a new IntegratedTendermintClient with the given HTTP and ABCI endpoints
func NewIntegratedTendermintClient(httpURL, abciAddr, rpcAddr string) *IntegratedTendermintClient {
	return &IntegratedTendermintClient{
		httpClient: NewTendermintClient(httpURL),
		abciClient: NewTendermintABCIClient(abciAddr, rpcAddr),
		closeCh:    make(chan struct{}),
	}
}

// Connect establishes connections to both HTTP and ABCI endpoints
func (c *IntegratedTendermintClient) Connect() error {
	// ABCI 연결 시도
	abciErr := c.abciClient.Connect()
	if abciErr != nil {
		log.Warn("Failed to connect to ABCI endpoint", "error", abciErr)
		// ABCI 연결 실패를 치명적인 오류로 처리하지 않고 로깅만 함
		// 클라이언트는 HTTP 클라이언트와 함께 계속 작동
	}

	// 이 클라이언트는 성공적으로 연결됨으로 간주
	return nil
}

// IsConnected returns whether the client is connected to either HTTP or ABCI
func (c *IntegratedTendermintClient) IsConnected() bool {
	// ABCI 연결이 없어도 HTTP 연결이 있으면 연결된 것으로 간주
	return c.abciClient.IsConnected() || true // HTTP 클라이언트는 항상 연결됨으로 간주
}

// -- HTTP 기반 기존 메서드 구현 --

// StateSyncEvents calls the HTTP client's StateSyncEvents method
func (c *IntegratedTendermintClient) StateSyncEvents(ctx context.Context, fromID uint64, to int64) ([]*clerk.EventRecordWithTime, error) {
	c.httpMutex.RLock()
	defer c.httpMutex.RUnlock()

	return c.httpClient.StateSyncEvents(ctx, fromID, to)
}

// Span calls the HTTP client's Span method
func (c *IntegratedTendermintClient) Span(ctx context.Context, spanID uint64) (*span.TendermintSpan, error) {
	c.httpMutex.RLock()
	defer c.httpMutex.RUnlock()

	return c.httpClient.Span(ctx, spanID)
}

// FetchCheckpoint calls the HTTP client's FetchCheckpoint method
func (c *IntegratedTendermintClient) FetchCheckpoint(ctx context.Context, number int64) (*checkpoint.Checkpoint, error) {
	c.httpMutex.RLock()
	defer c.httpMutex.RUnlock()

	return c.httpClient.FetchCheckpoint(ctx, number)
}

// FetchCheckpointCount calls the HTTP client's FetchCheckpointCount method
func (c *IntegratedTendermintClient) FetchCheckpointCount(ctx context.Context) (int64, error) {
	c.httpMutex.RLock()
	defer c.httpMutex.RUnlock()

	return c.httpClient.FetchCheckpointCount(ctx)
}

// FetchMilestone calls the HTTP client's FetchMilestone method
func (c *IntegratedTendermintClient) FetchMilestone(ctx context.Context) (*milestone.Milestone, error) {
	c.httpMutex.RLock()
	defer c.httpMutex.RUnlock()

	return c.httpClient.FetchMilestone(ctx)
}

// FetchMilestoneCount calls the HTTP client's FetchMilestoneCount method
func (c *IntegratedTendermintClient) FetchMilestoneCount(ctx context.Context) (int64, error) {
	c.httpMutex.RLock()
	defer c.httpMutex.RUnlock()

	return c.httpClient.FetchMilestoneCount(ctx)
}

// FetchNoAckMilestone calls the HTTP client's FetchNoAckMilestone method
func (c *IntegratedTendermintClient) FetchNoAckMilestone(ctx context.Context, milestoneID string) error {
	c.httpMutex.RLock()
	defer c.httpMutex.RUnlock()

	return c.httpClient.FetchNoAckMilestone(ctx, milestoneID)
}

// FetchLastNoAckMilestone calls the HTTP client's FetchLastNoAckMilestone method
func (c *IntegratedTendermintClient) FetchLastNoAckMilestone(ctx context.Context) (string, error) {
	c.httpMutex.RLock()
	defer c.httpMutex.RUnlock()

	return c.httpClient.FetchLastNoAckMilestone(ctx)
}

// FetchMilestoneID calls the HTTP client's FetchMilestoneID method
func (c *IntegratedTendermintClient) FetchMilestoneID(ctx context.Context, milestoneID string) error {
	c.httpMutex.RLock()
	defer c.httpMutex.RUnlock()

	return c.httpClient.FetchMilestoneID(ctx, milestoneID)
}

// -- ABCI 기반 새로운 메서드 구현 --

// InitChain calls the ABCI client's InitChain method
func (c *IntegratedTendermintClient) InitChain(ctx context.Context, req types.RequestInitChain) (*types.ResponseInitChain, error) {
	c.abciMutex.RLock()
	defer c.abciMutex.RUnlock()

	if !c.abciClient.IsConnected() {
		return nil, fmt.Errorf("ABCI client is not connected")
	}

	return c.abciClient.InitChain(ctx, req)
}

// BeginBlock calls the ABCI client's BeginBlock method
func (c *IntegratedTendermintClient) BeginBlock(ctx context.Context, req types.RequestBeginBlock) (*types.ResponseBeginBlock, error) {
	c.abciMutex.RLock()
	defer c.abciMutex.RUnlock()

	if !c.abciClient.IsConnected() {
		return nil, fmt.Errorf("ABCI client is not connected")
	}

	return c.abciClient.BeginBlock(ctx, req)
}

// CheckTx calls the ABCI client's CheckTx method
func (c *IntegratedTendermintClient) CheckTx(ctx context.Context, req types.RequestCheckTx) (*types.ResponseCheckTx, error) {
	c.abciMutex.RLock()
	defer c.abciMutex.RUnlock()

	if !c.abciClient.IsConnected() {
		return nil, fmt.Errorf("ABCI client is not connected")
	}

	return c.abciClient.CheckTx(ctx, req)
}

// DeliverTx calls the ABCI client's DeliverTx method
func (c *IntegratedTendermintClient) DeliverTx(ctx context.Context, req types.RequestDeliverTx) (*types.ResponseDeliverTx, error) {
	c.abciMutex.RLock()
	defer c.abciMutex.RUnlock()

	if !c.abciClient.IsConnected() {
		return nil, fmt.Errorf("ABCI client is not connected")
	}

	return c.abciClient.DeliverTx(ctx, req)
}

// EndBlock calls the ABCI client's EndBlock method
func (c *IntegratedTendermintClient) EndBlock(ctx context.Context, req types.RequestEndBlock) (*types.ResponseEndBlock, error) {
	c.abciMutex.RLock()
	defer c.abciMutex.RUnlock()

	if !c.abciClient.IsConnected() {
		return nil, fmt.Errorf("ABCI client is not connected")
	}

	return c.abciClient.EndBlock(ctx, req)
}

// Commit calls the ABCI client's Commit method
func (c *IntegratedTendermintClient) Commit(ctx context.Context) (*types.ResponseCommit, error) {
	c.abciMutex.RLock()
	defer c.abciMutex.RUnlock()

	if !c.abciClient.IsConnected() {
		return nil, fmt.Errorf("ABCI client is not connected")
	}

	return c.abciClient.Commit(ctx)
}

// GetValidators calls the appropriate client's GetValidators method
func (c *IntegratedTendermintClient) GetValidators(ctx context.Context) ([]*valset.Validator, error) {
	// 먼저 ABCI 클라이언트 시도
	c.abciMutex.RLock()
	abciConnected := c.abciClient.IsConnected()
	c.abciMutex.RUnlock()

	if abciConnected {
		validators, err := c.abciClient.GetValidators(ctx)
		if err == nil {
			return validators, nil
		}
		log.Warn("Failed to get validators from ABCI client, falling back to HTTP client", "error", err)
	}

	// ABCI 실패 시 HTTP 클라이언트 사용
	c.httpMutex.RLock()
	defer c.httpMutex.RUnlock()

	return c.httpClient.GetValidators(ctx)
}

// GetCurrentValidatorSet calls the appropriate client's GetCurrentValidatorSet method
func (c *IntegratedTendermintClient) GetCurrentValidatorSet(ctx context.Context) (*valset.ValidatorSet, error) {
	// 먼저 ABCI 클라이언트 시도
	c.abciMutex.RLock()
	abciConnected := c.abciClient.IsConnected()
	c.abciMutex.RUnlock()

	if abciConnected {
		validatorSet, err := c.abciClient.GetCurrentValidatorSet(ctx)
		if err == nil {
			return validatorSet, nil
		}
		log.Warn("Failed to get current validator set from ABCI client, falling back to HTTP client", "error", err)
	}

	// ABCI 실패 시 HTTP 클라이언트 사용
	c.httpMutex.RLock()
	defer c.httpMutex.RUnlock()

	return c.httpClient.GetCurrentValidatorSet(ctx)
}

// Close closes both HTTP and ABCI clients
func (c *IntegratedTendermintClient) Close() {
	// 이미 닫혔는지 확인
	select {
	case <-c.closeCh:
		// 이미 닫힌 상태
		return
	default:
		// 아직 열려 있음, 닫기 진행
		close(c.closeCh)
	}

	var wg sync.WaitGroup

	// HTTP 클라이언트 닫기
	wg.Add(1)
	go func() {
		defer wg.Done()

		c.httpMutex.Lock()
		defer c.httpMutex.Unlock()

		if c.httpClient != nil {
			c.httpClient.Close()
			log.Info("Closed HTTP client")
		}
	}()

	// ABCI 클라이언트 닫기
	wg.Add(1)
	go func() {
		defer wg.Done()

		c.abciMutex.Lock()
		defer c.abciMutex.Unlock()

		if c.abciClient != nil {
			c.abciClient.Close()
			log.Info("Closed ABCI client")
		}
	}()

	// 최대 5초간 대기
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Info("Successfully closed all Tendermint client connections")
	case <-time.After(5 * time.Second):
		log.Warn("Timeout while waiting for Tendermint client connections to close")
	}
}
