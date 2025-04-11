package tendermint

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	abciclient "github.com/tendermint/tendermint/abci/client"
	"github.com/tendermint/tendermint/abci/types"
	tmrpc "github.com/tendermint/tendermint/rpc/client/http"

	"github.com/zenanetwork/go-zenanet/common"
	"github.com/zenanetwork/go-zenanet/consensus/eirene/clerk"
	"github.com/zenanetwork/go-zenanet/consensus/eirene/tendermint/checkpoint"
	"github.com/zenanetwork/go-zenanet/consensus/eirene/tendermint/milestone"
	"github.com/zenanetwork/go-zenanet/consensus/eirene/tendermint/span"
	"github.com/zenanetwork/go-zenanet/consensus/eirene/valset"
	"github.com/zenanetwork/go-zenanet/log"
)

var (
	// ErrABCINotConnected is returned when the ABCI client is not connected
	ErrABCINotConnected = errors.New("ABCI client is not connected")
	// ErrRPCNotConnected is returned when the RPC client is not connected
	ErrRPCNotConnected = errors.New("RPC client is not connected")
	// ErrMaxRetriesExceeded is returned when max retries for an operation is exceeded
	ErrMaxRetriesExceeded = errors.New("maximum retries exceeded")
)

const (
	// Connection timeout
	connectionTimeout = 10 * time.Second
	// Dial retry delay
	dialRetryDelay = 3 * time.Second
	// Default reconnect interval
	defaultReconnectInterval = 5 * time.Second
	// Default maximum retries
	defaultMaxRetries = 3
)

// TendermintABCIClient implements the ITendermintClient interface for communication with Tendermint via ABCI
type TendermintABCIClient struct {
	abciClient abciclient.Client // Tendermint ABCI 클라이언트 (26658)
	rpcClient  *tmrpc.HTTP       // Tendermint RPC 클라이언트 (26657)

	abciAddr string // ABCI 주소 (26658)
	rpcAddr  string // RPC 주소 (26657)

	abciMutex sync.RWMutex // ABCI 연결 뮤텍스
	rpcMutex  sync.RWMutex // RPC 연결 뮤텍스

	isABCIConnected bool // ABCI 연결 상태
	isRPCConnected  bool // RPC 연결 상태

	reconnectInterval time.Duration // 재연결 시도 간격
	maxRetries        int           // 최대 재시도 횟수

	closeCh     chan struct{} // 종료 채널
	reconnectCh chan struct{} // 재연결 채널
}

// NewTendermintABCIClient creates a new TendermintABCIClient with the given ABCI and RPC addresses
func NewTendermintABCIClient(abciAddr, rpcAddr string) *TendermintABCIClient {
	return &TendermintABCIClient{
		abciAddr:          abciAddr,
		rpcAddr:           rpcAddr,
		reconnectInterval: defaultReconnectInterval,
		maxRetries:        defaultMaxRetries,
		closeCh:           make(chan struct{}),
		reconnectCh:       make(chan struct{}, 1),
	}
}

// SetReconnectInterval sets the interval between reconnection attempts
func (c *TendermintABCIClient) SetReconnectInterval(interval time.Duration) {
	c.reconnectInterval = interval
}

// SetMaxRetries sets the maximum number of retries for operations
func (c *TendermintABCIClient) SetMaxRetries(maxRetries int) {
	c.maxRetries = maxRetries
}

// Connect establishes a connection to both ABCI and RPC endpoints
func (c *TendermintABCIClient) Connect() error {
	// ABCI 클라이언트 연결
	abciErr := c.connectABCI()
	if abciErr != nil {
		return fmt.Errorf("failed to connect to ABCI endpoint: %w", abciErr)
	}

	// RPC 클라이언트 연결
	rpcErr := c.connectRPC()
	if rpcErr != nil {
		return fmt.Errorf("failed to connect to RPC endpoint: %w", rpcErr)
	}

	// 자동 재연결 고루틴 시작
	go c.reconnectLoop()

	return nil
}

// connectABCI establishes a connection to the ABCI endpoint
func (c *TendermintABCIClient) connectABCI() error {
	c.abciMutex.Lock()
	defer c.abciMutex.Unlock()

	if c.isABCIConnected {
		return nil
	}

	// 로컬 TCP 소켓을 통해 ABCI 클라이언트 생성
	client := abciclient.NewSocketClient(c.abciAddr, true)

	// Context와 함께 연결 시도, ctx를 변수로 사용시 수정 필요
	_, cancel := context.WithTimeout(context.Background(), connectionTimeout)
	defer cancel()

	// Start 메서드는 연결 시도를 수행
	if err := client.Start(); err != nil {
		return fmt.Errorf("error starting ABCI client: %w", err)
	}

	// 연결이 성공적으로 이루어졌는지 확인
	if !client.IsRunning() {
		return errors.New("ABCI client failed to start")
	}

	c.abciClient = client
	c.isABCIConnected = true
	log.Info("Connected to Tendermint ABCI endpoint", "address", c.abciAddr)

	return nil
}

// connectRPC establishes a connection to the RPC endpoint
func (c *TendermintABCIClient) connectRPC() error {
	c.rpcMutex.Lock()
	defer c.rpcMutex.Unlock()

	if c.isRPCConnected {
		return nil
	}

	// HTTP를 통한 RPC 클라이언트 생성
	client, err := tmrpc.New(c.rpcAddr, "/websocket")
	if err != nil {
		return fmt.Errorf("error creating RPC client: %w", err)
	}

	// Context와 함께 연결 시도, ctx 변수 사용시 수정 필요
	_, cancel := context.WithTimeout(context.Background(), connectionTimeout)
	defer cancel()

	if err := client.Start(); err != nil {
		return fmt.Errorf("error starting RPC client: %w", err)
	}

	c.rpcClient = client
	c.isRPCConnected = true
	log.Info("Connected to Tendermint RPC endpoint", "address", c.rpcAddr)

	return nil
}

// IsConnected returns whether both ABCI and RPC clients are connected
func (c *TendermintABCIClient) IsConnected() bool {
	c.abciMutex.RLock()
	abciConnected := c.isABCIConnected
	c.abciMutex.RUnlock()

	c.rpcMutex.RLock()
	rpcConnected := c.isRPCConnected
	c.rpcMutex.RUnlock()

	return abciConnected && rpcConnected
}

// retry 함수는 주어진 작업을 최대 maxRetries 횟수만큼 재시도합니다.
func (c *TendermintABCIClient) retry(operation string, fn func() error) error {
	var lastErr error
	for i := 0; i < c.maxRetries; i++ {
		if i > 0 {
			log.Debug("Retrying operation", "operation", operation, "attempt", i+1, "maxRetries", c.maxRetries)
		}

		lastErr = fn()
		if lastErr == nil {
			return nil
		}

		// ABCI 또는 RPC 연결 오류인 경우 재연결 시도
		if errors.Is(lastErr, ErrABCINotConnected) || errors.Is(lastErr, ErrRPCNotConnected) {
			c.triggerReconnect()
			time.Sleep(dialRetryDelay) // 재연결 시도 후 잠시 대기
			continue
		}

		// 일시적인 오류로 판단되는 경우 재시도
		if isTemporaryError(lastErr) {
			time.Sleep(dialRetryDelay)
			continue
		}

		// 그 외의 오류는 즉시 반환 (예: 비즈니스 로직 오류)
		return lastErr
	}

	return fmt.Errorf("%w: %s after %d attempts: %v", ErrMaxRetriesExceeded, operation, c.maxRetries, lastErr)
}

// retryWithContext 함수는 컨텍스트를 고려하여 주어진 작업을 최대 maxRetries 횟수만큼 재시도합니다.
func (c *TendermintABCIClient) retryWithContext(ctx context.Context, operation string, fn func() error) error {
	var lastErr error
	for i := 0; i < c.maxRetries; i++ {
		// 컨텍스트 타임아웃/취소 확인
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// 계속 진행
		}

		if i > 0 {
			log.Debug("Retrying operation with context", "operation", operation, "attempt", i+1, "maxRetries", c.maxRetries)
		}

		lastErr = fn()
		if lastErr == nil {
			return nil
		}

		// ABCI 또는 RPC 연결 오류인 경우 재연결 시도
		if errors.Is(lastErr, ErrABCINotConnected) || errors.Is(lastErr, ErrRPCNotConnected) {
			c.triggerReconnect()

			// 컨텍스트 타임아웃/취소 확인을 포함한 대기
			timer := time.NewTimer(dialRetryDelay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
				// 계속 진행
			}

			continue
		}

		// 일시적인 오류로 판단되는 경우 재시도
		if isTemporaryError(lastErr) {
			// 컨텍스트 타임아웃/취소 확인을 포함한 대기
			timer := time.NewTimer(dialRetryDelay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
				// 계속 진행
			}

			continue
		}

		// 그 외의 오류는 즉시 반환 (예: 비즈니스 로직 오류)
		return lastErr
	}

	return fmt.Errorf("%w: %s after %d attempts: %v", ErrMaxRetriesExceeded, operation, c.maxRetries, lastErr)
}

// isTemporaryError는 주어진 오류가 일시적인 오류인지 판단합니다.
func isTemporaryError(err error) bool {
	if err == nil {
		return false
	}

	// 타임아웃, 연결 거부 등 네트워크 관련 일시적 오류 판단
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}

	// 오류 메시지에 기반한 일시적 오류 판단
	errMsg := err.Error()
	return strings.Contains(errMsg, "timeout") ||
		strings.Contains(errMsg, "connection refused") ||
		strings.Contains(errMsg, "connection reset") ||
		strings.Contains(errMsg, "temporarily unavailable") ||
		strings.Contains(errMsg, "EOF")
}

// InitChain implements the ITendermintClient interface
func (c *TendermintABCIClient) InitChain(ctx context.Context, req types.RequestInitChain) (*types.ResponseInitChain, error) {
	var response *types.ResponseInitChain

	err := c.retry("InitChain", func() error {
		c.abciMutex.RLock()
		defer c.abciMutex.RUnlock()

		if !c.isABCIConnected {
			return ErrABCINotConnected
		}

		resp := c.abciClient.InitChainSync(req)
		if resp.Error != nil {
			return resp.Error
		}

		response = &resp.Response
		return nil
	})

	return response, err
}

// BeginBlock implements the ITendermintClient interface
func (c *TendermintABCIClient) BeginBlock(ctx context.Context, req types.RequestBeginBlock) (*types.ResponseBeginBlock, error) {
	var response *types.ResponseBeginBlock

	err := c.retry("BeginBlock", func() error {
		c.abciMutex.RLock()
		defer c.abciMutex.RUnlock()

		if !c.isABCIConnected {
			return ErrABCINotConnected
		}

		resp := c.abciClient.BeginBlockSync(req)
		if resp.Error != nil {
			return resp.Error
		}

		response = &resp.Response
		return nil
	})

	return response, err
}

// CheckTx implements the ITendermintClient interface
func (c *TendermintABCIClient) CheckTx(ctx context.Context, req types.RequestCheckTx) (*types.ResponseCheckTx, error) {
	var response *types.ResponseCheckTx

	err := c.retry("CheckTx", func() error {
		c.abciMutex.RLock()
		defer c.abciMutex.RUnlock()

		if !c.isABCIConnected {
			return ErrABCINotConnected
		}

		resp := c.abciClient.CheckTxSync(req)
		if resp.Error != nil {
			return resp.Error
		}

		response = &resp.Response
		return nil
	})

	return response, err
}

// DeliverTx implements the ITendermintClient interface
func (c *TendermintABCIClient) DeliverTx(ctx context.Context, req types.RequestDeliverTx) (*types.ResponseDeliverTx, error) {
	var response *types.ResponseDeliverTx

	err := c.retry("DeliverTx", func() error {
		c.abciMutex.RLock()
		defer c.abciMutex.RUnlock()

		if !c.isABCIConnected {
			return ErrABCINotConnected
		}

		resp := c.abciClient.DeliverTxSync(req)
		if resp.Error != nil {
			return resp.Error
		}

		response = &resp.Response
		return nil
	})

	return response, err
}

// EndBlock implements the ITendermintClient interface
func (c *TendermintABCIClient) EndBlock(ctx context.Context, req types.RequestEndBlock) (*types.ResponseEndBlock, error) {
	var response *types.ResponseEndBlock

	err := c.retry("EndBlock", func() error {
		c.abciMutex.RLock()
		defer c.abciMutex.RUnlock()

		if !c.isABCIConnected {
			return ErrABCINotConnected
		}

		resp := c.abciClient.EndBlockSync(req)
		if resp.Error != nil {
			return resp.Error
		}

		response = &resp.Response
		return nil
	})

	return response, err
}

// Commit implements the ITendermintClient interface
func (c *TendermintABCIClient) Commit(ctx context.Context) (*types.ResponseCommit, error) {
	var response *types.ResponseCommit

	err := c.retry("Commit", func() error {
		c.abciMutex.RLock()
		defer c.abciMutex.RUnlock()

		if !c.isABCIConnected {
			return ErrABCINotConnected
		}

		resp := c.abciClient.CommitSync()
		if resp.Error != nil {
			return resp.Error
		}

		response = &resp.Response
		return nil
	})

	return response, err
}

// GetValidators implements the ITendermintClient interface
func (c *TendermintABCIClient) GetValidators(ctx context.Context) ([]*valset.Validator, error) {
	var validators []*valset.Validator

	err := c.retry("GetValidators", func() error {
		c.rpcMutex.RLock()
		defer c.rpcMutex.RUnlock()

		if !c.isRPCConnected {
			return ErrRPCNotConnected
		}

		// Tendermint RPC를 통해 검증자 목록 가져오기
		validatorsResp, err := c.rpcClient.Validators(ctx, nil, nil, nil)
		if err != nil {
			return fmt.Errorf("failed to fetch validators: %w", err)
		}

		result := make([]*valset.Validator, 0, len(validatorsResp.Validators))
		for _, v := range validatorsResp.Validators {
			// Tendermint 주소를 Ethereum 주소로 변환
			// Tendermint 주소는 일반적으로 20바이트이므로 common.Address로 변환 가능
			var ethAddr common.Address
			addrBytes := v.Address
			if len(addrBytes) > common.AddressLength {
				addrBytes = addrBytes[:common.AddressLength]
			}
			copy(ethAddr[:], addrBytes)

			// Tendermint 검증자를 Eirene 검증자로 변환
			validator := valset.NewValidator(ethAddr, v.VotingPower)
			validator.ProposerPriority = v.ProposerPriority

			// ID는 주소의 해시 또는 적절한 방법으로 설정
			// 여기서는 주소의 첫 8바이트를 uint64로 변환하여 ID로 사용
			validator.ID = binary.BigEndian.Uint64(append(ethAddr.Bytes()[:8], make([]byte, 8-len(ethAddr.Bytes()[:8]))...))

			result = append(result, validator)
		}

		validators = result
		return nil
	})

	return validators, err
}

// GetCurrentValidatorSet implements the ITendermintClient interface
func (c *TendermintABCIClient) GetCurrentValidatorSet(ctx context.Context) (*valset.ValidatorSet, error) {
	validators, err := c.GetValidators(ctx)
	if err != nil {
		return nil, err
	}

	// 검증자 세트 생성
	validatorSet := valset.NewValidatorSet(validators)
	return validatorSet, nil
}

// StateSyncEvents는 특정 ID부터 지정된 블록까지의 상태 동기화 이벤트를 검색합니다.
func (c *TendermintABCIClient) StateSyncEvents(ctx context.Context, fromID uint64, to int64) ([]*clerk.EventRecordWithTime, error) {
	var events []*clerk.EventRecordWithTime

	err := c.retryWithContext(ctx, "StateSyncEvents", func() error {
		c.rpcMutex.RLock()
		defer c.rpcMutex.RUnlock()

		if !c.isRPCConnected {
			return ErrRPCNotConnected
		}

		// Tendermint RPC를 통해 이벤트 조회
		// 여기서는 tx_search를 사용하여 특정 타입의 이벤트를 조회합니다
		query := fmt.Sprintf("state_sync.from_id >= %d AND state_sync.to_block <= %d", fromID, to)
		searchResult, err := c.rpcClient.TxSearch(ctx, query, false, nil, nil, "asc")
		if err != nil {
			return fmt.Errorf("failed to search state sync events: %w", err)
		}

		// 결과를 EventRecordWithTime 형태로 변환
		result := make([]*clerk.EventRecordWithTime, 0, len(searchResult.Txs))
		for _, tx := range searchResult.Txs {
			// 각 트랜잭션에서 이벤트 데이터 추출
			for _, event := range tx.TxResult.Events {
				if event.Type != "state_sync" {
					continue
				}

				// 이벤트 속성에서 필요한 데이터 추출
				var id uint64
				var contract common.Address
				var recordTime time.Time
				var data []byte
				var txHash common.Hash
				var logIndex uint64
				var chainID string

				for _, attr := range event.Attributes {
					key := string(attr.Key)
					value := attr.Value

					switch key {
					case "id":
						id, _ = strconv.ParseUint(string(value), 10, 64)
					case "contract":
						contract = common.HexToAddress(string(value))
					case "time":
						timeInt, _ := strconv.ParseInt(string(value), 10, 64)
						recordTime = time.Unix(timeInt, 0)
					case "data":
						data = value
					case "tx_hash":
						txHash = common.HexToHash(string(value))
					case "log_index":
						logIndex, _ = strconv.ParseUint(string(value), 10, 64)
					case "chain_id":
						chainID = string(value)
					}
				}

				// EventRecordWithTime 생성 및 추가
				record := clerk.EventRecord{
					ID:       id,
					Contract: contract,
					Data:     data,
					TxHash:   txHash,
					LogIndex: logIndex,
					ChainID:  chainID,
				}
				eventWithTime := &clerk.EventRecordWithTime{
					EventRecord: record,
					Time:        recordTime,
				}
				result = append(result, eventWithTime)
			}
		}

		events = result
		return nil
	})

	return events, err
}

// Span는 지정된 spanID에 해당하는 Tendermint 스팬 정보를 가져옵니다.
func (c *TendermintABCIClient) Span(ctx context.Context, spanID uint64) (*span.TendermintSpan, error) {
	var tendermintSpan *span.TendermintSpan

	err := c.retryWithContext(ctx, "Span", func() error {
		c.rpcMutex.RLock()
		defer c.rpcMutex.RUnlock()

		if !c.isRPCConnected {
			return ErrRPCNotConnected
		}

		// Tendermint ABCI Query를 사용하여 스팬 정보 조회
		path := fmt.Sprintf("span/%d", spanID)
		queryResult, err := c.rpcClient.ABCIQuery(ctx, path, nil)
		if err != nil {
			return fmt.Errorf("failed to query span: %w", err)
		}

		if queryResult.Response.Code != 0 {
			return fmt.Errorf("query failed with code %d: %s",
				queryResult.Response.Code, queryResult.Response.Log)
		}

		// 응답 데이터가 없는 경우
		if len(queryResult.Response.Value) == 0 {
			return fmt.Errorf("no span found for ID %d", spanID)
		}

		// 응답 데이터를 TendermintSpan으로 언마샬
		var result span.TendermintSpan
		if err := json.Unmarshal(queryResult.Response.Value, &result); err != nil {
			return fmt.Errorf("failed to unmarshal span data: %w", err)
		}

		tendermintSpan = &result
		return nil
	})

	return tendermintSpan, err
}

// FetchCheckpoint는 지정된 번호의 체크포인트를 조회합니다.
func (c *TendermintABCIClient) FetchCheckpoint(ctx context.Context, number int64) (*checkpoint.Checkpoint, error) {
	var checkpointResult *checkpoint.Checkpoint

	err := c.retryWithContext(ctx, "FetchCheckpoint", func() error {
		c.rpcMutex.RLock()
		defer c.rpcMutex.RUnlock()

		if !c.isRPCConnected {
			return ErrRPCNotConnected
		}

		// Tendermint ABCI Query를 사용하여 체크포인트 조회
		path := fmt.Sprintf("checkpoint/%d", number)
		queryResult, err := c.rpcClient.ABCIQuery(ctx, path, nil)
		if err != nil {
			return fmt.Errorf("failed to query checkpoint: %w", err)
		}

		if queryResult.Response.Code != 0 {
			return fmt.Errorf("query failed with code %d: %s",
				queryResult.Response.Code, queryResult.Response.Log)
		}

		// 응답 데이터가 없는 경우
		if len(queryResult.Response.Value) == 0 {
			return fmt.Errorf("no checkpoint found for number %d", number)
		}

		// 응답 데이터를 Checkpoint 응답으로 언마샬
		var checkpointResp checkpoint.CheckpointResponse
		if err := json.Unmarshal(queryResult.Response.Value, &checkpointResp); err != nil {
			return fmt.Errorf("failed to unmarshal checkpoint data: %w", err)
		}

		checkpointResult = &checkpointResp.Result
		return nil
	})

	return checkpointResult, err
}

// FetchCheckpointCount는 체크포인트의 총 개수를 조회합니다.
func (c *TendermintABCIClient) FetchCheckpointCount(ctx context.Context) (int64, error) {
	var countResult int64

	err := c.retryWithContext(ctx, "FetchCheckpointCount", func() error {
		c.rpcMutex.RLock()
		defer c.rpcMutex.RUnlock()

		if !c.isRPCConnected {
			return ErrRPCNotConnected
		}

		// Tendermint ABCI Query를 사용하여 체크포인트 개수 조회
		path := "checkpoint/count"
		queryResult, err := c.rpcClient.ABCIQuery(ctx, path, nil)
		if err != nil {
			return fmt.Errorf("failed to query checkpoint count: %w", err)
		}

		if queryResult.Response.Code != 0 {
			return fmt.Errorf("query failed with code %d: %s",
				queryResult.Response.Code, queryResult.Response.Log)
		}

		// 응답 데이터가 없는 경우
		if len(queryResult.Response.Value) == 0 {
			return fmt.Errorf("checkpoint count not found")
		}

		// 응답 데이터를 CheckpointCount 응답으로 언마샬
		var countResp checkpoint.CheckpointCountResponse
		if err := json.Unmarshal(queryResult.Response.Value, &countResp); err != nil {
			return fmt.Errorf("failed to unmarshal checkpoint count data: %w", err)
		}

		countResult = countResp.Result.Result
		return nil
	})

	return countResult, err
}

// FetchMilestone는 최신 마일스톤을 조회합니다.
func (c *TendermintABCIClient) FetchMilestone(ctx context.Context) (*milestone.Milestone, error) {
	var milestoneResult *milestone.Milestone

	err := c.retryWithContext(ctx, "FetchMilestone", func() error {
		c.rpcMutex.RLock()
		defer c.rpcMutex.RUnlock()

		if !c.isRPCConnected {
			return ErrRPCNotConnected
		}

		// Tendermint ABCI Query를 사용하여 최신 마일스톤 조회
		path := "milestone/latest"
		queryResult, err := c.rpcClient.ABCIQuery(ctx, path, nil)
		if err != nil {
			return fmt.Errorf("failed to query latest milestone: %w", err)
		}

		if queryResult.Response.Code != 0 {
			return fmt.Errorf("query failed with code %d: %s",
				queryResult.Response.Code, queryResult.Response.Log)
		}

		// 응답 데이터가 없는 경우
		if len(queryResult.Response.Value) == 0 {
			return fmt.Errorf("latest milestone not found")
		}

		// 응답 데이터를 Milestone 응답으로 언마샬
		var milestoneResp milestone.MilestoneResponse
		if err := json.Unmarshal(queryResult.Response.Value, &milestoneResp); err != nil {
			return fmt.Errorf("failed to unmarshal milestone data: %w", err)
		}

		milestoneResult = &milestoneResp.Result
		return nil
	})

	return milestoneResult, err
}

// FetchMilestoneCount는 마일스톤의 총 개수를 조회합니다.
func (c *TendermintABCIClient) FetchMilestoneCount(ctx context.Context) (int64, error) {
	var countResult int64

	err := c.retryWithContext(ctx, "FetchMilestoneCount", func() error {
		c.rpcMutex.RLock()
		defer c.rpcMutex.RUnlock()

		if !c.isRPCConnected {
			return ErrRPCNotConnected
		}

		// Tendermint ABCI Query를 사용하여 마일스톤 개수 조회
		path := "milestone/count"
		queryResult, err := c.rpcClient.ABCIQuery(ctx, path, nil)
		if err != nil {
			return fmt.Errorf("failed to query milestone count: %w", err)
		}

		if queryResult.Response.Code != 0 {
			return fmt.Errorf("query failed with code %d: %s",
				queryResult.Response.Code, queryResult.Response.Log)
		}

		// 응답 데이터가 없는 경우
		if len(queryResult.Response.Value) == 0 {
			return fmt.Errorf("milestone count not found")
		}

		// 응답 데이터를 MilestoneCount 응답으로 언마샬
		var countResp milestone.MilestoneCountResponse
		if err := json.Unmarshal(queryResult.Response.Value, &countResp); err != nil {
			return fmt.Errorf("failed to unmarshal milestone count data: %w", err)
		}

		countResult = countResp.Result.Count
		return nil
	})

	return countResult, err
}

// FetchNoAckMilestone는 특정 ID의 마일스톤이 Tendermint에서 실패했는지 여부를 조회합니다.
func (c *TendermintABCIClient) FetchNoAckMilestone(ctx context.Context, milestoneID string) error {
	return c.retryWithContext(ctx, "FetchNoAckMilestone", func() error {
		c.rpcMutex.RLock()
		defer c.rpcMutex.RUnlock()

		if !c.isRPCConnected {
			return ErrRPCNotConnected
		}

		// Tendermint ABCI Query를 사용하여 마일스톤 무확인 상태 조회
		path := fmt.Sprintf("milestone/no-ack/%s", milestoneID)
		queryResult, err := c.rpcClient.ABCIQuery(ctx, path, nil)
		if err != nil {
			return fmt.Errorf("failed to query milestone no-ack: %w", err)
		}

		if queryResult.Response.Code != 0 {
			return fmt.Errorf("query failed with code %d: %s",
				queryResult.Response.Code, queryResult.Response.Log)
		}

		// 응답 데이터가 없는 경우
		if len(queryResult.Response.Value) == 0 {
			return fmt.Errorf("milestone no-ack information not found for ID %s", milestoneID)
		}

		// 응답 데이터를 MilestoneNoAck 응답으로 언마샬
		var noAckResp milestone.MilestoneNoAckResponse
		if err := json.Unmarshal(queryResult.Response.Value, &noAckResp); err != nil {
			return fmt.Errorf("failed to unmarshal milestone no-ack data: %w", err)
		}

		// 마일스톤이 실패한 경우 오류 반환
		if noAckResp.Result.Result {
			return fmt.Errorf("milestone %s failed in Tendermint", milestoneID)
		}

		return nil
	})
}

// FetchLastNoAckMilestone는 가장 최근에 실패한 마일스톤 ID를 조회합니다.
func (c *TendermintABCIClient) FetchLastNoAckMilestone(ctx context.Context) (string, error) {
	var lastNoAckID string

	err := c.retryWithContext(ctx, "FetchLastNoAckMilestone", func() error {
		c.rpcMutex.RLock()
		defer c.rpcMutex.RUnlock()

		if !c.isRPCConnected {
			return ErrRPCNotConnected
		}

		// Tendermint ABCI Query를 사용하여 마지막 무확인 마일스톤 조회
		path := "milestone/last-no-ack"
		queryResult, err := c.rpcClient.ABCIQuery(ctx, path, nil)
		if err != nil {
			return fmt.Errorf("failed to query last no-ack milestone: %w", err)
		}

		if queryResult.Response.Code != 0 {
			return fmt.Errorf("query failed with code %d: %s",
				queryResult.Response.Code, queryResult.Response.Log)
		}

		// 응답 데이터가 없는 경우
		if len(queryResult.Response.Value) == 0 {
			return fmt.Errorf("last no-ack milestone not found")
		}

		// 응답 데이터를 MilestoneLastNoAck 응답으로 언마샬
		var lastNoAckResp milestone.MilestoneLastNoAckResponse
		if err := json.Unmarshal(queryResult.Response.Value, &lastNoAckResp); err != nil {
			return fmt.Errorf("failed to unmarshal last no-ack milestone data: %w", err)
		}

		lastNoAckID = lastNoAckResp.Result.Result
		return nil
	})

	return lastNoAckID, err
}

// FetchMilestoneID는 특정 ID의 마일스톤이 Tendermint에서 처리 중인지 여부를 조회합니다.
func (c *TendermintABCIClient) FetchMilestoneID(ctx context.Context, milestoneID string) error {
	return c.retryWithContext(ctx, "FetchMilestoneID", func() error {
		c.rpcMutex.RLock()
		defer c.rpcMutex.RUnlock()

		if !c.isRPCConnected {
			return ErrRPCNotConnected
		}

		// Tendermint ABCI Query를 사용하여 마일스톤 ID 조회
		path := fmt.Sprintf("milestone/id/%s", milestoneID)
		queryResult, err := c.rpcClient.ABCIQuery(ctx, path, nil)
		if err != nil {
			return fmt.Errorf("failed to query milestone ID: %w", err)
		}

		if queryResult.Response.Code != 0 {
			return fmt.Errorf("query failed with code %d: %s",
				queryResult.Response.Code, queryResult.Response.Log)
		}

		// 응답 데이터가 없는 경우
		if len(queryResult.Response.Value) == 0 {
			return fmt.Errorf("milestone ID information not found for ID %s", milestoneID)
		}

		// 응답 데이터를 MilestoneID 응답으로 언마샬
		var idResp milestone.MilestoneIDResponse
		if err := json.Unmarshal(queryResult.Response.Value, &idResp); err != nil {
			return fmt.Errorf("failed to unmarshal milestone ID data: %w", err)
		}

		// 마일스톤이 처리 중이 아닌 경우 오류 반환
		if !idResp.Result.Result {
			return fmt.Errorf("milestone %s is not in process in Tendermint", milestoneID)
		}

		return nil
	})
}

// Close 메서드 - 클라이언트 연결 종료
func (c *TendermintABCIClient) Close() {
	c.abciMutex.Lock()
	if c.isABCIConnected && c.abciClient != nil {
		log.Info("Closing Tendermint ABCI client")
		c.abciClient.Stop()
		c.isABCIConnected = false
	}
	c.abciMutex.Unlock()

	c.rpcMutex.Lock()
	if c.isRPCConnected && c.rpcClient != nil {
		log.Info("Closing Tendermint RPC client")
		c.rpcClient.Stop()
		c.isRPCConnected = false
	}
	c.rpcMutex.Unlock()

	close(c.closeCh)
}

// reconnectLoop is a background goroutine that handles automatic reconnection
func (c *TendermintABCIClient) reconnectLoop() {
	ticker := time.NewTicker(c.reconnectInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.closeCh:
			return
		case <-c.reconnectCh:
			c.attemptReconnect()
		case <-ticker.C:
			// 주기적으로 연결 상태 확인 및 필요시 재연결
			if !c.IsConnected() {
				c.attemptReconnect()
			}
		}
	}
}

// attemptReconnect tries to reconnect to both ABCI and RPC endpoints
func (c *TendermintABCIClient) attemptReconnect() {
	log.Info("Attempting to reconnect to Tendermint endpoints")

	c.abciMutex.RLock()
	abciConnected := c.isABCIConnected
	c.abciMutex.RUnlock()

	if !abciConnected {
		if err := c.connectABCI(); err != nil {
			log.Error("Failed to reconnect to ABCI endpoint", "error", err)
		} else {
			log.Info("Successfully reconnected to ABCI endpoint")
		}
	}

	c.rpcMutex.RLock()
	rpcConnected := c.isRPCConnected
	c.rpcMutex.RUnlock()

	if !rpcConnected {
		if err := c.connectRPC(); err != nil {
			log.Error("Failed to reconnect to RPC endpoint", "error", err)
		} else {
			log.Info("Successfully reconnected to RPC endpoint")
		}
	}
}

// triggerReconnect triggers a reconnection attempt
func (c *TendermintABCIClient) triggerReconnect() {
	select {
	case c.reconnectCh <- struct{}{}:
		// 재연결 시그널 전송 성공
	default:
		// 채널이 이미 가득 찬 경우 (이미 재연결 시그널이 대기 중)
	}
}
