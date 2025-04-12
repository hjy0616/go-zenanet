package tendermint

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
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
	// ErrContextCanceled is returned when the context is canceled during an operation
	ErrContextCanceled = errors.New("context canceled during operation")
	// ErrContextDeadlineExceeded is returned when the context deadline is exceeded
	ErrContextDeadlineExceeded = errors.New("context deadline exceeded during operation")
)

const (
	// Connection timeout
	connectionTimeout = 10 * time.Second
	// Dial retry delay
	dialRetryDelay = 3 * time.Second
	// Maximum retry attempts for connection
	maxRetryAttempts = 5
	// Default query timeout
	defaultQueryTimeout = 5 * time.Second
	// Reconnect check interval
	reconnectInterval = 10 * time.Second
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

	closeCh chan struct{} // 종료 채널

	reconnectCtx    context.Context    // 재연결 컨텍스트
	reconnectCancel context.CancelFunc // 재연결 취소 함수
}

// NewTendermintABCIClient creates a new TendermintABCIClient with the given ABCI and RPC addresses
func NewTendermintABCIClient(abciAddr, rpcAddr string) *TendermintABCIClient {
	reconnectCtx, reconnectCancel := context.WithCancel(context.Background())

	return &TendermintABCIClient{
		abciAddr:        abciAddr,
		rpcAddr:         rpcAddr,
		closeCh:         make(chan struct{}),
		reconnectCtx:    reconnectCtx,
		reconnectCancel: reconnectCancel,
	}
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

	// 연결 상태를 주기적으로 확인하고 필요한 경우 재연결하는 고루틴 시작
	go c.monitorConnections()

	return nil
}

// monitorConnections는 연결 상태를 주기적으로 확인하고 필요한 경우 재연결을 시도합니다.
func (c *TendermintABCIClient) monitorConnections() {
	ticker := time.NewTicker(reconnectInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.checkAndReconnect()
		case <-c.reconnectCtx.Done():
			return
		case <-c.closeCh:
			return
		}
	}
}

// checkAndReconnect는 연결 상태를 확인하고 필요한 경우 재연결을 시도합니다.
func (c *TendermintABCIClient) checkAndReconnect() {
	// ABCI 연결 확인 및 재연결
	c.abciMutex.RLock()
	abciConnected := c.isABCIConnected && c.abciClient != nil && c.abciClient.IsRunning()
	c.abciMutex.RUnlock()

	if !abciConnected {
		log.Warn("ABCI connection is down, attempting to reconnect")
		if err := c.reconnectABCI(); err != nil {
			log.Error("Failed to reconnect to ABCI endpoint", "error", err)
		}
	}

	// RPC 연결 확인 및 재연결
	c.rpcMutex.RLock()
	rpcConnected := c.isRPCConnected && c.rpcClient != nil
	c.rpcMutex.RUnlock()

	// RPC 클라이언트는 IsRunning 등의 상태 확인 메서드가 없으므로
	// 간단한 Status 요청을 보내 연결 상태를 확인합니다
	if rpcConnected {
		ctx, cancel := context.WithTimeout(context.Background(), defaultQueryTimeout)
		defer cancel()

		_, err := c.rpcClient.Status(ctx)
		if err != nil {
			rpcConnected = false
		}
	}

	if !rpcConnected {
		log.Warn("RPC connection is down, attempting to reconnect")
		if err := c.reconnectRPC(); err != nil {
			log.Error("Failed to reconnect to RPC endpoint", "error", err)
		}
	}
}

// reconnectABCI는 ABCI 연결을 재시도합니다.
func (c *TendermintABCIClient) reconnectABCI() error {
	c.abciMutex.Lock()
	defer c.abciMutex.Unlock()

	// 이미 연결되어 있는 경우 먼저 연결 종료
	if c.abciClient != nil {
		if c.abciClient.IsRunning() {
			c.abciClient.Stop()
		}
		c.abciClient = nil
	}
	c.isABCIConnected = false

	// 최대 시도 횟수 동안 연결 시도
	var lastErr error
	for attempt := 0; attempt < maxRetryAttempts; attempt++ {
		// 첫 번째 시도가 아닌 경우 지연 시간 추가
		if attempt > 0 {
			select {
			case <-time.After(dialRetryDelay):
			case <-c.reconnectCtx.Done():
				return fmt.Errorf("reconnect context canceled")
			case <-c.closeCh:
				return fmt.Errorf("client closing")
			}
		}

		// 로컬 TCP 소켓을 통해 ABCI 클라이언트 생성
		client := abciclient.NewSocketClient(c.abciAddr, true)

		// Start 메서드는 연결 시도를 수행
		if err := client.Start(); err != nil {
			lastErr = err
			log.Warn("Failed to reconnect to ABCI endpoint, retrying", "attempt", attempt+1, "error", err)
			continue
		}

		// 연결이 성공적으로 이루어졌는지 확인
		if !client.IsRunning() {
			lastErr = errors.New("ABCI client failed to start")
			log.Warn("ABCI client failed to start, retrying", "attempt", attempt+1)
			continue
		}

		c.abciClient = client
		c.isABCIConnected = true
		log.Info("Successfully reconnected to Tendermint ABCI endpoint", "address", c.abciAddr, "attempt", attempt+1)
		return nil
	}

	return fmt.Errorf("failed to reconnect to ABCI endpoint after %d attempts: %w", maxRetryAttempts, lastErr)
}

// reconnectRPC는 RPC 연결을 재시도합니다.
func (c *TendermintABCIClient) reconnectRPC() error {
	c.rpcMutex.Lock()
	defer c.rpcMutex.Unlock()

	// 이미 연결되어 있는 경우 먼저 연결 종료
	if c.rpcClient != nil {
		c.rpcClient.Stop()
		c.rpcClient = nil
	}
	c.isRPCConnected = false

	// 최대 시도 횟수 동안 연결 시도
	var lastErr error
	for attempt := 0; attempt < maxRetryAttempts; attempt++ {
		// 첫 번째 시도가 아닌 경우 지연 시간 추가
		if attempt > 0 {
			select {
			case <-time.After(dialRetryDelay):
			case <-c.reconnectCtx.Done():
				return fmt.Errorf("reconnect context canceled")
			case <-c.closeCh:
				return fmt.Errorf("client closing")
			}
		}

		// HTTP를 통한 RPC 클라이언트 생성
		client, err := tmrpc.New(c.rpcAddr, "/websocket")
		if err != nil {
			lastErr = err
			log.Warn("Failed to create RPC client, retrying", "attempt", attempt+1, "error", err)
			continue
		}

		if err := client.Start(); err != nil {
			lastErr = err
			log.Warn("Failed to start RPC client, retrying", "attempt", attempt+1, "error", err)
			continue
		}

		// 연결 상태 확인을 위한 간단한 Status 요청
		ctx, cancel := context.WithTimeout(context.Background(), defaultQueryTimeout)
		_, statusErr := client.Status(ctx)
		cancel()

		if statusErr != nil {
			lastErr = statusErr
			log.Warn("RPC client connected but status check failed, retrying", "attempt", attempt+1, "error", statusErr)
			client.Stop()
			continue
		}

		c.rpcClient = client
		c.isRPCConnected = true
		log.Info("Successfully reconnected to Tendermint RPC endpoint", "address", c.rpcAddr, "attempt", attempt+1)
		return nil
	}

	return fmt.Errorf("failed to reconnect to RPC endpoint after %d attempts: %w", maxRetryAttempts, lastErr)
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

	// Context와 함께 연결 시도
	ctx, cancel := context.WithTimeout(context.Background(), connectionTimeout)
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

	// Context와 함께 연결 시도
	ctx, cancel := context.WithTimeout(context.Background(), connectionTimeout)
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

// InitChain implements the ITendermintClient interface
func (c *TendermintABCIClient) InitChain(ctx context.Context, req types.RequestInitChain) (*types.ResponseInitChain, error) {
	// 컨텍스트가 이미 취소되었는지 확인
	if ctx.Err() != nil {
		return nil, c.handleContextError(ctx.Err())
	}

	c.abciMutex.RLock()
	defer c.abciMutex.RUnlock()

	if !c.isABCIConnected || c.abciClient == nil {
		return nil, ErrABCINotConnected
	}

	// InitChainSync 호출
	resp := c.abciClient.InitChainSync(req)
	if resp.Error != nil {
		return nil, resp.Error
	}

	// 컨텍스트 취소 확인
	if ctx.Err() != nil {
		return nil, c.handleContextError(ctx.Err())
	}

	return &resp.Response, nil
}

// BeginBlock implements the ITendermintClient interface
func (c *TendermintABCIClient) BeginBlock(ctx context.Context, req types.RequestBeginBlock) (*types.ResponseBeginBlock, error) {
	// 컨텍스트가 이미 취소되었는지 확인
	if ctx.Err() != nil {
		return nil, c.handleContextError(ctx.Err())
	}

	c.abciMutex.RLock()
	defer c.abciMutex.RUnlock()

	if !c.isABCIConnected || c.abciClient == nil {
		return nil, ErrABCINotConnected
	}

	// BeginBlockSync 호출
	resp := c.abciClient.BeginBlockSync(req)
	if resp.Error != nil {
		return nil, resp.Error
	}

	// 컨텍스트 취소 확인
	if ctx.Err() != nil {
		return nil, c.handleContextError(ctx.Err())
	}

	return &resp.Response, nil
}

// CheckTx implements the ITendermintClient interface
func (c *TendermintABCIClient) CheckTx(ctx context.Context, req types.RequestCheckTx) (*types.ResponseCheckTx, error) {
	c.abciMutex.RLock()
	defer c.abciMutex.RUnlock()

	if !c.isABCIConnected {
		return nil, ErrABCINotConnected
	}

	resp := c.abciClient.CheckTxSync(req)
	if resp.Error != nil {
		return nil, resp.Error
	}

	return &resp.Response, nil
}

// DeliverTx implements the ITendermintClient interface
func (c *TendermintABCIClient) DeliverTx(ctx context.Context, req types.RequestDeliverTx) (*types.ResponseDeliverTx, error) {
	c.abciMutex.RLock()
	defer c.abciMutex.RUnlock()

	if !c.isABCIConnected {
		return nil, ErrABCINotConnected
	}

	resp := c.abciClient.DeliverTxSync(req)
	if resp.Error != nil {
		return nil, resp.Error
	}

	return &resp.Response, nil
}

// EndBlock implements the ITendermintClient interface
func (c *TendermintABCIClient) EndBlock(ctx context.Context, req types.RequestEndBlock) (*types.ResponseEndBlock, error) {
	c.abciMutex.RLock()
	defer c.abciMutex.RUnlock()

	if !c.isABCIConnected {
		return nil, ErrABCINotConnected
	}

	resp := c.abciClient.EndBlockSync(req)
	if resp.Error != nil {
		return nil, resp.Error
	}

	return &resp.Response, nil
}

// Commit implements the ITendermintClient interface
func (c *TendermintABCIClient) Commit(ctx context.Context) (*types.ResponseCommit, error) {
	c.abciMutex.RLock()
	defer c.abciMutex.RUnlock()

	if !c.isABCIConnected {
		return nil, ErrABCINotConnected
	}

	resp := c.abciClient.CommitSync()
	if resp.Error != nil {
		return nil, resp.Error
	}

	return &resp.Response, nil
}

// GetValidators implements the ITendermintClient interface
func (c *TendermintABCIClient) GetValidators(ctx context.Context) ([]*valset.Validator, error) {
	// 컨텍스트가 이미 취소되었는지 확인
	if ctx.Err() != nil {
		return nil, c.handleContextError(ctx.Err())
	}

	c.rpcMutex.RLock()
	defer c.rpcMutex.RUnlock()

	if !c.isRPCConnected || c.rpcClient == nil {
		return nil, ErrRPCNotConnected
	}

	// 기본 타임아웃 컨텍스트가 없으면 추가
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultQueryTimeout)
		defer cancel()
	}

	// Tendermint RPC를 통해 검증자 목록 가져오기
	validatorsResp, err := c.rpcClient.Validators(ctx, nil, nil, nil)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, c.handleContextError(ctxErr)
		}
		return nil, fmt.Errorf("failed to fetch validators: %w", err)
	}

	// 컨텍스트 취소 확인
	if ctx.Err() != nil {
		return nil, c.handleContextError(ctx.Err())
	}

	// 검증자 목록이 없는 경우
	if validatorsResp == nil || len(validatorsResp.Validators) == 0 {
		log.Warn("No validators returned from Tendermint")
		return []*valset.Validator{}, nil
	}

	validators := make([]*valset.Validator, 0, len(validatorsResp.Validators))
	for _, v := range validatorsResp.Validators {
		// Tendermint 주소를 Ethereum 주소로 변환
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
		idBytes := make([]byte, 8)
		copy(idBytes, ethAddr.Bytes()[:8])
		validator.ID = binary.BigEndian.Uint64(idBytes)

		validators = append(validators, validator)
	}

	return validators, nil
}

// GetCurrentValidatorSet implements the ITendermintClient interface
func (c *TendermintABCIClient) GetCurrentValidatorSet(ctx context.Context) (*valset.ValidatorSet, error) {
	// 컨텍스트가 이미 취소되었는지 확인
	if ctx.Err() != nil {
		return nil, c.handleContextError(ctx.Err())
	}

	validators, err := c.GetValidators(ctx)
	if err != nil {
		return nil, err
	}

	// 검증자가 없는 경우
	if len(validators) == 0 {
		log.Warn("Creating empty validator set")
		return valset.NewValidatorSet([]*valset.Validator{}), nil
	}

	// 검증자 세트 생성
	validatorSet := valset.NewValidatorSet(validators)

	// 유효성 검사: 검증자 세트의 총 투표력이 0보다 큰지 확인
	if validatorSet.TotalVotingPower() <= 0 {
		log.Warn("Validator set has zero or negative total voting power", "power", validatorSet.TotalVotingPower())
	}

	return validatorSet, nil
}

// StateSyncEvents는 특정 ID부터 지정된 블록까지의 상태 동기화 이벤트를 검색합니다.
func (c *TendermintABCIClient) StateSyncEvents(ctx context.Context, fromID uint64, to int64) ([]*clerk.EventRecordWithTime, error) {
	// 컨텍스트가 이미 취소되었는지 확인
	if ctx.Err() != nil {
		return nil, c.handleContextError(ctx.Err())
	}

	c.rpcMutex.RLock()
	defer c.rpcMutex.RUnlock()

	if !c.isRPCConnected || c.rpcClient == nil {
		return nil, ErrRPCNotConnected
	}

	// 기본 타임아웃 컨텍스트가 없으면 추가
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultQueryTimeout)
		defer cancel()
	}

	// Tendermint RPC를 통해 이벤트 조회
	// 여기서는 tx_search를 사용하여 특정 타입의 이벤트를 조회합니다
	query := fmt.Sprintf("state_sync.from_id >= %d AND state_sync.to_block <= %d", fromID, to)

	searchResult, err := c.rpcClient.TxSearch(ctx, query, false, nil, nil, "asc")
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, c.handleContextError(ctxErr)
		}
		return nil, fmt.Errorf("failed to search state sync events: %w", err)
	}

	// 결과를 EventRecordWithTime 형태로 변환
	events := make([]*clerk.EventRecordWithTime, 0, len(searchResult.Txs))
	for _, tx := range searchResult.Txs {
		// 컨텍스트 취소 확인
		if ctx.Err() != nil {
			return nil, c.handleContextError(ctx.Err())
		}

		// 각 트랜잭션에서 이벤트 데이터 추출
		for _, event := range tx.TxResult.Events {
			if event.Type != "state_sync" {
				continue
			}

			// 이벤트 속성에서 필요한 데이터 추출
			var id uint64
			var stateID uint64
			var recordTime time.Time
			var data []byte

			for _, attr := range event.Attributes {
				key := string(attr.Key)
				value := attr.Value

				switch key {
				case "id":
					parsedID, err := strconv.ParseUint(string(value), 10, 64)
					if err != nil {
						log.Warn("Failed to parse state sync event ID", "value", string(value), "error", err)
						continue
					}
					id = parsedID
				case "state_id":
					parsedStateID, err := strconv.ParseUint(string(value), 10, 64)
					if err != nil {
						log.Warn("Failed to parse state sync state ID", "value", string(value), "error", err)
						continue
					}
					stateID = parsedStateID
				case "time":
					timeInt, err := strconv.ParseInt(string(value), 10, 64)
					if err != nil {
						log.Warn("Failed to parse state sync time", "value", string(value), "error", err)
						continue
					}
					recordTime = time.Unix(timeInt, 0)
				case "data":
					data = value
				}
			}

			// 필수 필드가 모두 있는지 확인
			if id == 0 || stateID == 0 || recordTime.IsZero() || len(data) == 0 {
				log.Warn("Incomplete state sync event data", "id", id, "stateID", stateID, "time", recordTime)
				continue
			}

			// EventRecordWithTime 생성 및 추가
			record := &clerk.EventRecord{
				ID:      id,
				StateID: stateID,
				Data:    data,
			}
			eventWithTime := &clerk.EventRecordWithTime{
				EventRecord: record,
				RecordTime:  recordTime,
			}
			events = append(events, eventWithTime)
		}
	}

	return events, nil
}

// handleContextError는 컨텍스트 오류를 처리하고 적절한 오류를 반환합니다.
func (c *TendermintABCIClient) handleContextError(err error) error {
	if err == context.Canceled {
		return ErrContextCanceled
	}
	if err == context.DeadlineExceeded {
		return ErrContextDeadlineExceeded
	}
	return err
}

// Span는 지정된 spanID에 해당하는 Tendermint 스팬 정보를 가져옵니다.
func (c *TendermintABCIClient) Span(ctx context.Context, spanID uint64) (*span.TendermintSpan, error) {
	// 컨텍스트가 이미 취소되었는지 확인
	if ctx.Err() != nil {
		return nil, c.handleContextError(ctx.Err())
	}

	c.rpcMutex.RLock()
	defer c.rpcMutex.RUnlock()

	if !c.isRPCConnected || c.rpcClient == nil {
		return nil, ErrRPCNotConnected
	}

	// 기본 타임아웃 컨텍스트가 없으면 추가
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultQueryTimeout)
		defer cancel()
	}

	// Tendermint ABCI Query를 사용하여 스팬 정보 조회
	path := fmt.Sprintf("span/%d", spanID)
	queryResult, err := c.rpcClient.ABCIQuery(ctx, path, nil)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, c.handleContextError(ctxErr)
		}
		return nil, fmt.Errorf("failed to query span: %w", err)
	}

	// 응답 코드 확인
	if queryResult.Response.Code != 0 {
		return nil, fmt.Errorf("query failed with code %d: %s",
			queryResult.Response.Code, queryResult.Response.Log)
	}

	// 응답 데이터가 없는 경우
	if len(queryResult.Response.Value) == 0 {
		return nil, fmt.Errorf("no span found for ID %d", spanID)
	}

	// 응답 데이터를 TendermintSpan으로 언마샬
	var tendermintSpan span.TendermintSpan
	if err := json.Unmarshal(queryResult.Response.Value, &tendermintSpan); err != nil {
		return nil, fmt.Errorf("failed to unmarshal span data: %w", err)
	}

	// 스팬 데이터 유효성 검사
	if tendermintSpan.ID != spanID {
		return nil, fmt.Errorf("received span ID (%d) doesn't match requested ID (%d)", tendermintSpan.ID, spanID)
	}

	// 스팬 구조체 필드 유효성 검사
	if tendermintSpan.StartBlock >= tendermintSpan.EndBlock {
		log.Warn("Invalid span block range", "spanID", spanID, "start", tendermintSpan.StartBlock, "end", tendermintSpan.EndBlock)
	}

	// 검증자 세트 유효성 검사
	if tendermintSpan.ValidatorSet.Size() == 0 {
		log.Warn("Empty validator set in span", "spanID", spanID)
	}

	return &tendermintSpan, nil
}

// FetchCheckpoint는 지정된 번호의 체크포인트를 조회합니다.
func (c *TendermintABCIClient) FetchCheckpoint(ctx context.Context, number int64) (*checkpoint.Checkpoint, error) {
	c.rpcMutex.RLock()
	defer c.rpcMutex.RUnlock()

	if !c.isRPCConnected {
		return nil, ErrRPCNotConnected
	}

	// Tendermint ABCI Query를 사용하여 체크포인트 조회
	path := fmt.Sprintf("checkpoint/%d", number)
	queryResult, err := c.rpcClient.ABCIQuery(ctx, path, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to query checkpoint: %w", err)
	}

	if queryResult.Response.Code != 0 {
		return nil, fmt.Errorf("query failed with code %d: %s",
			queryResult.Response.Code, queryResult.Response.Log)
	}

	// 응답 데이터가 없는 경우
	if len(queryResult.Response.Value) == 0 {
		return nil, fmt.Errorf("no checkpoint found for number %d", number)
	}

	// 응답 데이터를 Checkpoint 응답으로 언마샬
	var checkpointResp checkpoint.CheckpointResponse
	if err := json.Unmarshal(queryResult.Response.Value, &checkpointResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal checkpoint data: %w", err)
	}

	return &checkpointResp.Result, nil
}

// FetchCheckpointCount는 체크포인트의 총 개수를 조회합니다.
func (c *TendermintABCIClient) FetchCheckpointCount(ctx context.Context) (int64, error) {
	c.rpcMutex.RLock()
	defer c.rpcMutex.RUnlock()

	if !c.isRPCConnected {
		return 0, ErrRPCNotConnected
	}

	// Tendermint ABCI Query를 사용하여 체크포인트 개수 조회
	path := "checkpoint/count"
	queryResult, err := c.rpcClient.ABCIQuery(ctx, path, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to query checkpoint count: %w", err)
	}

	if queryResult.Response.Code != 0 {
		return 0, fmt.Errorf("query failed with code %d: %s",
			queryResult.Response.Code, queryResult.Response.Log)
	}

	// 응답 데이터가 없는 경우
	if len(queryResult.Response.Value) == 0 {
		return 0, fmt.Errorf("checkpoint count not found")
	}

	// 응답 데이터를 CheckpointCount 응답으로 언마샬
	var countResp checkpoint.CheckpointCountResponse
	if err := json.Unmarshal(queryResult.Response.Value, &countResp); err != nil {
		return 0, fmt.Errorf("failed to unmarshal checkpoint count data: %w", err)
	}

	return countResp.Result.Result, nil
}

// FetchMilestone는 최신 마일스톤을 조회합니다.
func (c *TendermintABCIClient) FetchMilestone(ctx context.Context) (*milestone.Milestone, error) {
	c.rpcMutex.RLock()
	defer c.rpcMutex.RUnlock()

	if !c.isRPCConnected {
		return nil, ErrRPCNotConnected
	}

	// Tendermint ABCI Query를 사용하여 최신 마일스톤 조회
	path := "milestone/latest"
	queryResult, err := c.rpcClient.ABCIQuery(ctx, path, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to query latest milestone: %w", err)
	}

	if queryResult.Response.Code != 0 {
		return nil, fmt.Errorf("query failed with code %d: %s",
			queryResult.Response.Code, queryResult.Response.Log)
	}

	// 응답 데이터가 없는 경우
	if len(queryResult.Response.Value) == 0 {
		return nil, fmt.Errorf("latest milestone not found")
	}

	// 응답 데이터를 Milestone 응답으로 언마샬
	var milestoneResp milestone.MilestoneResponse
	if err := json.Unmarshal(queryResult.Response.Value, &milestoneResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal milestone data: %w", err)
	}

	return &milestoneResp.Result, nil
}

// FetchMilestoneCount는 마일스톤의 총 개수를 조회합니다.
func (c *TendermintABCIClient) FetchMilestoneCount(ctx context.Context) (int64, error) {
	c.rpcMutex.RLock()
	defer c.rpcMutex.RUnlock()

	if !c.isRPCConnected {
		return 0, ErrRPCNotConnected
	}

	// Tendermint ABCI Query를 사용하여 마일스톤 개수 조회
	path := "milestone/count"
	queryResult, err := c.rpcClient.ABCIQuery(ctx, path, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to query milestone count: %w", err)
	}

	if queryResult.Response.Code != 0 {
		return 0, fmt.Errorf("query failed with code %d: %s",
			queryResult.Response.Code, queryResult.Response.Log)
	}

	// 응답 데이터가 없는 경우
	if len(queryResult.Response.Value) == 0 {
		return 0, fmt.Errorf("milestone count not found")
	}

	// 응답 데이터를 MilestoneCount 응답으로 언마샬
	var countResp milestone.MilestoneCountResponse
	if err := json.Unmarshal(queryResult.Response.Value, &countResp); err != nil {
		return 0, fmt.Errorf("failed to unmarshal milestone count data: %w", err)
	}

	return countResp.Result.Count, nil
}

// FetchNoAckMilestone는 특정 ID의 마일스톤이 Tendermint에서 실패했는지 여부를 조회합니다.
func (c *TendermintABCIClient) FetchNoAckMilestone(ctx context.Context, milestoneID string) error {
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
}

// FetchLastNoAckMilestone는 가장 최근에 실패한 마일스톤 ID를 조회합니다.
func (c *TendermintABCIClient) FetchLastNoAckMilestone(ctx context.Context) (string, error) {
	c.rpcMutex.RLock()
	defer c.rpcMutex.RUnlock()

	if !c.isRPCConnected {
		return "", ErrRPCNotConnected
	}

	// Tendermint ABCI Query를 사용하여 마지막 무확인 마일스톤 조회
	path := "milestone/last-no-ack"
	queryResult, err := c.rpcClient.ABCIQuery(ctx, path, nil)
	if err != nil {
		return "", fmt.Errorf("failed to query last no-ack milestone: %w", err)
	}

	if queryResult.Response.Code != 0 {
		return "", fmt.Errorf("query failed with code %d: %s",
			queryResult.Response.Code, queryResult.Response.Log)
	}

	// 응답 데이터가 없는 경우
	if len(queryResult.Response.Value) == 0 {
		return "", fmt.Errorf("last no-ack milestone not found")
	}

	// 응답 데이터를 MilestoneLastNoAck 응답으로 언마샬
	var lastNoAckResp milestone.MilestoneLastNoAckResponse
	if err := json.Unmarshal(queryResult.Response.Value, &lastNoAckResp); err != nil {
		return "", fmt.Errorf("failed to unmarshal last no-ack milestone data: %w", err)
	}

	return lastNoAckResp.Result.Result, nil
}

// FetchMilestoneID는 특정 ID의 마일스톤이 Tendermint에서 처리 중인지 여부를 조회합니다.
func (c *TendermintABCIClient) FetchMilestoneID(ctx context.Context, milestoneID string) error {
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
}

// Close 메서드 - 클라이언트 연결 종료
func (c *TendermintABCIClient) Close() {
	// 재연결 고루틴 취소
	if c.reconnectCancel != nil {
		c.reconnectCancel()
	}

	// ABCI 클라이언트 종료
	c.abciMutex.Lock()
	if c.isABCIConnected && c.abciClient != nil {
		log.Info("Closing Tendermint ABCI client")
		c.abciClient.Stop()
		c.isABCIConnected = false
		c.abciClient = nil
	}
	c.abciMutex.Unlock()

	// RPC 클라이언트 종료
	c.rpcMutex.Lock()
	if c.isRPCConnected && c.rpcClient != nil {
		log.Info("Closing Tendermint RPC client")
		c.rpcClient.Stop()
		c.isRPCConnected = false
		c.rpcClient = nil
	}
	c.rpcMutex.Unlock()

	// 중복 종료 방지를 위한 select 패턴 사용
	select {
	case <-c.closeCh:
		// 이미 닫혀 있음
	default:
		close(c.closeCh)
	}

	log.Info("Tendermint ABCI client closed successfully")
}
