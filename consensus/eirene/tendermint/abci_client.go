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
)

const (
	// Connection timeout
	connectionTimeout = 10 * time.Second
	// Dial retry delay
	dialRetryDelay = 3 * time.Second
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
}

// NewTendermintABCIClient creates a new TendermintABCIClient with the given ABCI and RPC addresses
func NewTendermintABCIClient(abciAddr, rpcAddr string) *TendermintABCIClient {
	return &TendermintABCIClient{
		abciAddr: abciAddr,
		rpcAddr:  rpcAddr,
		closeCh:  make(chan struct{}),
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

	// Context와 함께 연결 시도
	ctx, cancel := context.WithTimeout(context.Background(), connectionTimeout)
	defer cancel()

	// 연결 시도 고루틴 생성
	errCh := make(chan error, 1)
	go func() {
		// Start 메서드는 연결 시도를 수행
		if err := client.Start(); err != nil {
			errCh <- fmt.Errorf("error starting ABCI client: %w", err)
			return
		}

		// 연결이 성공적으로 이루어졌는지 확인
		if !client.IsRunning() {
			errCh <- errors.New("ABCI client failed to start")
			return
		}

		errCh <- nil
	}()

	// 타임아웃 또는 결과 대기
	select {
	case <-ctx.Done():
		// 연결 시도 취소 및 정리
		if client.IsRunning() {
			client.Stop()
		}
		return fmt.Errorf("ABCI connection timeout: %w", ctx.Err())
	case err := <-errCh:
		if err != nil {
			return err
		}
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

	// 연결 시도 고루틴 생성
	errCh := make(chan error, 1)
	go func() {
		if err := client.Start(); err != nil {
			errCh <- fmt.Errorf("error starting RPC client: %w", err)
			return
		}
		errCh <- nil
	}()

	// 타임아웃 또는 결과 대기
	select {
	case <-ctx.Done():
		// 연결 시도 취소 및 정리
		if client.IsRunning() {
			client.Stop()
		}
		return fmt.Errorf("RPC connection timeout: %w", ctx.Err())
	case err := <-errCh:
		if err != nil {
			return err
		}
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
	c.abciMutex.RLock()
	isConnected := c.isABCIConnected
	c.abciMutex.RUnlock()

	if !isConnected {
		// 자동 재연결 시도
		if err := c.ReconnectIfNeeded(); err != nil {
			return nil, fmt.Errorf("failed to reconnect: %w", err)
		}

		// 재연결 후에도 연결 상태 확인
		c.abciMutex.RLock()
		isConnected = c.isABCIConnected
		c.abciMutex.RUnlock()

		if !isConnected {
			return nil, ErrABCINotConnected
		}
	}

	c.abciMutex.RLock()
	defer c.abciMutex.RUnlock()

	// 최대 재시도 횟수
	maxRetries := 3
	var resp types.ResponseInitChain
	var lastErr error

	// 재시도 로직
	for i := 0; i < maxRetries; i++ {
		// 컨텍스트 만료 확인
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		result := c.abciClient.InitChainSync(req)
		if result.Error == nil {
			return &result.Response, nil
		}

		lastErr = result.Error
		log.Warn("InitChain request failed, retrying", "error", lastErr, "retry", i+1)
		time.Sleep(dialRetryDelay)
	}

	return nil, fmt.Errorf("failed to execute InitChain after %d retries: %w", maxRetries, lastErr)
}

// BeginBlock implements the ITendermintClient interface
func (c *TendermintABCIClient) BeginBlock(ctx context.Context, req types.RequestBeginBlock) (*types.ResponseBeginBlock, error) {
	c.abciMutex.RLock()
	isConnected := c.isABCIConnected
	c.abciMutex.RUnlock()

	if !isConnected {
		// 자동 재연결 시도
		if err := c.ReconnectIfNeeded(); err != nil {
			return nil, fmt.Errorf("failed to reconnect: %w", err)
		}

		// 재연결 후에도 연결 상태 확인
		c.abciMutex.RLock()
		isConnected = c.isABCIConnected
		c.abciMutex.RUnlock()

		if !isConnected {
			return nil, ErrABCINotConnected
		}
	}

	c.abciMutex.RLock()
	defer c.abciMutex.RUnlock()

	// 최대 재시도 횟수
	maxRetries := 3
	var lastErr error

	// 재시도 로직
	for i := 0; i < maxRetries; i++ {
		// 컨텍스트 만료 확인
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		resp := c.abciClient.BeginBlockSync(req)
		if resp.Error == nil {
			return &resp.Response, nil
		}

		lastErr = resp.Error
		log.Warn("BeginBlock request failed, retrying", "error", lastErr, "retry", i+1)
		time.Sleep(dialRetryDelay)
	}

	return nil, fmt.Errorf("failed to execute BeginBlock after %d retries: %w", maxRetries, lastErr)
}

// CheckTx implements the ITendermintClient interface
func (c *TendermintABCIClient) CheckTx(ctx context.Context, req types.RequestCheckTx) (*types.ResponseCheckTx, error) {
	c.abciMutex.RLock()
	isConnected := c.isABCIConnected
	c.abciMutex.RUnlock()

	if !isConnected {
		// 자동 재연결 시도
		if err := c.ReconnectIfNeeded(); err != nil {
			return nil, fmt.Errorf("failed to reconnect: %w", err)
		}

		// 재연결 후에도 연결 상태 확인
		c.abciMutex.RLock()
		isConnected = c.isABCIConnected
		c.abciMutex.RUnlock()

		if !isConnected {
			return nil, ErrABCINotConnected
		}
	}

	c.abciMutex.RLock()
	defer c.abciMutex.RUnlock()

	// 최대 재시도 횟수
	maxRetries := 3
	var lastErr error

	// 재시도 로직
	for i := 0; i < maxRetries; i++ {
		// 컨텍스트 만료 확인
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		resp := c.abciClient.CheckTxSync(req)
		if resp.Error == nil {
			return &resp.Response, nil
		}

		lastErr = resp.Error
		log.Warn("CheckTx request failed, retrying", "error", lastErr, "retry", i+1)
		time.Sleep(dialRetryDelay)
	}

	return nil, fmt.Errorf("failed to execute CheckTx after %d retries: %w", maxRetries, lastErr)
}

// DeliverTx implements the ITendermintClient interface
func (c *TendermintABCIClient) DeliverTx(ctx context.Context, req types.RequestDeliverTx) (*types.ResponseDeliverTx, error) {
	c.abciMutex.RLock()
	isConnected := c.isABCIConnected
	c.abciMutex.RUnlock()

	if !isConnected {
		// 자동 재연결 시도
		if err := c.ReconnectIfNeeded(); err != nil {
			return nil, fmt.Errorf("failed to reconnect: %w", err)
		}

		// 재연결 후에도 연결 상태 확인
		c.abciMutex.RLock()
		isConnected = c.isABCIConnected
		c.abciMutex.RUnlock()

		if !isConnected {
			return nil, ErrABCINotConnected
		}
	}

	c.abciMutex.RLock()
	defer c.abciMutex.RUnlock()

	// 최대 재시도 횟수
	maxRetries := 3
	var lastErr error

	// 재시도 로직
	for i := 0; i < maxRetries; i++ {
		// 컨텍스트 만료 확인
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		resp := c.abciClient.DeliverTxSync(req)
		if resp.Error == nil {
			return &resp.Response, nil
		}

		lastErr = resp.Error
		log.Warn("DeliverTx request failed, retrying", "error", lastErr, "retry", i+1)
		time.Sleep(dialRetryDelay)
	}

	return nil, fmt.Errorf("failed to execute DeliverTx after %d retries: %w", maxRetries, lastErr)
}

// EndBlock implements the ITendermintClient interface
func (c *TendermintABCIClient) EndBlock(ctx context.Context, req types.RequestEndBlock) (*types.ResponseEndBlock, error) {
	c.abciMutex.RLock()
	isConnected := c.isABCIConnected
	c.abciMutex.RUnlock()

	if !isConnected {
		// 자동 재연결 시도
		if err := c.ReconnectIfNeeded(); err != nil {
			return nil, fmt.Errorf("failed to reconnect: %w", err)
		}

		// 재연결 후에도 연결 상태 확인
		c.abciMutex.RLock()
		isConnected = c.isABCIConnected
		c.abciMutex.RUnlock()

		if !isConnected {
			return nil, ErrABCINotConnected
		}
	}

	c.abciMutex.RLock()
	defer c.abciMutex.RUnlock()

	// 최대 재시도 횟수
	maxRetries := 3
	var lastErr error

	// 재시도 로직
	for i := 0; i < maxRetries; i++ {
		// 컨텍스트 만료 확인
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		resp := c.abciClient.EndBlockSync(req)
		if resp.Error == nil {
			return &resp.Response, nil
		}

		lastErr = resp.Error
		log.Warn("EndBlock request failed, retrying", "error", lastErr, "retry", i+1)
		time.Sleep(dialRetryDelay)
	}

	return nil, fmt.Errorf("failed to execute EndBlock after %d retries: %w", maxRetries, lastErr)
}

// Commit implements the ITendermintClient interface
func (c *TendermintABCIClient) Commit(ctx context.Context) (*types.ResponseCommit, error) {
	c.abciMutex.RLock()
	isConnected := c.isABCIConnected
	c.abciMutex.RUnlock()

	if !isConnected {
		// 자동 재연결 시도
		if err := c.ReconnectIfNeeded(); err != nil {
			return nil, fmt.Errorf("failed to reconnect: %w", err)
		}

		// 재연결 후에도 연결 상태 확인
		c.abciMutex.RLock()
		isConnected = c.isABCIConnected
		c.abciMutex.RUnlock()

		if !isConnected {
			return nil, ErrABCINotConnected
		}
	}

	c.abciMutex.RLock()
	defer c.abciMutex.RUnlock()

	// 최대 재시도 횟수
	maxRetries := 3
	var lastErr error

	// 재시도 로직
	for i := 0; i < maxRetries; i++ {
		// 컨텍스트 만료 확인
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		resp := c.abciClient.CommitSync()
		if resp.Error == nil {
			return &resp.Response, nil
		}

		lastErr = resp.Error
		log.Warn("Commit request failed, retrying", "error", lastErr, "retry", i+1)
		time.Sleep(dialRetryDelay)
	}

	return nil, fmt.Errorf("failed to execute Commit after %d retries: %w", maxRetries, lastErr)
}

// GetValidators implements the ITendermintClient interface
func (c *TendermintABCIClient) GetValidators(ctx context.Context) ([]*valset.Validator, error) {
	c.rpcMutex.RLock()
	isConnected := c.isRPCConnected
	c.rpcMutex.RUnlock()

	if !isConnected {
		// 자동 재연결 시도
		if err := c.ReconnectIfNeeded(); err != nil {
			return nil, fmt.Errorf("failed to reconnect: %w", err)
		}

		// 재연결 후에도 연결 상태 확인
		c.rpcMutex.RLock()
		isConnected = c.isRPCConnected
		c.rpcMutex.RUnlock()

		if !isConnected {
			return nil, ErrRPCNotConnected
		}
	}

	c.rpcMutex.RLock()
	defer c.rpcMutex.RUnlock()

	// Tendermint RPC를 통해 검증자 목록 가져오기
	validatorsResp, err := c.rpcClient.Validators(ctx, nil, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch validators: %w", err)
	}

	if validatorsResp == nil || validatorsResp.Validators == nil {
		return []*valset.Validator{}, nil
	}

	validators := make([]*valset.Validator, 0, len(validatorsResp.Validators))
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

		validators = append(validators, validator)
	}

	return validators, nil
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

// ReconnectIfNeeded attempts to reconnect if any connection is lost
func (c *TendermintABCIClient) ReconnectIfNeeded() error {
	// ABCI 연결 확인 및 재연결
	c.abciMutex.RLock()
	abciConnected := c.isABCIConnected && c.abciClient != nil && c.abciClient.IsRunning()
	c.abciMutex.RUnlock()

	if !abciConnected {
		log.Warn("ABCI connection lost, attempting to reconnect")
		if err := c.connectABCI(); err != nil {
			return fmt.Errorf("failed to reconnect to ABCI: %w", err)
		}
	}

	// RPC 연결 확인 및 재연결
	c.rpcMutex.RLock()
	rpcConnected := c.isRPCConnected && c.rpcClient != nil && c.rpcClient.IsRunning()
	c.rpcMutex.RUnlock()

	if !rpcConnected {
		log.Warn("RPC connection lost, attempting to reconnect")
		if err := c.connectRPC(); err != nil {
			return fmt.Errorf("failed to reconnect to RPC: %w", err)
		}
	}

	return nil
}

// StateSyncEvents는 특정 ID부터 지정된 블록까지의 상태 동기화 이벤트를 검색합니다.
func (c *TendermintABCIClient) StateSyncEvents(ctx context.Context, fromID uint64, to int64) ([]*clerk.EventRecordWithTime, error) {
	c.rpcMutex.RLock()
	isConnected := c.isRPCConnected
	c.rpcMutex.RUnlock()

	if !isConnected {
		// 자동 재연결 시도
		if err := c.ReconnectIfNeeded(); err != nil {
			return nil, fmt.Errorf("failed to reconnect: %w", err)
		}

		// 재연결 후에도 연결 상태 확인
		c.rpcMutex.RLock()
		isConnected = c.isRPCConnected
		c.rpcMutex.RUnlock()

		if !isConnected {
			return nil, ErrRPCNotConnected
		}
	}

	c.rpcMutex.RLock()
	defer c.rpcMutex.RUnlock()

	// Tendermint RPC를 통해 이벤트 조회
	// 여기서는 tx_search를 사용하여 특정 타입의 이벤트를 조회합니다
	query := fmt.Sprintf("state_sync.from_id >= %d AND state_sync.to_block <= %d", fromID, to)
	searchResult, err := c.rpcClient.TxSearch(ctx, query, false, nil, nil, "asc")
	if err != nil {
		return nil, fmt.Errorf("failed to search state sync events: %w", err)
	}

	// 결과가 없는 경우 빈 배열 반환
	if searchResult == nil || len(searchResult.Txs) == 0 {
		return []*clerk.EventRecordWithTime{}, nil
	}

	// 결과를 EventRecordWithTime 형태로 변환
	events := make([]*clerk.EventRecordWithTime, 0, len(searchResult.Txs))
	for _, tx := range searchResult.Txs {
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
					id, _ = strconv.ParseUint(string(value), 10, 64)
				case "state_id":
					stateID, _ = strconv.ParseUint(string(value), 10, 64)
				case "time":
					timeInt, _ := strconv.ParseInt(string(value), 10, 64)
					recordTime = time.Unix(timeInt, 0)
				case "data":
					data = value
				}
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

// Span는 지정된 spanID에 해당하는 Tendermint 스팬 정보를 가져옵니다.
func (c *TendermintABCIClient) Span(ctx context.Context, spanID uint64) (*span.TendermintSpan, error) {
	c.rpcMutex.RLock()
	isConnected := c.isRPCConnected
	c.rpcMutex.RUnlock()

	if !isConnected {
		// 자동 재연결 시도
		if err := c.ReconnectIfNeeded(); err != nil {
			return nil, fmt.Errorf("failed to reconnect: %w", err)
		}

		// 재연결 후에도 연결 상태 확인
		c.rpcMutex.RLock()
		isConnected = c.isRPCConnected
		c.rpcMutex.RUnlock()

		if !isConnected {
			return nil, ErrRPCNotConnected
		}
	}

	c.rpcMutex.RLock()
	defer c.rpcMutex.RUnlock()

	// Tendermint ABCI Query를 사용하여 스팬 정보 조회
	path := fmt.Sprintf("span/%d", spanID)
	queryResult, err := c.rpcClient.ABCIQuery(ctx, path, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to query span: %w", err)
	}

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

	return &tendermintSpan, nil
}

// FetchCheckpoint는 지정된 번호의 체크포인트를 조회합니다.
func (c *TendermintABCIClient) FetchCheckpoint(ctx context.Context, number int64) (*checkpoint.Checkpoint, error) {
	c.rpcMutex.RLock()
	isConnected := c.isRPCConnected
	c.rpcMutex.RUnlock()

	if !isConnected {
		// 자동 재연결 시도
		if err := c.ReconnectIfNeeded(); err != nil {
			return nil, fmt.Errorf("failed to reconnect: %w", err)
		}

		// 재연결 후에도 연결 상태 확인
		c.rpcMutex.RLock()
		isConnected = c.isRPCConnected
		c.rpcMutex.RUnlock()

		if !isConnected {
			return nil, ErrRPCNotConnected
		}
	}

	c.rpcMutex.RLock()
	defer c.rpcMutex.RUnlock()

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
	isConnected := c.isRPCConnected
	c.rpcMutex.RUnlock()

	if !isConnected {
		// 자동 재연결 시도
		if err := c.ReconnectIfNeeded(); err != nil {
			return 0, fmt.Errorf("failed to reconnect: %w", err)
		}

		// 재연결 후에도 연결 상태 확인
		c.rpcMutex.RLock()
		isConnected = c.isRPCConnected
		c.rpcMutex.RUnlock()

		if !isConnected {
			return 0, ErrRPCNotConnected
		}
	}

	c.rpcMutex.RLock()
	defer c.rpcMutex.RUnlock()

	// 최대 재시도 횟수
	maxRetries := 3
	var lastErr error

	// 재시도 로직
	for i := 0; i < maxRetries; i++ {
		// 컨텍스트 만료 확인
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}

		// Tendermint ABCI Query를 사용하여 체크포인트 개수 조회
		path := "checkpoint/count"
		queryResult, err := c.rpcClient.ABCIQuery(ctx, path, nil)
		if err != nil {
			lastErr = err
			log.Warn("Failed to query checkpoint count, retrying", "error", err, "retry", i+1)
			time.Sleep(dialRetryDelay)
			continue
		}

		if queryResult.Response.Code != 0 {
			lastErr = fmt.Errorf("query failed with code %d: %s",
				queryResult.Response.Code, queryResult.Response.Log)
			log.Warn("Query returned error code, retrying", "error", lastErr, "retry", i+1)
			time.Sleep(dialRetryDelay)
			continue
		}

		// 응답 데이터가 없는 경우
		if len(queryResult.Response.Value) == 0 {
			lastErr = fmt.Errorf("checkpoint count not found")
			log.Warn("Empty response data, retrying", "error", lastErr, "retry", i+1)
			time.Sleep(dialRetryDelay)
			continue
		}

		// 응답 데이터를 CheckpointCount 응답으로 언마샬
		var countResp checkpoint.CheckpointCountResponse
		if err := json.Unmarshal(queryResult.Response.Value, &countResp); err != nil {
			lastErr = fmt.Errorf("failed to unmarshal checkpoint count data: %w", err)
			log.Warn("Failed to unmarshal response, retrying", "error", lastErr, "retry", i+1)
			time.Sleep(dialRetryDelay)
			continue
		}

		return countResp.Result.Result, nil
	}

	return 0, fmt.Errorf("failed to fetch checkpoint count after %d retries: %w", maxRetries, lastErr)
}

// FetchMilestone는 최신 마일스톤을 조회합니다.
func (c *TendermintABCIClient) FetchMilestone(ctx context.Context) (*milestone.Milestone, error) {
	c.rpcMutex.RLock()
	isConnected := c.isRPCConnected
	c.rpcMutex.RUnlock()

	if !isConnected {
		// 자동 재연결 시도
		if err := c.ReconnectIfNeeded(); err != nil {
			return nil, fmt.Errorf("failed to reconnect: %w", err)
		}

		// 재연결 후에도 연결 상태 확인
		c.rpcMutex.RLock()
		isConnected = c.isRPCConnected
		c.rpcMutex.RUnlock()

		if !isConnected {
			return nil, ErrRPCNotConnected
		}
	}

	c.rpcMutex.RLock()
	defer c.rpcMutex.RUnlock()

	// 최대 재시도 횟수
	maxRetries := 3
	var lastErr error

	// 재시도 로직
	for i := 0; i < maxRetries; i++ {
		// 컨텍스트 만료 확인
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		// Tendermint ABCI Query를 사용하여 최신 마일스톤 조회
		path := "milestone/latest"
		queryResult, err := c.rpcClient.ABCIQuery(ctx, path, nil)
		if err != nil {
			lastErr = err
			log.Warn("Failed to query latest milestone, retrying", "error", err, "retry", i+1)
			time.Sleep(dialRetryDelay)
			continue
		}

		if queryResult.Response.Code != 0 {
			lastErr = fmt.Errorf("query failed with code %d: %s",
				queryResult.Response.Code, queryResult.Response.Log)
			log.Warn("Query returned error code, retrying", "error", lastErr, "retry", i+1)
			time.Sleep(dialRetryDelay)
			continue
		}

		// 응답 데이터가 없는 경우
		if len(queryResult.Response.Value) == 0 {
			lastErr = fmt.Errorf("latest milestone not found")
			log.Warn("Empty response data, retrying", "error", lastErr, "retry", i+1)
			time.Sleep(dialRetryDelay)
			continue
		}

		// 응답 데이터를 Milestone 응답으로 언마샬
		var milestoneResp milestone.MilestoneResponse
		if err := json.Unmarshal(queryResult.Response.Value, &milestoneResp); err != nil {
			lastErr = fmt.Errorf("failed to unmarshal milestone data: %w", err)
			log.Warn("Failed to unmarshal response, retrying", "error", lastErr, "retry", i+1)
			time.Sleep(dialRetryDelay)
			continue
		}

		return &milestoneResp.Result, nil
	}

	return nil, fmt.Errorf("failed to fetch latest milestone after %d retries: %w", maxRetries, lastErr)
}

// FetchMilestoneCount는 마일스톤의 총 개수를 조회합니다.
func (c *TendermintABCIClient) FetchMilestoneCount(ctx context.Context) (int64, error) {
	c.rpcMutex.RLock()
	isConnected := c.isRPCConnected
	c.rpcMutex.RUnlock()

	if !isConnected {
		// 자동 재연결 시도
		if err := c.ReconnectIfNeeded(); err != nil {
			return 0, fmt.Errorf("failed to reconnect: %w", err)
		}

		// 재연결 후에도 연결 상태 확인
		c.rpcMutex.RLock()
		isConnected = c.isRPCConnected
		c.rpcMutex.RUnlock()

		if !isConnected {
			return 0, ErrRPCNotConnected
		}
	}

	c.rpcMutex.RLock()
	defer c.rpcMutex.RUnlock()

	// 최대 재시도 횟수
	maxRetries := 3
	var lastErr error

	// 재시도 로직
	for i := 0; i < maxRetries; i++ {
		// 컨텍스트 만료 확인
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}

		// Tendermint ABCI Query를 사용하여 마일스톤 개수 조회
		path := "milestone/count"
		queryResult, err := c.rpcClient.ABCIQuery(ctx, path, nil)
		if err != nil {
			lastErr = err
			log.Warn("Failed to query milestone count, retrying", "error", err, "retry", i+1)
			time.Sleep(dialRetryDelay)
			continue
		}

		if queryResult.Response.Code != 0 {
			lastErr = fmt.Errorf("query failed with code %d: %s",
				queryResult.Response.Code, queryResult.Response.Log)
			log.Warn("Query returned error code, retrying", "error", lastErr, "retry", i+1)
			time.Sleep(dialRetryDelay)
			continue
		}

		// 응답 데이터가 없는 경우
		if len(queryResult.Response.Value) == 0 {
			lastErr = fmt.Errorf("milestone count not found")
			log.Warn("Empty response data, retrying", "error", lastErr, "retry", i+1)
			time.Sleep(dialRetryDelay)
			continue
		}

		// 응답 데이터를 MilestoneCount 응답으로 언마샬
		var countResp milestone.MilestoneCountResponse
		if err := json.Unmarshal(queryResult.Response.Value, &countResp); err != nil {
			lastErr = fmt.Errorf("failed to unmarshal milestone count data: %w", err)
			log.Warn("Failed to unmarshal response, retrying", "error", lastErr, "retry", i+1)
			time.Sleep(dialRetryDelay)
			continue
		}

		return countResp.Result.Count, nil
	}

	return 0, fmt.Errorf("failed to fetch milestone count after %d retries: %w", maxRetries, lastErr)
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
