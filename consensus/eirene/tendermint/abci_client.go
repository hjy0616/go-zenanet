package tendermint

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
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
	// ErrNotInRejectedList is returned when the milestone is not in the rejected list
	ErrNotInRejectedList = errors.New("milestone not in rejected list")
	// ErrNotInMilestoneList is returned when the milestone is not in the milestone list
	ErrNotInMilestoneList = errors.New("milestone not in milestone list")
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
	// 첫 번째로 ABCI 연결 시도
	abciErr := c.connectABCIWithRetry()

	// 두 번째로 RPC 연결 시도
	rpcErr := c.connectRPCWithRetry()

	// 두 연결 모두 실패한 경우만 오류 반환
	if abciErr != nil && rpcErr != nil {
		return fmt.Errorf("failed to connect to Tendermint: ABCI error: %v, RPC error: %v", abciErr, rpcErr)
	}

	// 경고 로깅 (하나라도 실패한 경우)
	if abciErr != nil {
		log.Warn("Failed to connect to ABCI endpoint, will continue with RPC only", "error", abciErr)
	}

	if rpcErr != nil {
		log.Warn("Failed to connect to RPC endpoint, will continue with ABCI only", "error", rpcErr)
	}

	// 모니터링 고루틴 시작
	go c.monitorConnections()

	return nil
}

// connectABCIWithRetry attempts to connect to the ABCI endpoint with retry mechanism
func (c *TendermintABCIClient) connectABCIWithRetry() error {
	retryCount := 0
	maxRetries := 5
	backoffFactor := 1.5
	retryDelay := dialRetryDelay

	for retryCount < maxRetries {
		err := c.connectABCI()
		if err == nil {
			return nil
		}

		retryCount++
		if retryCount >= maxRetries {
			return fmt.Errorf("failed to connect to ABCI after %d attempts: %w", maxRetries, err)
		}

		log.Warn("Failed to connect to ABCI endpoint, retrying...",
			"attempt", retryCount,
			"maxRetries", maxRetries,
			"delay", retryDelay,
			"error", err)

		time.Sleep(retryDelay)
		retryDelay = time.Duration(float64(retryDelay) * backoffFactor)
	}

	return errors.New("failed to connect to ABCI endpoint after max retries")
}

// connectRPCWithRetry attempts to connect to the RPC endpoint with retry mechanism
func (c *TendermintABCIClient) connectRPCWithRetry() error {
	retryCount := 0
	maxRetries := 5
	backoffFactor := 1.5
	retryDelay := dialRetryDelay

	for retryCount < maxRetries {
		err := c.connectRPC()
		if err == nil {
			return nil
		}

		retryCount++
		if retryCount >= maxRetries {
			return fmt.Errorf("failed to connect to RPC after %d attempts: %w", maxRetries, err)
		}

		log.Warn("Failed to connect to RPC endpoint, retrying...",
			"attempt", retryCount,
			"maxRetries", maxRetries,
			"delay", retryDelay,
			"error", err)

		time.Sleep(retryDelay)
		retryDelay = time.Duration(float64(retryDelay) * backoffFactor)
	}

	return errors.New("failed to connect to RPC endpoint after max retries")
}

// monitorConnections periodically checks connection status and attempts to reconnect if needed
func (c *TendermintABCIClient) monitorConnections() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.closeCh:
			return
		case <-ticker.C:
			// ABCI 연결 확인 및 필요시 재연결
			c.abciMutex.RLock()
			abciConnected := c.isABCIConnected
			c.abciMutex.RUnlock()

			if !abciConnected {
				log.Info("Trying to reconnect to ABCI endpoint")
				if err := c.connectABCI(); err != nil {
					log.Warn("Failed to reconnect to ABCI endpoint", "error", err)
				} else {
					log.Info("Successfully reconnected to ABCI endpoint")
				}
			}

			// RPC 연결 확인 및 필요시 재연결
			c.rpcMutex.RLock()
			rpcConnected := c.isRPCConnected
			c.rpcMutex.RUnlock()

			if !rpcConnected {
				log.Info("Trying to reconnect to RPC endpoint")
				if err := c.connectRPC(); err != nil {
					log.Warn("Failed to reconnect to RPC endpoint", "error", err)
				} else {
					log.Info("Successfully reconnected to RPC endpoint")
				}
			}
		}
	}
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

	// Context와 함께 연결 시도, ctx 파일을 수정해야함 나중에
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

	// Context와 함께 연결 시도, ctx 파일을 수정해야함 나중에
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

// InitChain implements the ITendermintClient interface
func (c *TendermintABCIClient) InitChain(ctx context.Context, req types.RequestInitChain) (*types.ResponseInitChain, error) {
	c.abciMutex.RLock()
	defer c.abciMutex.RUnlock()

	if !c.isABCIConnected {
		return nil, ErrABCINotConnected
	}

	resp := c.abciClient.InitChainSync(req)
	if resp.Error != nil {
		return nil, resp.Error
	}

	return &resp.Response, nil
}

// BeginBlock implements the ITendermintClient interface
func (c *TendermintABCIClient) BeginBlock(ctx context.Context, req types.RequestBeginBlock) (*types.ResponseBeginBlock, error) {
	c.abciMutex.RLock()
	defer c.abciMutex.RUnlock()

	if !c.isABCIConnected {
		return nil, ErrABCINotConnected
	}

	resp := c.abciClient.BeginBlockSync(req)
	if resp.Error != nil {
		return nil, resp.Error
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
	c.rpcMutex.RLock()
	defer c.rpcMutex.RUnlock()

	if !c.isRPCConnected {
		return nil, ErrRPCNotConnected
	}

	// 최신 검증자 정보 요청
	result, err := c.rpcClient.Validators(ctx, nil, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get validators: %w", err)
	}

	validators := make([]*valset.Validator, 0, len(result.Validators))
	for _, v := range result.Validators {
		// Tendermint 검증자를 Eirene valset.Validator로 변환
		addr := common.BytesToAddress(v.Address)
		power := v.VotingPower

		validator := valset.NewValidator(addr, power)
		validators = append(validators, validator)
	}

	return validators, nil
}

// GetCurrentValidatorSet implements the ITendermintClient interface
func (c *TendermintABCIClient) GetCurrentValidatorSet(ctx context.Context) (*valset.ValidatorSet, error) {
	validators, err := c.GetValidators(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get validators: %w", err)
	}

	if len(validators) == 0 {
		return nil, errors.New("received empty validator set")
	}

	// 검증자 세트 생성
	validatorSet := valset.NewValidatorSet(validators)

	// 검증자 우선순위 초기화 (Tendermint 합의를 위해 필요)
	validatorSet.IncrementProposerPriority(1)

	return validatorSet, nil
}

// StateSyncEvents implements the ITendermintClient interface
func (c *TendermintABCIClient) StateSyncEvents(ctx context.Context, fromID uint64, to int64) ([]*clerk.EventRecordWithTime, error) {
	c.rpcMutex.RLock()
	defer c.rpcMutex.RUnlock()

	if !c.isRPCConnected {
		return nil, ErrRPCNotConnected
	}

	// 상태 동기화 이벤트를 가져올 쿼리 생성
	query := fmt.Sprintf("state_sync.from_id=%d AND state_sync.to_time=%d", fromID, to)

	// 페이지 옵션 설정 (한 번에 가져올 최대 이벤트 수)
	pageSize := 50
	page := 1

	// 결과를 저장할 슬라이스
	var allEvents []*clerk.EventRecordWithTime

	for {
		// RPC 클라이언트를 통해 이벤트 쿼리
		result, err := c.rpcClient.TxSearch(ctx, query, false, &page, &pageSize, "asc")
		if err != nil {
			return nil, fmt.Errorf("failed to search state sync events: %w", err)
		}

		// 결과가 없으면 중단
		if len(result.Txs) == 0 {
			break
		}

		// 각 트랜잭션에서 이벤트 추출
		for _, tx := range result.Txs {
			// 각 트랜잭션의 결과에서 이벤트 추출
			for _, event := range tx.TxResult.Events {
				// 상태 동기화 이벤트만 처리
				if event.Type == "state_sync" {
					// 이벤트 데이터 파싱
					eventRecord, err := parseStateEventFromAttributes(event.Attributes, tx.Height, tx.TxResult.Time)
					if err != nil {
						log.Warn("Failed to parse state sync event", "error", err)
						continue
					}

					// ID가 fromID 이상인 이벤트만 추가
					if eventRecord.ID >= fromID {
						allEvents = append(allEvents, eventRecord)
					}
				}
			}
		}

		// 가져온 결과가 페이지 크기보다 적으면 더 이상 데이터가 없음
		if len(result.Txs) < pageSize {
			break
		}

		// 다음 페이지로
		page++
	}

	// ID 기준으로 정렬
	sort.Slice(allEvents, func(i, j int) bool {
		return allEvents[i].ID < allEvents[j].ID
	})

	return allEvents, nil
}

// parseStateEventFromAttributes 는 Tendermint 이벤트 속성에서 EventRecord를 파싱합니다
func parseStateEventFromAttributes(attrs []tmrpc.EventAttribute, height int64, timestamp time.Time) (*clerk.EventRecordWithTime, error) {
	record := &clerk.EventRecordWithTime{
		EventRecord: clerk.EventRecord{},
		Time:        timestamp,
	}

	for _, attr := range attrs {
		key := string(attr.Key)
		value := string(attr.Value)

		switch key {
		case "id":
			id, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid event ID: %w", err)
			}
			record.ID = id
		case "contract":
			record.Contract = common.HexToAddress(value)
		case "data":
			record.Data = common.FromHex(value)
		case "chain_id":
			record.ChainID = value
		case "event_type":
			record.Type = value
		}
	}

	// 필수 필드 검증
	if record.ID == 0 {
		return nil, fmt.Errorf("event missing ID")
	}

	return record, nil
}

// Span implements the ITendermintClient interface
func (c *TendermintABCIClient) Span(ctx context.Context, spanID uint64) (*span.TendermintSpan, error) {
	c.rpcMutex.RLock()
	defer c.rpcMutex.RUnlock()

	if !c.isRPCConnected {
		return nil, ErrRPCNotConnected
	}

	// Tendermint 커스텀 쿼리를 사용하여 스팬 정보 요청
	// 쿼리 경로: custom/eirene/span/{id}
	path := fmt.Sprintf("custom/eirene/span/%d", spanID)

	result, err := c.rpcClient.ABCIQuery(ctx, path, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to query span information: %w", err)
	}

	if !result.Response.IsOK() {
		return nil, fmt.Errorf("failed to get span: %s", result.Response.Log)
	}

	// 응답이 비어있는 경우
	if len(result.Response.Value) == 0 {
		return nil, fmt.Errorf("span not found for ID: %d", spanID)
	}

	// JSON 응답 파싱
	var spanResponse struct {
		Height string              `json:"height"`
		Result span.TendermintSpan `json:"result"`
	}

	if err := json.Unmarshal(result.Response.Value, &spanResponse); err != nil {
		return nil, fmt.Errorf("failed to unmarshal span data: %w", err)
	}

	// 스팬 검증
	if spanResponse.Result.ID != spanID {
		return nil, fmt.Errorf("received span ID (%d) doesn't match requested ID (%d)",
			spanResponse.Result.ID, spanID)
	}

	// 검증자 세트가 없는 경우, 현재 검증자 세트 사용
	if spanResponse.Result.ValidatorSet.IsNilOrEmpty() {
		validatorSet, err := c.GetCurrentValidatorSet(ctx)
		if err != nil {
			log.Warn("Failed to get current validator set for span", "error", err)
		} else {
			spanResponse.Result.ValidatorSet = *validatorSet
		}
	}

	return &spanResponse.Result, nil
}

// FetchCheckpoint implements the ITendermintClient interface
func (c *TendermintABCIClient) FetchCheckpoint(ctx context.Context, number int64) (*checkpoint.Checkpoint, error) {
	c.rpcMutex.RLock()
	defer c.rpcMutex.RUnlock()

	if !c.isRPCConnected {
		return nil, ErrRPCNotConnected
	}

	// 체크포인트 쿼리 경로 구성
	path := fmt.Sprintf("custom/checkpoint/%d", number)

	result, err := c.rpcClient.ABCIQuery(ctx, path, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to query checkpoint: %w", err)
	}

	if !result.Response.IsOK() {
		return nil, fmt.Errorf("checkpoint query failed: %s", result.Response.Log)
	}

	// 응답이 비어있는 경우
	if len(result.Response.Value) == 0 {
		return nil, fmt.Errorf("no checkpoint found for number: %d", number)
	}

	// JSON 응답 파싱
	var checkpointResponse struct {
		Height string                `json:"height"`
		Result checkpoint.Checkpoint `json:"result"`
	}

	if err := json.Unmarshal(result.Response.Value, &checkpointResponse); err != nil {
		return nil, fmt.Errorf("failed to unmarshal checkpoint data: %w", err)
	}

	return &checkpointResponse.Result, nil
}

// FetchCheckpointCount implements the ITendermintClient interface
func (c *TendermintABCIClient) FetchCheckpointCount(ctx context.Context) (int64, error) {
	c.rpcMutex.RLock()
	defer c.rpcMutex.RUnlock()

	if !c.isRPCConnected {
		return 0, ErrRPCNotConnected
	}

	// 체크포인트 개수 쿼리 경로
	path := "custom/checkpoint/count"

	result, err := c.rpcClient.ABCIQuery(ctx, path, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to query checkpoint count: %w", err)
	}

	if !result.Response.IsOK() {
		return 0, fmt.Errorf("checkpoint count query failed: %s", result.Response.Log)
	}

	// 응답이 비어있는 경우
	if len(result.Response.Value) == 0 {
		return 0, errors.New("empty response for checkpoint count")
	}

	// JSON 응답 파싱
	var countResponse struct {
		Height string `json:"height"`
		Result struct {
			Count int64 `json:"count"`
		} `json:"result"`
	}

	if err := json.Unmarshal(result.Response.Value, &countResponse); err != nil {
		return 0, fmt.Errorf("failed to unmarshal checkpoint count data: %w", err)
	}

	return countResponse.Result.Count, nil
}

// FetchMilestone implements the ITendermintClient interface
func (c *TendermintABCIClient) FetchMilestone(ctx context.Context) (*milestone.Milestone, error) {
	c.rpcMutex.RLock()
	defer c.rpcMutex.RUnlock()

	if !c.isRPCConnected {
		return nil, ErrRPCNotConnected
	}

	// 최신 마일스톤 쿼리 경로
	path := "custom/milestone/latest"

	result, err := c.rpcClient.ABCIQuery(ctx, path, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to query latest milestone: %w", err)
	}

	if !result.Response.IsOK() {
		return nil, fmt.Errorf("milestone query failed: %s", result.Response.Log)
	}

	// 응답이 비어있는 경우
	if len(result.Response.Value) == 0 {
		return nil, errors.New("no milestone found")
	}

	// JSON 응답 파싱
	var milestoneResponse struct {
		Height string              `json:"height"`
		Result milestone.Milestone `json:"result"`
	}

	if err := json.Unmarshal(result.Response.Value, &milestoneResponse); err != nil {
		return nil, fmt.Errorf("failed to unmarshal milestone data: %w", err)
	}

	return &milestoneResponse.Result, nil
}

// FetchMilestoneCount implements the ITendermintClient interface
func (c *TendermintABCIClient) FetchMilestoneCount(ctx context.Context) (int64, error) {
	c.rpcMutex.RLock()
	defer c.rpcMutex.RUnlock()

	if !c.isRPCConnected {
		return 0, ErrRPCNotConnected
	}

	// 마일스톤 개수 쿼리 경로
	path := "custom/milestone/count"

	result, err := c.rpcClient.ABCIQuery(ctx, path, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to query milestone count: %w", err)
	}

	if !result.Response.IsOK() {
		return 0, fmt.Errorf("milestone count query failed: %s", result.Response.Log)
	}

	// 응답이 비어있는 경우
	if len(result.Response.Value) == 0 {
		return 0, errors.New("empty response for milestone count")
	}

	// JSON 응답 파싱
	var countResponse struct {
		Height string `json:"height"`
		Result struct {
			Count int64 `json:"count"`
		} `json:"result"`
	}

	if err := json.Unmarshal(result.Response.Value, &countResponse); err != nil {
		return 0, fmt.Errorf("failed to unmarshal milestone count data: %w", err)
	}

	return countResponse.Result.Count, nil
}

// FetchNoAckMilestone implements the ITendermintClient interface
func (c *TendermintABCIClient) FetchNoAckMilestone(ctx context.Context, milestoneID string) error {
	c.rpcMutex.RLock()
	defer c.rpcMutex.RUnlock()

	if !c.isRPCConnected {
		return ErrRPCNotConnected
	}

	// 실패한 마일스톤 쿼리 경로
	path := fmt.Sprintf("custom/milestone/noack/%s", milestoneID)

	result, err := c.rpcClient.ABCIQuery(ctx, path, nil)
	if err != nil {
		return fmt.Errorf("failed to query no-ack milestone: %w", err)
	}

	if !result.Response.IsOK() {
		if strings.Contains(result.Response.Log, "not found") {
			return ErrNotInRejectedList
		}
		return fmt.Errorf("no-ack milestone query failed: %s", result.Response.Log)
	}

	return nil
}

// FetchLastNoAckMilestone implements the ITendermintClient interface
func (c *TendermintABCIClient) FetchLastNoAckMilestone(ctx context.Context) (string, error) {
	c.rpcMutex.RLock()
	defer c.rpcMutex.RUnlock()

	if !c.isRPCConnected {
		return "", ErrRPCNotConnected
	}

	// 마지막 실패 마일스톤 쿼리 경로
	path := "custom/milestone/lastnoack"

	result, err := c.rpcClient.ABCIQuery(ctx, path, nil)
	if err != nil {
		return "", fmt.Errorf("failed to query last no-ack milestone: %w", err)
	}

	if !result.Response.IsOK() {
		return "", fmt.Errorf("last no-ack milestone query failed: %s", result.Response.Log)
	}

	// 응답이 비어있는 경우
	if len(result.Response.Value) == 0 {
		return "", errors.New("no last no-ack milestone found")
	}

	// JSON 응답 파싱
	var lastNoAckResponse struct {
		Height string `json:"height"`
		Result struct {
			ID string `json:"id"`
		} `json:"result"`
	}

	if err := json.Unmarshal(result.Response.Value, &lastNoAckResponse); err != nil {
		return "", fmt.Errorf("failed to unmarshal last no-ack milestone data: %w", err)
	}

	return lastNoAckResponse.Result.ID, nil
}

// FetchMilestoneID implements the ITendermintClient interface
func (c *TendermintABCIClient) FetchMilestoneID(ctx context.Context, milestoneID string) error {
	c.rpcMutex.RLock()
	defer c.rpcMutex.RUnlock()

	if !c.isRPCConnected {
		return ErrRPCNotConnected
	}

	// 마일스톤 ID 쿼리 경로
	path := fmt.Sprintf("custom/milestone/id/%s", milestoneID)

	result, err := c.rpcClient.ABCIQuery(ctx, path, nil)
	if err != nil {
		return fmt.Errorf("failed to query milestone ID: %w", err)
	}

	if !result.Response.IsOK() {
		if strings.Contains(result.Response.Log, "not found") {
			return ErrNotInMilestoneList
		}
		return fmt.Errorf("milestone ID query failed: %s", result.Response.Log)
	}

	return nil
}

// Close closes connections to both ABCI and RPC endpoints
func (c *TendermintABCIClient) Close() {
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

	// ABCI 클라이언트 닫기
	wg.Add(1)
	go func() {
		defer wg.Done()

		c.abciMutex.Lock()
		defer c.abciMutex.Unlock()

		if c.isABCIConnected && c.abciClient != nil {
			if c.abciClient.IsRunning() {
				if err := c.abciClient.Stop(); err != nil {
					log.Error("Error stopping ABCI client", "error", err)
				}
			}
			c.isABCIConnected = false
		}
	}()

	// RPC 클라이언트 닫기
	wg.Add(1)
	go func() {
		defer wg.Done()

		c.rpcMutex.Lock()
		defer c.rpcMutex.Unlock()

		if c.isRPCConnected && c.rpcClient != nil {
			if c.rpcClient.IsRunning() {
				if err := c.rpcClient.Stop(); err != nil {
					log.Error("Error stopping RPC client", "error", err)
				}
			}
			c.isRPCConnected = false
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
		log.Info("Successfully closed Tendermint ABCI client connections")
	case <-time.After(5 * time.Second):
		log.Warn("Timeout while waiting for Tendermint connections to close")
	}
}
