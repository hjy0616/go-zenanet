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

	// 재연결 루프 시작
	c.StartReconnectLoop()

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
		log.Warn("RPC client not connected when getting validators, attempting to reconnect...")
		c.rpcMutex.RUnlock() // 현재 락 해제
		if err := c.connectRPC(); err != nil {
			log.Error("Failed to reconnect RPC client for validators", "error", err)
			c.rpcMutex.RLock() // 다시 락 설정
			return nil, ErrRPCNotConnected
		}
		c.rpcMutex.RLock() // 다시 락 설정
	}

	// Tendermint RPC를 통해 검증자 목록 가져오기
	log.Debug("Fetching validators from Tendermint")
	validatorsResp, err := c.rpcClient.Validators(ctx, nil, nil, nil)
	if err != nil {
		log.Error("Failed to fetch validators", "error", err)
		return nil, fmt.Errorf("failed to fetch validators: %w", err)
	}

	log.Debug("Retrieved validators from Tendermint", "count", len(validatorsResp.Validators))
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
		log.Debug("Added validator", "address", ethAddr.Hex(), "power", v.VotingPower, "ID", validator.ID)
	}

	return validators, nil
}

// GetCurrentValidatorSet implements the ITendermintClient interface
func (c *TendermintABCIClient) GetCurrentValidatorSet(ctx context.Context) (*valset.ValidatorSet, error) {
	log.Debug("Fetching current validator set from Tendermint")
	validators, err := c.GetValidators(ctx)
	if err != nil {
		log.Error("Failed to get validators for validator set", "error", err)
		return nil, err
	}

	if len(validators) == 0 {
		log.Warn("Empty validator set received from Tendermint")
	}

	// 검증자 세트 생성
	validatorSet := valset.NewValidatorSet(validators)
	log.Debug("Created validator set", "count", len(validators), "totalVotingPower", validatorSet.TotalVotingPower())
	return validatorSet, nil
}

// StateSyncEvents는 특정 ID부터 지정된 블록까지의 상태 동기화 이벤트를 검색합니다.
func (c *TendermintABCIClient) StateSyncEvents(ctx context.Context, fromID uint64, to int64) ([]*clerk.EventRecordWithTime, error) {
	c.rpcMutex.RLock()
	defer c.rpcMutex.RUnlock()

	if !c.isRPCConnected {
		log.Warn("RPC client not connected, attempting to reconnect...")
		c.rpcMutex.RUnlock() // 현재 락 해제
		if err := c.connectRPC(); err != nil {
			log.Error("Failed to reconnect RPC client", "error", err)
			c.rpcMutex.RLock() // 다시 락 설정
			return nil, ErrRPCNotConnected
		}
		c.rpcMutex.RLock() // 다시 락 설정
	}

	// Tendermint RPC를 통해 이벤트 조회
	query := fmt.Sprintf("state_sync.from_id >= %d AND state_sync.to_block <= %d", fromID, to)
	searchResult, err := c.rpcClient.TxSearch(ctx, query, false, nil, nil, "asc")
	if err != nil {
		log.Error("Failed to search state sync events", "error", err, "query", query)
		return nil, fmt.Errorf("failed to search state sync events: %w", err)
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

	if len(events) == 0 {
		log.Info("No state sync events found", "fromID", fromID, "to", to)
	} else {
		log.Info("Retrieved state sync events", "count", len(events), "fromID", fromID, "to", to)
	}

	return events, nil
}

// Span는 지정된 spanID에 해당하는 Tendermint 스팬 정보를 가져옵니다.
func (c *TendermintABCIClient) Span(ctx context.Context, spanID uint64) (*span.TendermintSpan, error) {
	c.rpcMutex.RLock()
	defer c.rpcMutex.RUnlock()

	if !c.isRPCConnected {
		log.Warn("RPC client not connected when fetching span, attempting to reconnect...", "spanID", spanID)
		c.rpcMutex.RUnlock() // 현재 락 해제
		if err := c.connectRPC(); err != nil {
			log.Error("Failed to reconnect RPC client for span", "error", err)
			c.rpcMutex.RLock() // 다시 락 설정
			return nil, ErrRPCNotConnected
		}
		c.rpcMutex.RLock() // 다시 락 설정
	}

	// Tendermint ABCI Query를 사용하여 스팬 정보 조회
	path := fmt.Sprintf("span/%d", spanID)
	log.Debug("Querying Tendermint for span", "spanID", spanID, "path", path)

	queryResult, err := c.rpcClient.ABCIQuery(ctx, path, nil)
	if err != nil {
		log.Error("Failed to query span", "spanID", spanID, "error", err)
		return nil, fmt.Errorf("failed to query span: %w", err)
	}

	if queryResult.Response.Code != 0 {
		log.Error("Span query failed", "spanID", spanID, "code", queryResult.Response.Code, "log", queryResult.Response.Log)
		return nil, fmt.Errorf("query failed with code %d: %s",
			queryResult.Response.Code, queryResult.Response.Log)
	}

	// 응답 데이터가 없는 경우
	if len(queryResult.Response.Value) == 0 {
		log.Error("No span data found", "spanID", spanID)
		return nil, fmt.Errorf("no span found for ID %d", spanID)
	}

	// 응답 데이터를 TendermintSpan으로 언마샬
	var tendermintSpan span.TendermintSpan
	if err := json.Unmarshal(queryResult.Response.Value, &tendermintSpan); err != nil {
		log.Error("Failed to unmarshal span data", "spanID", spanID, "error", err, "data", string(queryResult.Response.Value))
		return nil, fmt.Errorf("failed to unmarshal span data: %w", err)
	}

	log.Debug("Successfully retrieved span", "spanID", spanID, "startBlock", tendermintSpan.StartBlock, "endBlock", tendermintSpan.EndBlock)
	return &tendermintSpan, nil
}

// FetchCheckpoint는 지정된 번호의 체크포인트를 조회합니다.
func (c *TendermintABCIClient) FetchCheckpoint(ctx context.Context, number int64) (*checkpoint.Checkpoint, error) {
	c.rpcMutex.RLock()
	defer c.rpcMutex.RUnlock()

	if !c.isRPCConnected {
		log.Warn("RPC client not connected when fetching checkpoint, attempting to reconnect...", "checkpointNumber", number)
		c.rpcMutex.RUnlock() // 현재 락 해제
		if err := c.connectRPC(); err != nil {
			log.Error("Failed to reconnect RPC client for checkpoint", "error", err)
			c.rpcMutex.RLock() // 다시 락 설정
			return nil, ErrRPCNotConnected
		}
		c.rpcMutex.RLock() // 다시 락 설정
	}

	// Tendermint ABCI Query를 사용하여 체크포인트 조회
	path := fmt.Sprintf("checkpoint/%d", number)
	log.Debug("Querying Tendermint for checkpoint", "checkpointNumber", number, "path", path)

	queryResult, err := c.rpcClient.ABCIQuery(ctx, path, nil)
	if err != nil {
		log.Error("Failed to query checkpoint", "checkpointNumber", number, "error", err)
		return nil, fmt.Errorf("failed to query checkpoint: %w", err)
	}

	if queryResult.Response.Code != 0 {
		log.Error("Checkpoint query failed", "checkpointNumber", number, "code", queryResult.Response.Code, "log", queryResult.Response.Log)
		return nil, fmt.Errorf("query failed with code %d: %s",
			queryResult.Response.Code, queryResult.Response.Log)
	}

	// 응답 데이터가 없는 경우
	if len(queryResult.Response.Value) == 0 {
		log.Error("No checkpoint data found", "checkpointNumber", number)
		return nil, fmt.Errorf("no checkpoint found for number %d", number)
	}

	// 응답 데이터를 Checkpoint 응답으로 언마샬
	var checkpointResp checkpoint.CheckpointResponse
	if err := json.Unmarshal(queryResult.Response.Value, &checkpointResp); err != nil {
		log.Error("Failed to unmarshal checkpoint data", "checkpointNumber", number, "error", err, "data", string(queryResult.Response.Value))
		return nil, fmt.Errorf("failed to unmarshal checkpoint data: %w", err)
	}

	log.Debug("Successfully retrieved checkpoint", "checkpointNumber", number, "rootHash", checkpointResp.Result.RootHash)
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

// Reconnect 메서드 추가 - 연결이 끊어진 경우 자동 재연결 시도
func (c *TendermintABCIClient) Reconnect() error {
	// ABCI 연결 재시도
	abciErr := c.connectABCI()
	if abciErr != nil {
		log.Error("Failed to reconnect to ABCI endpoint", "error", abciErr)
		return fmt.Errorf("failed to reconnect to ABCI endpoint: %w", abciErr)
	}

	// RPC 연결 재시도
	rpcErr := c.connectRPC()
	if rpcErr != nil {
		log.Error("Failed to reconnect to RPC endpoint", "error", rpcErr)
		return fmt.Errorf("failed to reconnect to RPC endpoint: %w", rpcErr)
	}

	log.Info("Successfully reconnected to Tendermint endpoints")
	return nil
}

// StartReconnectLoop 메서드 추가 - 백그라운드에서 연결 상태 모니터링 및 재연결 시도
func (c *TendermintABCIClient) StartReconnectLoop() {
	go func() {
		reconnectTicker := time.NewTicker(30 * time.Second)
		defer reconnectTicker.Stop()

		for {
			select {
			case <-c.closeCh:
				return
			case <-reconnectTicker.C:
				if !c.IsConnected() {
					log.Info("Connection to Tendermint lost, attempting to reconnect...")
					if err := c.Reconnect(); err != nil {
						log.Error("Failed to reconnect to Tendermint", "error", err)
					}
				}
			}
		}
	}()
}

// Close 메서드 수정 - 종료 시 안전하게 채널 닫기
func (c *TendermintABCIClient) Close() {
	// closeCh 채널이 두 번 닫히지 않도록 보호
	select {
	case <-c.closeCh:
		// 이미 닫혀있음
		return
	default:
		// 아직 닫히지 않음
	}

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
