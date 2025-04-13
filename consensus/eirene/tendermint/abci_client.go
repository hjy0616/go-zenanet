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
	"github.com/zenanetwork/go-zenanet/consensus/eirene"
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

	// Tendermint RPC를 통해 검증자 목록 가져오기
	validatorsResp, err := c.rpcClient.Validators(ctx, nil, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch validators: %w", err)
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

// StateSyncEvents는 특정 ID부터 지정된 블록까지의 상태 동기화 이벤트를 검색합니다.
func (c *TendermintABCIClient) StateSyncEvents(ctx context.Context, fromID uint64, to int64) ([]*clerk.EventRecordWithTime, error) {
	c.rpcMutex.RLock()
	defer c.rpcMutex.RUnlock()

	if !c.isRPCConnected {
		return nil, ErrRPCNotConnected
	}

	// Tendermint RPC를 통해 이벤트 조회
	// 여기서는 tx_search를 사용하여 특정 타입의 이벤트를 조회합니다
	query := fmt.Sprintf("state_sync.from_id >= %d AND state_sync.to_block <= %d", fromID, to)
	searchResult, err := c.rpcClient.TxSearch(ctx, query, false, nil, nil, "asc")
	if err != nil {
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

	return events, nil
}

// Span는 지정된 spanID에 해당하는 Tendermint 스팬 정보를 가져옵니다.
func (c *TendermintABCIClient) Span(ctx context.Context, spanID uint64) (*span.TendermintSpan, error) {
	c.rpcMutex.RLock()
	defer c.rpcMutex.RUnlock()

	if !c.isRPCConnected {
		return nil, ErrRPCNotConnected
	}

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
	maxRetries := 3
	var err error

	for i := 0; i <= maxRetries; i++ {
		if i > 0 {
			log.Warn("Retrying to fetch checkpoint count", "attempt", i)
		}

		// 연결 상태 확인 및 재연결 시도
		if err = c.ReconnectIfNeeded(); err != nil {
			log.Error("Failed to reconnect", "err", err)
			continue
		}

		c.rpcMutex.RLock()
		if !c.isRPCConnected {
			c.rpcMutex.RUnlock()
			err = ErrRPCNotConnected
			continue
		}

		// Tendermint ABCI Query를 사용하여 체크포인트 개수 조회
		path := "checkpoint/count"
		queryResult, queryErr := c.rpcClient.ABCIQuery(ctx, path, nil)
		c.rpcMutex.RUnlock()

		if queryErr != nil {
			err = fmt.Errorf("failed to query checkpoint count: %w", queryErr)
			continue
		}

		if queryResult.Response.Code != 0 {
			err = fmt.Errorf("query failed with code %d: %s",
				queryResult.Response.Code, queryResult.Response.Log)
			continue
		}

		// 응답 데이터가 없는 경우
		if len(queryResult.Response.Value) == 0 {
			err = fmt.Errorf("checkpoint count not found")
			continue
		}

		// 응답 데이터를 CheckpointCount 응답으로 언마샬
		var countResp checkpoint.CheckpointCountResponse
		if unmarshalErr := json.Unmarshal(queryResult.Response.Value, &countResp); unmarshalErr != nil {
			err = fmt.Errorf("failed to unmarshal checkpoint count data: %w", unmarshalErr)
			continue
		}

		return countResp.Result.Result, nil
	}

	return 0, fmt.Errorf("failed to fetch checkpoint count after %d attempts: %w", maxRetries, err)
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
	maxRetries := 3
	var err error

	for i := 0; i <= maxRetries; i++ {
		if i > 0 {
			log.Warn("Retrying to fetch milestone count", "attempt", i)
		}

		// 연결 상태 확인 및 재연결 시도
		if err = c.ReconnectIfNeeded(); err != nil {
			log.Error("Failed to reconnect", "err", err)
			continue
		}

		c.rpcMutex.RLock()
		if !c.isRPCConnected {
			c.rpcMutex.RUnlock()
			err = ErrRPCNotConnected
			continue
		}

		// Tendermint ABCI Query를 사용하여 마일스톤 개수 조회
		path := "milestone/count"
		queryResult, queryErr := c.rpcClient.ABCIQuery(ctx, path, nil)
		c.rpcMutex.RUnlock()

		if queryErr != nil {
			err = fmt.Errorf("failed to query milestone count: %w", queryErr)
			continue
		}

		if queryResult.Response.Code != 0 {
			err = fmt.Errorf("query failed with code %d: %s",
				queryResult.Response.Code, queryResult.Response.Log)
			continue
		}

		// 응답 데이터가 없는 경우
		if len(queryResult.Response.Value) == 0 {
			err = fmt.Errorf("milestone count not found")
			continue
		}

		// 응답 데이터를 MilestoneCount 응답으로 언마샬
		var countResp milestone.MilestoneCountResponse
		if unmarshalErr := json.Unmarshal(queryResult.Response.Value, &countResp); unmarshalErr != nil {
			err = fmt.Errorf("failed to unmarshal milestone count data: %w", unmarshalErr)
			continue
		}

		return countResp.Result.Count, nil
	}

	return 0, fmt.Errorf("failed to fetch milestone count after %d attempts: %w", maxRetries, err)
}

// FetchNoAckMilestone는 특정 ID의 마일스톤이 Tendermint에서 실패했는지 여부를 조회합니다.
func (c *TendermintABCIClient) FetchNoAckMilestone(ctx context.Context, milestoneID string) error {
	maxRetries := 3
	var err error

	for i := 0; i <= maxRetries; i++ {
		if i > 0 {
			log.Warn("Retrying to fetch no ack milestone", "attempt", i, "milestoneID", milestoneID)
		}

		// 연결 상태 확인 및 재연결 시도
		if err = c.ReconnectIfNeeded(); err != nil {
			log.Error("Failed to reconnect", "err", err)
			continue
		}

		c.rpcMutex.RLock()
		if !c.isRPCConnected {
			c.rpcMutex.RUnlock()
			err = ErrRPCNotConnected
			continue
		}

		// Tendermint ABCI Query를 사용하여 실패한 마일스톤 조회
		path := fmt.Sprintf("milestone/no-ack/%s", milestoneID)
		queryResult, queryErr := c.rpcClient.ABCIQuery(ctx, path, nil)
		c.rpcMutex.RUnlock()

		if queryErr != nil {
			err = fmt.Errorf("failed to query no-ack milestone: %w", queryErr)
			continue
		}

		if queryResult.Response.Code != 0 {
			err = fmt.Errorf("query failed with code %d: %s",
				queryResult.Response.Code, queryResult.Response.Log)
			continue
		}

		// 응답 데이터가 없는 경우는 마일스톤이 실패하지 않았음을 의미
		if len(queryResult.Response.Value) == 0 {
			return nil
		}

		// 응답 데이터를 boolean으로 언마샬
		var isFailed bool
		if unmarshalErr := json.Unmarshal(queryResult.Response.Value, &isFailed); unmarshalErr != nil {
			err = fmt.Errorf("failed to unmarshal no-ack milestone data: %w", unmarshalErr)
			continue
		}

		// isFailed가 true이면 마일스톤이 실패한 것
		if isFailed {
			return fmt.Errorf("milestone %s failed in Tendermint", milestoneID)
		}

		return nil
	}

	return fmt.Errorf("failed to fetch no-ack milestone after %d attempts: %w", maxRetries, err)
}

// FetchLastNoAckMilestone는 가장 최근에 실패한 마일스톤 ID를 조회합니다.
func (c *TendermintABCIClient) FetchLastNoAckMilestone(ctx context.Context) (string, error) {
	maxRetries := 3
	var err error

	for i := 0; i <= maxRetries; i++ {
		if i > 0 {
			log.Warn("Retrying to fetch last no-ack milestone", "attempt", i)
		}

		// 연결 상태 확인 및 재연결 시도
		if err = c.ReconnectIfNeeded(); err != nil {
			log.Error("Failed to reconnect", "err", err)
			continue
		}

		c.rpcMutex.RLock()
		if !c.isRPCConnected {
			c.rpcMutex.RUnlock()
			err = ErrRPCNotConnected
			continue
		}

		// Tendermint ABCI Query를 사용하여 마지막으로 실패한 마일스톤 조회
		path := "milestone/last-no-ack"
		queryResult, queryErr := c.rpcClient.ABCIQuery(ctx, path, nil)
		c.rpcMutex.RUnlock()

		if queryErr != nil {
			err = fmt.Errorf("failed to query last no-ack milestone: %w", queryErr)
			continue
		}

		if queryResult.Response.Code != 0 {
			err = fmt.Errorf("query failed with code %d: %s",
				queryResult.Response.Code, queryResult.Response.Log)
			continue
		}

		// 응답 데이터가 없는 경우는 실패한 마일스톤이 없음을 의미
		if len(queryResult.Response.Value) == 0 {
			return "", nil
		}

		// 응답 데이터에서 마일스톤 ID 언마샬
		var milestoneID string
		if unmarshalErr := json.Unmarshal(queryResult.Response.Value, &milestoneID); unmarshalErr != nil {
			err = fmt.Errorf("failed to unmarshal last no-ack milestone data: %w", unmarshalErr)
			continue
		}

		return milestoneID, nil
	}

	return "", fmt.Errorf("failed to fetch last no-ack milestone after %d attempts: %w", maxRetries, err)
}

// FetchMilestoneID는 특정 ID의 마일스톤이 Tendermint에서 처리 중인지 여부를 조회합니다.
func (c *TendermintABCIClient) FetchMilestoneID(ctx context.Context, milestoneID string) error {
	maxRetries := 3
	var err error

	for i := 0; i <= maxRetries; i++ {
		if i > 0 {
			log.Warn("Retrying to fetch milestone ID", "attempt", i, "milestoneID", milestoneID)
		}

		// 연결 상태 확인 및 재연결 시도
		if err = c.ReconnectIfNeeded(); err != nil {
			log.Error("Failed to reconnect", "err", err)
			continue
		}

		c.rpcMutex.RLock()
		if !c.isRPCConnected {
			c.rpcMutex.RUnlock()
			err = ErrRPCNotConnected
			continue
		}

		// Tendermint ABCI Query를 사용하여 마일스톤 처리 상태 조회
		path := fmt.Sprintf("milestone/process/%s", milestoneID)
		queryResult, queryErr := c.rpcClient.ABCIQuery(ctx, path, nil)
		c.rpcMutex.RUnlock()

		if queryErr != nil {
			err = fmt.Errorf("failed to query milestone process status: %w", queryErr)
			continue
		}

		if queryResult.Response.Code != 0 {
			err = fmt.Errorf("query failed with code %d: %s",
				queryResult.Response.Code, queryResult.Response.Log)
			continue
		}

		// 응답 데이터가 없는 경우는 마일스톤이 처리 중이 아님을 의미
		if len(queryResult.Response.Value) == 0 {
			return fmt.Errorf("milestone %s is not in process", milestoneID)
		}

		// 응답 데이터를 boolean으로 언마샬
		var isInProcess bool
		if unmarshalErr := json.Unmarshal(queryResult.Response.Value, &isInProcess); unmarshalErr != nil {
			err = fmt.Errorf("failed to unmarshal milestone process data: %w", unmarshalErr)
			continue
		}

		// isInProcess가 false이면 마일스톤이 처리 중이 아님
		if !isInProcess {
			return fmt.Errorf("milestone %s is not in process", milestoneID)
		}

		return nil
	}

	return fmt.Errorf("failed to fetch milestone process status after %d attempts: %w", maxRetries, err)
}

// BlockInfo는 특정 높이의 블록 정보를 조회합니다.
func (c *TendermintABCIClient) BlockInfo(ctx context.Context, height *int64) (*eirene.BlockInfo, error) {
	maxRetries := 3
	var err error

	for i := 0; i <= maxRetries; i++ {
		if i > 0 {
			log.Warn("Retrying to fetch block info", "attempt", i, "height", height)
		}

		// 연결 상태 확인 및 재연결 시도
		if err = c.ReconnectIfNeeded(); err != nil {
			log.Error("Failed to reconnect", "err", err)
			continue
		}

		c.rpcMutex.RLock()
		if !c.isRPCConnected {
			c.rpcMutex.RUnlock()
			err = ErrRPCNotConnected
			continue
		}

		// Tendermint RPC를 사용하여 블록 정보 조회
		var blockData map[string]interface{}
		var queryErr error

		if height == nil {
			// 높이가 지정되지 않으면 최신 블록 조회
			blockResp, blockErr := c.rpcClient.Call(ctx, "block", map[string]interface{}{})
			queryErr = blockErr
			if blockErr == nil && blockResp.Result != nil {
				blockData = blockResp.Result.(map[string]interface{})
			}
		} else {
			// 지정된 높이의 블록 조회
			blockResp, blockErr := c.rpcClient.Call(ctx, "block", map[string]interface{}{"height": *height})
			queryErr = blockErr
			if blockErr == nil && blockResp.Result != nil {
				blockData = blockResp.Result.(map[string]interface{})
			}
		}

		c.rpcMutex.RUnlock()

		if queryErr != nil {
			err = fmt.Errorf("failed to query block info: %w", queryErr)
			continue
		}

		// 결과가 없는 경우
		if blockData == nil {
			err = fmt.Errorf("no block found for height %v", height)
			continue
		}

		// JSON 응답에서 필요한 데이터 추출
		blockInfo := &eirene.BlockInfo{}
		// JSON 파싱 로직 구현
		// 이 부분은 실제 응답 구조에 맞게 구현해야 합니다

		// 간략한 구현 예시
		if blockIDData, ok := blockData["block_id"].(map[string]interface{}); ok {
			if hash, ok := blockIDData["hash"].([]byte); ok {
				blockInfo.BlockID.Hash = hash
			}
		}

		if blockData, ok := blockData["block"].(map[string]interface{}); ok {
			if headerData, ok := blockData["header"].(map[string]interface{}); ok {
				if height, ok := headerData["height"].(int64); ok {
					blockInfo.Header.Height = height
				}
				if chainID, ok := headerData["chain_id"].(string); ok {
					blockInfo.Header.ChainID = chainID
				}
				// 기타 필드 파싱...
			}
		}

		return blockInfo, nil
	}

	return nil, fmt.Errorf("failed to fetch block info after %d attempts: %w", maxRetries, err)
}

// ReconnectIfNeeded는 필요한 경우 ABCI 및 RPC 클라이언트를 재연결합니다.
func (c *TendermintABCIClient) ReconnectIfNeeded() error {
	c.abciMutex.RLock()
	abciConnected := c.isABCIConnected
	c.abciMutex.RUnlock()

	c.rpcMutex.RLock()
	rpcConnected := c.isRPCConnected
	c.rpcMutex.RUnlock()

	// 둘 다 연결되어 있으면 재연결 필요 없음
	if abciConnected && rpcConnected {
		return nil
	}

	// ABCI가 연결되어 있지 않으면 재연결 시도
	if !abciConnected {
		if err := c.connectABCI(); err != nil {
			return fmt.Errorf("failed to reconnect ABCI client: %w", err)
		}
	}

	// RPC가 연결되어 있지 않으면 재연결 시도
	if !rpcConnected {
		if err := c.connectRPC(); err != nil {
			return fmt.Errorf("failed to reconnect RPC client: %w", err)
		}
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
