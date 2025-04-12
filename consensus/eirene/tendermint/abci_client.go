package tendermint

import (
	"context"
	"crypto/crypto"
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
	"github.com/zenanetwork/go-zenanet/common/hexutil"
	"github.com/zenanetwork/go-zenanet/consensus/eirene/clerk"
	"github.com/zenanetwork/go-zenanet/consensus/eirene/tendermint/checkpoint"
	"github.com/zenanetwork/go-zenanet/consensus/eirene/tendermint/milestone"
	"github.com/zenanetwork/go-zenanet/consensus/eirene/tendermint/span"
	"github.com/zenanetwork/go-zenanet/consensus/eirene/valset"
	"github.com/zenanetwork/go-zenanet/crypto"
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

	autoReconnect     bool          // 자동 재연결 기능 활성화 여부
	reconnectInterval time.Duration // 재연결 시도 간격
	maxRetries        int           // 최대 재시도 횟수

	healthCheckTicker *time.Ticker // 연결 상태 확인용 타이머

	ctx        context.Context    // 클라이언트 컨텍스트
	cancelFunc context.CancelFunc // 컨텍스트 취소 함수
}

// NewTendermintABCIClient creates a new TendermintABCIClient with the given ABCI and RPC addresses
func NewTendermintABCIClient(abciAddr, rpcAddr string) *TendermintABCIClient {
	ctx, cancel := context.WithCancel(context.Background())

	return &TendermintABCIClient{
		abciAddr:          abciAddr,
		rpcAddr:           rpcAddr,
		closeCh:           make(chan struct{}),
		autoReconnect:     true,
		reconnectInterval: 5 * time.Second,
		maxRetries:        10,
		ctx:               ctx,
		cancelFunc:        cancel,
	}
}

// SetAutoReconnect enables or disables automatic reconnection
func (c *TendermintABCIClient) SetAutoReconnect(enable bool) {
	c.autoReconnect = enable
}

// SetReconnectParams sets the reconnection parameters
func (c *TendermintABCIClient) SetReconnectParams(interval time.Duration, maxRetries int) {
	c.reconnectInterval = interval
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

	// 주기적인 연결 상태 확인 시작
	if c.autoReconnect && c.healthCheckTicker == nil {
		c.startHealthCheck()
	}

	return nil
}

// startHealthCheck starts a goroutine that periodically checks the connection health
func (c *TendermintABCIClient) startHealthCheck() {
	c.healthCheckTicker = time.NewTicker(c.reconnectInterval)

	go func() {
		for {
			select {
			case <-c.healthCheckTicker.C:
				c.checkConnectionHealth()
			case <-c.closeCh:
				if c.healthCheckTicker != nil {
					c.healthCheckTicker.Stop()
				}
				return
			}
		}
	}()

	log.Info("Started Tendermint connection health check",
		"interval", c.reconnectInterval,
		"abciAddr", c.abciAddr,
		"rpcAddr", c.rpcAddr)
}

// checkConnectionHealth checks the health of ABCI and RPC connections and attempts to reconnect if needed
func (c *TendermintABCIClient) checkConnectionHealth() {
	c.abciMutex.RLock()
	abciConnected := c.isABCIConnected && c.abciClient != nil && c.abciClient.IsRunning()
	c.abciMutex.RUnlock()

	if !abciConnected {
		log.Warn("ABCI connection lost, attempting to reconnect", "address", c.abciAddr)
		if err := c.connectABCI(); err != nil {
			log.Error("Failed to reconnect to ABCI", "err", err)
		} else {
			log.Info("Successfully reconnected to ABCI", "address", c.abciAddr)
		}
	}

	c.rpcMutex.RLock()
	rpcConnected := c.isRPCConnected && c.rpcClient != nil
	c.rpcMutex.RUnlock()

	// RPC 클라이언트는 IsRunning() 메서드가 없어서 건강 확인을 위해 간단한 요청을 시도
	if rpcConnected {
		ctx, cancel := context.WithTimeout(c.ctx, 2*time.Second)
		defer cancel()

		_, err := c.rpcClient.Health(ctx)
		if err != nil {
			log.Warn("RPC connection health check failed", "err", err)
			rpcConnected = false
		}
	}

	if !rpcConnected {
		log.Warn("RPC connection lost, attempting to reconnect", "address", c.rpcAddr)
		if err := c.connectRPC(); err != nil {
			log.Error("Failed to reconnect to RPC", "err", err)
		} else {
			log.Info("Successfully reconnected to RPC", "address", c.rpcAddr)
		}
	}
}

// connectABCI establishes a connection to the ABCI endpoint
func (c *TendermintABCIClient) connectABCI() error {
	c.abciMutex.Lock()
	defer c.abciMutex.Unlock()

	if c.isABCIConnected && c.abciClient != nil && c.abciClient.IsRunning() {
		return nil
	}

	// 기존 클라이언트가 있으면 중지
	if c.abciClient != nil && c.abciClient.IsRunning() {
		c.abciClient.Stop()
	}

	// 로컬 TCP 소켓을 통해 ABCI 클라이언트 생성
	client := abciclient.NewSocketClient(c.abciAddr, true)

	// 여러 번 연결 시도
	var err error
	for i := 0; i < c.maxRetries; i++ {
		// Start 메서드는 연결 시도를 수행
		err = client.Start()
		if err == nil && client.IsRunning() {
			break
		}

		log.Warn("ABCI connection attempt failed, retrying",
			"attempt", i+1,
			"maxRetries", c.maxRetries,
			"err", err)

		// 다음 시도 전에 잠시 대기
		time.Sleep(dialRetryDelay)
	}

	// 모든 시도 후에도 연결이 안 되면 오류 반환
	if err != nil || !client.IsRunning() {
		if err == nil {
			err = errors.New("ABCI client failed to start after multiple attempts")
		}
		return err
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

	if c.isRPCConnected && c.rpcClient != nil {
		// 간단한 요청으로 연결 상태 확인
		ctx, cancel := context.WithTimeout(c.ctx, 2*time.Second)
		defer cancel()

		_, err := c.rpcClient.Health(ctx)
		if err == nil {
			return nil
		}

		// Health 체크에 실패했지만 클라이언트는 있으면 중지
		if c.rpcClient != nil {
			log.Warn("RPC health check failed, reconnecting", "err", err)
			c.rpcClient.Stop()
		}
	}

	// 여러 번 연결 시도
	var client *tmrpc.HTTP
	var err error

	for i := 0; i < c.maxRetries; i++ {
		// HTTP를 통한 RPC 클라이언트 생성
		client, err = tmrpc.New(c.rpcAddr, "/websocket")
		if err != nil {
			log.Warn("Error creating RPC client, retrying",
				"attempt", i+1,
				"maxRetries", c.maxRetries,
				"err", err)
			time.Sleep(dialRetryDelay)
			continue
		}

		// 시작 시도
		err = client.Start()
		if err == nil {
			// 간단한 요청으로 연결 확인
			ctx, cancel := context.WithTimeout(c.ctx, 2*time.Second)
			_, healthErr := client.Health(ctx)
			cancel()

			if healthErr == nil {
				// 연결 성공
				break
			}

			// Health 체크에 실패하면 중지하고 재시도
			log.Warn("RPC client started but health check failed", "err", healthErr)
			client.Stop()
			err = healthErr
		} else {
			log.Warn("Error starting RPC client, retrying",
				"attempt", i+1,
				"maxRetries", c.maxRetries,
				"err", err)
		}

		time.Sleep(dialRetryDelay)
	}

	// 모든 시도 후에도 연결이 안 되면 오류 반환
	if err != nil {
		return err
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

// InitChain implements the ITendermintClient interface with retry mechanism
func (c *TendermintABCIClient) InitChain(ctx context.Context, req types.RequestInitChain) (*types.ResponseInitChain, error) {
	c.abciMutex.RLock()
	defer c.abciMutex.RUnlock()

	if !c.isABCIConnected {
		return nil, ErrABCINotConnected
	}

	// 첫번째 시도
	abciResp := c.abciClient.InitChainSync(req)
	if abciResp.Error != nil {
		// 연결 오류인 경우 재연결 시도
		if isConnectionError(abciResp.Error) && c.autoReconnect {
			log.Warn("Connection error during InitChain, attempting to reconnect", "err", abciResp.Error)

			// ABCI 연결이 끊어졌으므로 재연결 시도
			c.abciMutex.RUnlock() // 읽기 락 해제
			if reconnectErr := c.connectABCI(); reconnectErr != nil {
				log.Error("Failed to reconnect to ABCI", "err", reconnectErr)
				c.abciMutex.RLock() // 읽기 락 다시 획득
				return nil, fmt.Errorf("init chain failed and reconnect failed: %w", abciResp.Error)
			}
			c.abciMutex.RLock() // 읽기 락 다시 획득

			// 재연결 성공 후 다시 시도
			abciResp = c.abciClient.InitChainSync(req)
			if abciResp.Error != nil {
				return nil, fmt.Errorf("init chain failed after reconnect: %w", abciResp.Error)
			}
		} else {
			return nil, abciResp.Error
		}
	}

	return &abciResp.Response, nil
}

// BeginBlock implements the ITendermintClient interface with retry mechanism
func (c *TendermintABCIClient) BeginBlock(ctx context.Context, req types.RequestBeginBlock) (*types.ResponseBeginBlock, error) {
	c.abciMutex.RLock()
	defer c.abciMutex.RUnlock()

	if !c.isABCIConnected {
		return nil, ErrABCINotConnected
	}

	// 첫번째 시도
	abciResp := c.abciClient.BeginBlockSync(req)
	if abciResp.Error != nil {
		// 연결 오류인 경우 재연결 시도
		if isConnectionError(abciResp.Error) && c.autoReconnect {
			log.Warn("Connection error during BeginBlock, attempting to reconnect", "err", abciResp.Error)

			// ABCI 연결이 끊어졌으므로 재연결 시도
			c.abciMutex.RUnlock() // 읽기 락 해제
			if reconnectErr := c.connectABCI(); reconnectErr != nil {
				log.Error("Failed to reconnect to ABCI", "err", reconnectErr)
				c.abciMutex.RLock() // 읽기 락 다시 획득
				return nil, fmt.Errorf("begin block failed and reconnect failed: %w", abciResp.Error)
			}
			c.abciMutex.RLock() // 읽기 락 다시 획득

			// 재연결 성공 후 다시 시도
			abciResp = c.abciClient.BeginBlockSync(req)
			if abciResp.Error != nil {
				return nil, fmt.Errorf("begin block failed after reconnect: %w", abciResp.Error)
			}
		} else {
			return nil, abciResp.Error
		}
	}

	return &abciResp.Response, nil
}

// CheckTx implements the ITendermintClient interface with retry mechanism
func (c *TendermintABCIClient) CheckTx(ctx context.Context, req types.RequestCheckTx) (*types.ResponseCheckTx, error) {
	c.abciMutex.RLock()
	defer c.abciMutex.RUnlock()

	if !c.isABCIConnected {
		return nil, ErrABCINotConnected
	}

	// 첫번째 시도
	abciResp := c.abciClient.CheckTxSync(req)
	if abciResp.Error != nil {
		// 연결 오류인 경우 재연결 시도
		if isConnectionError(abciResp.Error) && c.autoReconnect {
			log.Warn("Connection error during CheckTx, attempting to reconnect", "err", abciResp.Error)

			// ABCI 연결이 끊어졌으므로 재연결 시도
			c.abciMutex.RUnlock() // 읽기 락 해제
			if reconnectErr := c.connectABCI(); reconnectErr != nil {
				log.Error("Failed to reconnect to ABCI", "err", reconnectErr)
				c.abciMutex.RLock() // 읽기 락 다시 획득
				return nil, fmt.Errorf("check tx failed and reconnect failed: %w", abciResp.Error)
			}
			c.abciMutex.RLock() // 읽기 락 다시 획득

			// 재연결 성공 후 다시 시도
			abciResp = c.abciClient.CheckTxSync(req)
			if abciResp.Error != nil {
				return nil, fmt.Errorf("check tx failed after reconnect: %w", abciResp.Error)
			}
		} else {
			return nil, abciResp.Error
		}
	}

	return &abciResp.Response, nil
}

// DeliverTx implements the ITendermintClient interface with retry mechanism
func (c *TendermintABCIClient) DeliverTx(ctx context.Context, req types.RequestDeliverTx) (*types.ResponseDeliverTx, error) {
	c.abciMutex.RLock()
	defer c.abciMutex.RUnlock()

	if !c.isABCIConnected {
		return nil, ErrABCINotConnected
	}

	// 첫번째 시도
	abciResp := c.abciClient.DeliverTxSync(req)
	if abciResp.Error != nil {
		// 연결 오류인 경우 재연결 시도
		if isConnectionError(abciResp.Error) && c.autoReconnect {
			log.Warn("Connection error during DeliverTx, attempting to reconnect", "err", abciResp.Error)

			// ABCI 연결이 끊어졌으므로 재연결 시도
			c.abciMutex.RUnlock() // 읽기 락 해제
			if reconnectErr := c.connectABCI(); reconnectErr != nil {
				log.Error("Failed to reconnect to ABCI", "err", reconnectErr)
				c.abciMutex.RLock() // 읽기 락 다시 획득
				return nil, fmt.Errorf("deliver tx failed and reconnect failed: %w", abciResp.Error)
			}
			c.abciMutex.RLock() // 읽기 락 다시 획득

			// 재연결 성공 후 다시 시도
			abciResp = c.abciClient.DeliverTxSync(req)
			if abciResp.Error != nil {
				return nil, fmt.Errorf("deliver tx failed after reconnect: %w", abciResp.Error)
			}
		} else {
			return nil, abciResp.Error
		}
	}

	return &abciResp.Response, nil
}

// EndBlock implements the ITendermintClient interface with retry mechanism
func (c *TendermintABCIClient) EndBlock(ctx context.Context, req types.RequestEndBlock) (*types.ResponseEndBlock, error) {
	c.abciMutex.RLock()
	defer c.abciMutex.RUnlock()

	if !c.isABCIConnected {
		return nil, ErrABCINotConnected
	}

	// 첫번째 시도
	abciResp := c.abciClient.EndBlockSync(req)
	if abciResp.Error != nil {
		// 연결 오류인 경우 재연결 시도
		if isConnectionError(abciResp.Error) && c.autoReconnect {
			log.Warn("Connection error during EndBlock, attempting to reconnect", "err", abciResp.Error)

			// ABCI 연결이 끊어졌으므로 재연결 시도
			c.abciMutex.RUnlock() // 읽기 락 해제
			if reconnectErr := c.connectABCI(); reconnectErr != nil {
				log.Error("Failed to reconnect to ABCI", "err", reconnectErr)
				c.abciMutex.RLock() // 읽기 락 다시 획득
				return nil, fmt.Errorf("end block failed and reconnect failed: %w", abciResp.Error)
			}
			c.abciMutex.RLock() // 읽기 락 다시 획득

			// 재연결 성공 후 다시 시도
			abciResp = c.abciClient.EndBlockSync(req)
			if abciResp.Error != nil {
				return nil, fmt.Errorf("end block failed after reconnect: %w", abciResp.Error)
			}
		} else {
			return nil, abciResp.Error
		}
	}

	return &abciResp.Response, nil
}

// Commit implements the ITendermintClient interface with retry mechanism
func (c *TendermintABCIClient) Commit(ctx context.Context) (*types.ResponseCommit, error) {
	c.abciMutex.RLock()
	defer c.abciMutex.RUnlock()

	if !c.isABCIConnected {
		return nil, ErrABCINotConnected
	}

	// 첫번째 시도
	abciResp := c.abciClient.CommitSync()
	if abciResp.Error != nil {
		// 연결 오류인 경우 재연결 시도
		if isConnectionError(abciResp.Error) && c.autoReconnect {
			log.Warn("Connection error during Commit, attempting to reconnect", "err", abciResp.Error)

			// ABCI 연결이 끊어졌으므로 재연결 시도
			c.abciMutex.RUnlock() // 읽기 락 해제
			if reconnectErr := c.connectABCI(); reconnectErr != nil {
				log.Error("Failed to reconnect to ABCI", "err", reconnectErr)
				c.abciMutex.RLock() // 읽기 락 다시 획득
				return nil, fmt.Errorf("commit failed and reconnect failed: %w", abciResp.Error)
			}
			c.abciMutex.RLock() // 읽기 락 다시 획득

			// 재연결 성공 후 다시 시도
			abciResp = c.abciClient.CommitSync()
			if abciResp.Error != nil {
				return nil, fmt.Errorf("commit failed after reconnect: %w", abciResp.Error)
			}
		} else {
			return nil, abciResp.Error
		}
	}

	return &abciResp.Response, nil
}

// GetValidators implements the ITendermintClient interface
func (c *TendermintABCIClient) GetValidators(ctx context.Context) ([]*valset.Validator, error) {
	c.rpcMutex.RLock()
	defer c.rpcMutex.RUnlock()

	if !c.isRPCConnected {
		return nil, ErrRPCNotConnected
	}

	// 컨텍스트에 타임아웃 설정
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// Tendermint RPC를 통해 검증자 목록 가져오기
	validatorsResp, err := c.rpcClient.Validators(ctx, nil, nil, nil)
	if err != nil {
		// 오류가 연결 관련 문제인지 확인하고 필요시 재연결 시도
		if isConnectionError(err) && c.autoReconnect {
			log.Warn("Connection error during validators query, attempting to reconnect", "err", err)

			// RPC 연결이 끊어졌으므로 재연결 시도
			c.rpcMutex.RUnlock() // 읽기 락 해제
			if reconnectErr := c.connectRPC(); reconnectErr != nil {
				log.Error("Failed to reconnect to RPC", "err", reconnectErr)
				c.rpcMutex.RLock() // 읽기 락 다시 획득
				return nil, fmt.Errorf("failed to fetch validators and reconnect failed: %w", err)
			}
			c.rpcMutex.RLock() // 읽기 락 다시 획득

			// 재연결 성공 후 다시 시도
			validatorsResp, err = c.rpcClient.Validators(ctx, nil, nil, nil)
			if err != nil {
				return nil, fmt.Errorf("failed to fetch validators after reconnect: %w", err)
			}
		} else {
			return nil, fmt.Errorf("failed to fetch validators: %w", err)
		}
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

		// ID는 주소의 해시에서 추출
		idBytes := crypto.Keccak256(ethAddr[:])
		if len(idBytes) >= 8 {
			validator.ID = binary.BigEndian.Uint64(idBytes[:8])
		}

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

	// 컨텍스트에 타임아웃 설정
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second) // 더 긴 타임아웃 설정 (이벤트 검색에 시간이 더 걸릴 수 있음)
	defer cancel()

	// Tendermint RPC를 통해 이벤트 조회
	// 여기서는 tx_search를 사용하여 특정 타입의 이벤트를 조회합니다
	query := fmt.Sprintf("state_sync.from_id >= %d AND state_sync.to_block <= %d", fromID, to)

	var searchResult *tmrpc.ResultTxSearch
	var err error

	// 검색 시도
	searchResult, err = c.rpcClient.TxSearch(ctx, query, false, nil, nil, "asc")
	if err != nil {
		// 연결 문제인 경우 재연결 시도
		if isConnectionError(err) && c.autoReconnect {
			log.Warn("Connection error during state sync events query, attempting to reconnect", "err", err)

			// RPC 연결이 끊어졌으므로 재연결 시도
			c.rpcMutex.RUnlock() // 읽기 락 해제
			if reconnectErr := c.connectRPC(); reconnectErr != nil {
				log.Error("Failed to reconnect to RPC", "err", reconnectErr)
				c.rpcMutex.RLock() // 읽기 락 다시 획득
				return nil, fmt.Errorf("failed to search state sync events and reconnect failed: %w", err)
			}
			c.rpcMutex.RLock() // 읽기 락 다시 획득

			// 재연결 성공 후 다시 시도
			searchResult, err = c.rpcClient.TxSearch(ctx, query, false, nil, nil, "asc")
			if err != nil {
				return nil, fmt.Errorf("failed to search state sync events after reconnect: %w", err)
			}
		} else {
			return nil, fmt.Errorf("failed to search state sync events: %w", err)
		}
	}

	// 결과가 없는 경우 빈 슬라이스 반환
	if len(searchResult.Txs) == 0 {
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
			var stateID uint64
			var recordTime time.Time
			var data []byte
			var contract common.Address
			var txHash common.Hash
			var logIndex uint64
			var chainID string

			for _, attr := range event.Attributes {
				key := string(attr.Key)
				value := attr.Value

				switch key {
				case "state_id":
					stateID, _ = strconv.ParseUint(string(value), 10, 64)
				case "time":
					timeInt, _ := strconv.ParseInt(string(value), 10, 64)
					recordTime = time.Unix(timeInt, 0)
				case "data":
					data = value
				case "contract":
					contractAddr, err := hexutil.Decode(string(value))
					if err == nil && len(contractAddr) == common.AddressLength {
						copy(contract[:], contractAddr)
					}
				case "tx_hash":
					hashBytes, err := hexutil.Decode(string(value))
					if err == nil && len(hashBytes) == common.HashLength {
						copy(txHash[:], hashBytes)
					}
				case "log_index":
					logIndex, _ = strconv.ParseUint(string(value), 10, 64)
				case "chain_id":
					chainID = string(value)
				}
			}

			// EventRecordWithTime 생성 및 추가
			record := &clerk.EventRecord{
				ID:       stateID,
				Contract: contract,
				Data:     data,
				TxHash:   txHash,
				LogIndex: logIndex,
				ChainID:  chainID,
			}

			eventWithTime := &clerk.EventRecordWithTime{
				EventRecord: *record,
				Time:        recordTime,
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

	// 컨텍스트에 타임아웃 설정
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// Tendermint ABCI Query를 사용하여 스팬 정보 조회
	path := fmt.Sprintf("span/%d", spanID)

	var queryResult *tmrpc.ABCIQueryResponse
	var err error

	// 쿼리 실행 및 가능한 연결 오류 처리
	queryResult, err = c.executeQueryWithRetry(ctx, path, nil)
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

	// 컨텍스트에 타임아웃 설정
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// Tendermint ABCI Query를 사용하여 체크포인트 조회
	path := fmt.Sprintf("checkpoint/%d", number)

	var queryResult *tmrpc.ABCIQueryResponse
	var err error

	// 쿼리 실행 및 가능한 연결 오류 처리
	queryResult, err = c.executeQueryWithRetry(ctx, path, nil)
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

// executeQueryWithRetry executes an ABCI query with automatic retry on connection errors
func (c *TendermintABCIClient) executeQueryWithRetry(ctx context.Context, path string, data []byte) (*tmrpc.ABCIQueryResponse, error) {
	var queryResult *tmrpc.ABCIQueryResponse
	var err error

	// 첫번째 시도
	queryResult, err = c.rpcClient.ABCIQuery(ctx, path, data)
	if err != nil && isConnectionError(err) && c.autoReconnect {
		log.Warn("Connection error during ABCI query, attempting to reconnect", "path", path, "err", err)

		// RPC 연결이 끊어졌으므로 재연결 시도
		c.rpcMutex.RUnlock() // 읽기 락 해제
		if reconnectErr := c.connectRPC(); reconnectErr != nil {
			log.Error("Failed to reconnect to RPC", "err", reconnectErr)
			c.rpcMutex.RLock() // 읽기 락 다시 획득
			return nil, fmt.Errorf("query failed and reconnect failed: %w", err)
		}
		c.rpcMutex.RLock() // 읽기 락 다시 획득

		// 재연결 성공 후 다시 시도
		queryResult, err = c.rpcClient.ABCIQuery(ctx, path, data)
		if err != nil {
			return nil, fmt.Errorf("query failed after reconnect: %w", err)
		}
	} else if err != nil {
		return nil, err
	}

	return queryResult, nil
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
	// health check 중지
	if c.healthCheckTicker != nil {
		c.healthCheckTicker.Stop()
		c.healthCheckTicker = nil
	}

	// 컨텍스트 취소
	c.cancelFunc()

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

// isConnectionError checks if the given error is related to connection issues
func isConnectionError(err error) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()
	// 연결 관련 오류를 나타내는 문자열들
	connectionErrors := []string{
		"connection refused",
		"connection reset",
		"broken pipe",
		"i/o timeout",
		"no connection",
		"connection closed",
		"EOF",
		"context deadline exceeded",
		"dial tcp",
	}

	for _, connErr := range connectionErrors {
		if strings.Contains(strings.ToLower(errStr), strings.ToLower(connErr)) {
			return true
		}
	}

	return false
}
