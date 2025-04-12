package tendermint

import (
	"fmt"

	"github.com/zenanetwork/go-zenanet/consensus/eirene"
	"github.com/zenanetwork/go-zenanet/log"
)

// ClientType represents the type of Tendermint client to use
type ClientType int

const (
	// HTTPClient represents the HTTP-based Tendermint client
	HTTPClient ClientType = iota
	// ABCIClient represents the ABCI-based Tendermint client
	ABCIClient
	// IntegratedClient represents the integrated Tendermint client
	IntegratedClient
)

// CreateTendermintClient creates a Tendermint client of the specified type
func CreateTendermintClient(clientType ClientType, config *Config) (eirene.ITendermintClient, error) {
	switch clientType {
	case HTTPClient:
		log.Info("Creating HTTP-based Tendermint client (via IntegratedClient)", "url", config.HttpURL)
		// HTTP 클라이언트 대신 IntegratedClient 사용 - ABCI 메서드를 지원하는 클라이언트
		client := NewIntegratedTendermintClient(config.HttpURL, "", "")
		return client, nil
	case ABCIClient:
		log.Info("Creating ABCI-based Tendermint client", "abci", config.ABCIAddress, "rpc", config.RPCAddress)
		client := NewTendermintABCIClient(config.ABCIAddress, config.RPCAddress)
		if err := client.Connect(); err != nil {
			return nil, fmt.Errorf("failed to connect to Tendermint ABCI: %w", err)
		}
		return client, nil
	case IntegratedClient:
		log.Info("Creating integrated Tendermint client", "http", config.HttpURL, "abci", config.ABCIAddress, "rpc", config.RPCAddress)
		client := NewIntegratedTendermintClient(config.HttpURL, config.ABCIAddress, config.RPCAddress)
		if err := client.Connect(); err != nil {
			log.Warn("Failed to connect to Tendermint ABCI, will continue with HTTP only", "error", err)
		}
		return client, nil
	default:
		return nil, fmt.Errorf("unknown client type: %d", clientType)
	}
}

// Config holds the configuration for Tendermint clients
type Config struct {
	// HttpURL is the URL for the HTTP-based Tendermint client
	HttpURL string
	// ABCIAddress is the address for the ABCI client
	ABCIAddress string
	// RPCAddress is the address for the RPC client
	RPCAddress string
}

// NewConfig creates a new Config with the given URLs
func NewConfig(httpURL, abciAddress, rpcAddress string) *Config {
	return &Config{
		HttpURL:     httpURL,
		ABCIAddress: abciAddress,
		RPCAddress:  rpcAddress,
	}
}

// DefaultConfig returns a default configuration
func DefaultConfig() *Config {
	return &Config{
		HttpURL:     "http://localhost:26657",
		ABCIAddress: "tcp://localhost:26658",
		RPCAddress:  "http://localhost:26657",
	}
}

// Factory creates and manages Tendermint integration components
type Factory struct {
	client        eirene.ITendermintClient
	validatorSync *ValidatorSyncer
	spanManager   *SpanManager
}

// FactoryConfig contains configuration parameters for the factory
type FactoryConfig struct {
	ABCIAddress   string
	RPCAddress    string
	SpanDuration  uint64
	ProducerCount int
}

// DefaultFactoryConfig returns the default factory configuration
func DefaultFactoryConfig() *FactoryConfig {
	return &FactoryConfig{
		ABCIAddress:   "tcp://localhost:26658",
		RPCAddress:    "http://localhost:26657",
		SpanDuration:  100,
		ProducerCount: 10,
	}
}

// NewFactory creates a new Tendermint integration factory
func NewFactory(config *FactoryConfig) (*Factory, error) {
	if config == nil {
		config = DefaultFactoryConfig()
	}

	// Tendermint ABCI 클라이언트 생성
	client := NewTendermintABCIClient(config.ABCIAddress, config.RPCAddress)

	// 연결 시도
	if err := client.Connect(); err != nil {
		return nil, fmt.Errorf("failed to connect to Tendermint: %w", err)
	}

	// 검증자 동기화 모듈 생성
	validatorSync := NewValidatorSyncer(client)

	// 스팬 관리자 생성
	spanManager := NewSpanManager(
		client,
		validatorSync,
		WithSpanDuration(config.SpanDuration),
		WithProducerCount(config.ProducerCount),
	)

	return &Factory{
		client:        client,
		validatorSync: validatorSync,
		spanManager:   spanManager,
	}, nil
}

// Start initializes and starts all Tendermint integration components
func (f *Factory) Start() error {
	// 검증자 동기화 모듈 시작
	if err := f.validatorSync.Start(); err != nil {
		return fmt.Errorf("failed to start validator syncer: %w", err)
	}

	// 스팬 관리자 시작
	if err := f.spanManager.Start(); err != nil {
		return fmt.Errorf("failed to start span manager: %w", err)
	}

	log.Info("Tendermint integration factory started")
	return nil
}

// Stop halts all Tendermint integration components
func (f *Factory) Stop() {
	// 스팬 관리자 중지
	f.spanManager.Stop()

	// 검증자 동기화 모듈 중지
	f.validatorSync.Stop()

	// ABCI 클라이언트 연결 종료
	f.client.Close()

	log.Info("Tendermint integration factory stopped")
}

// GetClient returns the Tendermint ABCI client
func (f *Factory) GetClient() eirene.ITendermintClient {
	return f.client
}

// GetValidatorSyncer returns the validator synchronization module
func (f *Factory) GetValidatorSyncer() *ValidatorSyncer {
	return f.validatorSync
}

// GetSpanManager returns the span manager
func (f *Factory) GetSpanManager() *SpanManager {
	return f.spanManager
}

// IsConnected returns whether the Tendermint client is connected
func (f *Factory) IsConnected() bool {
	return f.client.IsConnected()
}

// ForceSync forces an immediate synchronization of validators
func (f *Factory) ForceSync() error {
	return f.validatorSync.ForceSync()
}
