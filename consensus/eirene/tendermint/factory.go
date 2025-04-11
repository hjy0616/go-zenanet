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
