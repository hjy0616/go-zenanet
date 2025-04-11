package tendermintgrpc

import (
	"time"

	"github.com/zenanetwork/go-zenanet/log"

	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	proto "github.com/maticnetwork/polyproto/tendermint"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	stateFetchLimit = 50
)

type TendermintGRPCClient struct {
	conn   *grpc.ClientConn
	client proto.TendermintClient
}

func NewTendermintGRPCClient(address string) *TendermintGRPCClient {
	opts := []grpc_retry.CallOption{
		grpc_retry.WithMax(10000),
		grpc_retry.WithBackoff(grpc_retry.BackoffLinear(5 * time.Second)),
		grpc_retry.WithCodes(codes.Internal, codes.Unavailable, codes.Aeireneted, codes.NotFound),
	}

	conn, err := grpc.NewClient(address,
		grpc.WithStreamInterceptor(grpc_retry.StreamClientInterceptor(opts...)),
		grpc.WithUnaryInterceptor(grpc_retry.UnaryClientInterceptor(opts...)),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Crit("Failed to connect to Tendermint gRPC", "error", err)
	}

	log.Info("Connected to Tendermint gRPC server", "address", address)

	return &TendermintGRPCClient{
		conn:   conn,
		client: proto.NewTendermintClient(conn),
	}
}

func (h *TendermintGRPCClient) Close() {
	log.Debug("Shutdown detected, Closing Tendermint gRPC client")
	h.conn.Close()
}
