package tendermintapp

import (
	"github.com/cosmos/cosmos-sdk/types"

	"github.com/zenanetwork/go-zenanet/log"

	"github.com/maticnetwork/tendermint/app"
	"github.com/maticnetwork/tendermint/cmd/tendermintd/service"

	abci "github.com/tendermint/tendermint/abci/types"
)

const (
	stateFetchLimit = 50
)

type TendermintAppClient struct {
	hApp *app.TendermintApp
}

func NewTendermintAppClient() *TendermintAppClient {
	return &TendermintAppClient{
		hApp: service.GetTendermintApp(),
	}
}

func (h *TendermintAppClient) Close() {
	// Nothing to close as of now
	log.Warn("Shutdown detected, Closing Tendermint App conn")
}

func (h *TendermintAppClient) NewContext() types.Context {
	return h.hApp.NewContext(true, abci.Header{Height: h.hApp.LastBlockHeight()})
}
