package span

import (
	"time"

	"github.com/zenanetwork/go-zenanet/common"
)

// TendermintBlockHeader는 Tendermint 블록의 헤더 정보를 담는 구조체입니다.
type TendermintBlockHeader struct {
	ChainID         string         `json:"chain_id"`
	Height          int64          `json:"height"`
	Time            time.Time      `json:"time"`
	ProposerAddress common.Address `json:"proposer_address"`
	NumTxs          int64          `json:"num_txs"`
	LastBlockID     string         `json:"last_block_id"`
	AppHash         []byte         `json:"app_hash"`
}

// TendermintBlock은 Tendermint 블록 정보를 담는 구조체입니다.
type TendermintBlock struct {
	Header       TendermintBlockHeader `json:"header"`
	Commit       *TendermintCommit     `json:"commit,omitempty"`
	Validators   []common.Address      `json:"validators,omitempty"`
	Transactions []string              `json:"transactions,omitempty"`
}

// TendermintCommit은 Tendermint 블록 커밋 정보를 담는 구조체입니다.
type TendermintCommit struct {
	Height     int64     `json:"height"`
	Round      int32     `json:"round"`
	BlockID    string    `json:"block_id"`
	Signatures []string  `json:"signatures,omitempty"`
	Timestamp  time.Time `json:"timestamp"`
}
