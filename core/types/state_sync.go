// Package types contains data types related to Ethereum consensus.
package types

import (
	"github.com/zenanetwork/go-zenanet/common"
)

// StateSyncData represents the data needed for state synchronization
type StateSyncData struct {
	ID       uint64         // 상태 식별자
	Contract common.Address // 계약 주소
	Data     string         // 헥스로 인코딩된 데이터
	TxHash   common.Hash    // 트랜잭션 해시
	Root     common.Hash    // 상태 루트
}
