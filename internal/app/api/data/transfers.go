package data

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/AloneAtWar/chainstream-market/internal/app/api/service"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/chdb"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	"gorm.io/gorm"
)

// transferReader reads ch-loader's raw_transfers.
type transferReader struct {
	db *gorm.DB
}

// NewTransferReader reads ch-loader's transfers.
func NewTransferReader(data *Data) service.TransferReader { return &transferReader{db: data.market} }

// Transfers returns matching transfers newest first.
func (reader *transferReader) Transfers(ctx context.Context, query model.TransferQuery) ([]model.Transfer, error) {
	if query.Limit <= 0 || (query.Token == "" && query.Holder == "") {
		return nil, errors.New("clickhouse transfers: transfer query needs a limit and a token or holder")
	}
	filters := []string{"chain_id = ?"}
	args := []any{query.ChainID}
	if query.Token != "" {
		filters = append(filters, "token = ?")
		args = append(args, query.Token)
	}
	if query.Holder != "" {
		// Matches the raw_transfers_party index expression exactly.
		filters = append(filters, "has([from_address, to_address], ?)")
		args = append(args, query.Holder)
	}
	switch {
	case query.BeforeHeight > 0 && query.BeforeLogIndex != nil:
		filters = append(filters, "(block_height < ? OR (block_height = ? AND log_index < ?))")
		args = append(args, query.BeforeHeight, query.BeforeHeight, *query.BeforeLogIndex)
	case query.BeforeHeight > 0:
		filters = append(filters, "block_height < ?")
		args = append(args, query.BeforeHeight)
	}
	var rows []chdb.TransferRow
	err := reader.db.WithContext(ctx).Select(`block_height, block_time, transaction_hash, transaction_index,
		log_index, token, from_address, to_address, amount`).
		Where(strings.Join(filters, " AND "), args...).
		Order("block_height DESC, log_index DESC").Limit(query.Limit).Find(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("clickhouse transfers: query transfers: %w", err)
	}
	if len(rows) == 0 {
		return nil, nil
	}
	transfers := make([]model.Transfer, len(rows))
	for index, row := range rows {
		transfers[index] = model.Transfer{BlockHeight: row.BlockHeight, BlockTime: row.BlockTime.UTC(), RawTransfer: model.RawTransfer{
			TransactionHash: row.TransactionHash, TransactionIndex: row.TransactionIndex, LogIndex: row.LogIndex,
			Token: row.Token, From: row.FromAddress, To: row.ToAddress, Amount: row.Amount,
		}}
	}
	return transfers, nil
}
