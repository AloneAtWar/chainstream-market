package biz_test

import (
	"github.com/AloneAtWar/chainstream-market/internal/app/engine/biz"
	"github.com/AloneAtWar/chainstream-market/internal/app/engine/data"
)

func init() {
	biz.SetTestStores(biz.TestStores{
		Output: data.NewOutputRepo, State: data.NewStateRepo, History: data.NewHistoryRepo,
	})
}
