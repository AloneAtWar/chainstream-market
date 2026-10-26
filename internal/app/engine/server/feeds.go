package server

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/AloneAtWar/chainstream-market/internal/app/engine/biz"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/chainlink"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/config"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/rpc"
)

// ResolveFeeds turns a chain's configured feeds into trusted aggregators.
// A proxy is read over the chain's rpc_url for the aggregator of every phase,
// and its decimals and description are checked against the config.
func ResolveFeeds(ctx context.Context, chain *config.Chain, logger *slog.Logger) ([]biz.Feed, error) {
	var client *rpc.Client
	proxies := make(map[string]chainlink.Proxy)
	var feeds []biz.Feed
	for _, feed := range chain.GetFeeds() {
		decimals := uint8(feed.GetDecimals())
		if feed.GetProxy() == "" {
			feeds = append(feeds, biz.Feed{Address: feed.GetAddress(), Token: feed.GetToken(), Decimals: decimals})
			continue
		}
		address := strings.ToLower(strings.TrimSpace(feed.GetProxy()))
		proxy, ok := proxies[address]
		if !ok {
			var err error
			if client == nil {
				if client, err = rpc.New(rpc.Options{
					URL: chain.GetRpcUrl(), ChainID: chain.GetChainId(), Chain: chain.GetName(), Logger: logger,
				}); err != nil {
					return nil, err
				}
			}
			if proxy, err = chainlink.Resolve(ctx, client, address); err != nil {
				return nil, err
			}
			proxies[address] = proxy
			logger.Info("feed proxy resolved", "chain", chain.GetName(), "proxy", address,
				"description", proxy.Description, "decimals", proxy.Decimals, "aggregators", proxy.Aggregators)
		}
		if decimals != 0 && decimals != proxy.Decimals {
			return nil, fmt.Errorf("feed proxy %s has %d decimals, config says %d", address, proxy.Decimals, decimals)
		}
		if want := strings.TrimSpace(feed.GetDescription()); want != "" && !strings.EqualFold(want, strings.TrimSpace(proxy.Description)) {
			return nil, fmt.Errorf("feed proxy %s is %q, config says %q", address, proxy.Description, want)
		}
		for _, aggregator := range proxy.Aggregators {
			feeds = append(feeds, biz.Feed{Address: aggregator, Token: feed.GetToken(), Decimals: proxy.Decimals})
		}
	}
	return feeds, nil
}
