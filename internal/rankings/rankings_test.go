package rankings

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/AloneAtWar/chainstream-market/internal/storage"
)

type fakeSource struct {
	head          time.Time
	hasHead       bool
	err           error
	quoteSince    time.Time
	quoteTokens   []string
	activitySince time.Time
	interval      time.Duration
}

func (fake *fakeSource) ChainHead(context.Context, uint64) (time.Time, bool, error) {
	return fake.head, fake.hasHead, fake.err
}

func (fake *fakeSource) TokenActivity(_ context.Context, _ uint64, interval time.Duration, since time.Time) ([]storage.TokenActivity, error) {
	fake.interval, fake.activitySince = interval, since
	return []storage.TokenActivity{{Token: "0xa"}}, nil
}

func (fake *fakeSource) LatestQuotes(_ context.Context, _ uint64, since time.Time, tokens []string) ([]storage.TokenQuote, error) {
	fake.quoteSince, fake.quoteTokens = since, tokens
	return []storage.TokenQuote{{Token: "0xa"}}, nil
}

type fakeStore struct {
	mu          sync.Mutex
	cursor      time.Time
	hasCursor   bool
	rankedSince time.Time
	commits     []storage.RankingRefresh
}

func (fake *fakeStore) LoadRankingCursor(context.Context, uint64) (time.Time, bool, error) {
	return fake.cursor, fake.hasCursor, nil
}

func (fake *fakeStore) RankedSince(_ context.Context, _ uint64, since time.Time) ([]string, error) {
	fake.rankedSince = since
	return []string{"0xold"}, nil
}

func (fake *fakeStore) CommitRankings(_ context.Context, refresh storage.RankingRefresh) error {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.commits = append(fake.commits, refresh)
	fake.cursor, fake.hasCursor = refresh.Head, true
	return nil
}

func newTestRefresher(t *testing.T, source *fakeSource, store *fakeStore) *Refresher {
	t.Helper()
	refresher, err := New(Options{
		ChainID: 1, Source: source, Store: store, Interval: 5 * time.Minute, ReorgWindow: time.Hour,
		Now: func() time.Time { return time.Unix(9_000, 0) },
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return refresher
}

func TestRefreshScansEverythingFirstThenIncrementally(t *testing.T) {
	ctx := context.Background()
	head := time.Date(2026, 3, 10, 12, 7, 30, 0, time.UTC)
	source, store := &fakeSource{head: head, hasHead: true}, &fakeStore{}
	refresher := newTestRefresher(t, source, store)

	if err := refresher.Refresh(ctx); err != nil {
		t.Fatalf("Refresh() error = %v", err)
	}
	if !source.quoteSince.IsZero() || source.quoteTokens != nil {
		t.Fatalf("first scan since = %s, tokens = %v", source.quoteSince, source.quoteTokens)
	}
	if want := time.Date(2026, 3, 9, 12, 5, 0, 0, time.UTC); !source.activitySince.Equal(want) || source.interval != 5*time.Minute {
		t.Fatalf("activity since = %s, interval = %s", source.activitySince, source.interval)
	}
	first := store.commits[0]
	if !first.Since.IsZero() || !first.Head.Equal(head) || len(first.Quotes) != 1 || len(first.Activity) != 1 ||
		!first.RefreshedAt.Equal(time.Unix(9_000, 0)) {
		t.Fatalf("first refresh = %+v", first)
	}

	source.head = head.Add(time.Minute)
	if err := refresher.Refresh(ctx); err != nil {
		t.Fatalf("second Refresh() error = %v", err)
	}
	since := head.Add(-time.Hour)
	if !store.rankedSince.Equal(since) || !source.quoteSince.Equal(since) || !slices.Equal(source.quoteTokens, []string{"0xold"}) {
		t.Fatalf("incremental scan since = %s/%s, tokens = %v", store.rankedSince, source.quoteSince, source.quoteTokens)
	}
	if second := store.commits[1]; !second.Since.Equal(since) || !second.Head.Equal(head.Add(time.Minute)) {
		t.Fatalf("second refresh = %+v", second)
	}
}

func TestRefreshWaitsForBlocksAndReportsErrors(t *testing.T) {
	source, store := &fakeSource{}, &fakeStore{}
	refresher := newTestRefresher(t, source, store)
	if err := refresher.Refresh(context.Background()); err != nil || len(store.commits) != 0 {
		t.Fatalf("Refresh() without blocks = %v, commits %d", err, len(store.commits))
	}
	source.err = errors.New("down")
	if err := refresher.Refresh(context.Background()); !errors.Is(err, source.err) {
		t.Fatalf("Refresh() error = %v", err)
	}
}

func TestRunStopsWithContext(t *testing.T) {
	source, store := &fakeSource{head: time.Unix(100_000, 0), hasHead: true}, &fakeStore{}
	refresher := newTestRefresher(t, source, store)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		refresher.Run(ctx, time.Hour)
		close(done)
	}()
	deadline := time.After(5 * time.Second)
	for len(commitsOf(store)) == 0 {
		select {
		case <-deadline:
			t.Fatal("Run() did not refresh")
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()
	<-done
}

func commitsOf(store *fakeStore) []storage.RankingRefresh {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.commits
}

func TestNewValidatesOptions(t *testing.T) {
	base := Options{ChainID: 1, Source: &fakeSource{}, Store: &fakeStore{}, Interval: time.Minute, ReorgWindow: time.Hour}
	for name, mutate := range map[string]func(*Options){
		"no chain":     func(options *Options) { options.ChainID = 0 },
		"no source":    func(options *Options) { options.Source = nil },
		"long candle":  func(options *Options) { options.Interval = 48 * time.Hour },
		"no reorg gap": func(options *Options) { options.ReorgWindow = 0 },
	} {
		options := base
		mutate(&options)
		if _, err := New(options); err == nil {
			t.Errorf("%s: New() error = nil", name)
		}
	}
}
