package chainsync

import (
	"context"
	"errors"
	"testing"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/source"
	streamv1 "github.com/AloneAtWar/chainstream-market/proto/stream/v1"
	"google.golang.org/protobuf/proto"
)

const testChainID = 1

// fakeSource returns its messages from start, then cancels the run.
type fakeSource struct {
	messages []source.Message
	cancel   context.CancelFunc
	done     bool
}

func (fake *fakeSource) Fetch(ctx context.Context) ([]source.Message, error) {
	if fake.done {
		fake.cancel()
		<-ctx.Done()
		return nil, ctx.Err()
	}
	fake.done = true
	return fake.messages, nil
}

func (*fakeSource) Close() {}

func block(height int64, hash, parent string) *streamv1.StreamRecord {
	return &streamv1.StreamRecord{Payload: &streamv1.StreamRecord_Block{Block: &streamv1.BlockEvent{
		ChainId: testChainID, Chain: "eth", BlockHeight: height, BlockHash: hash, ParentHash: parent,
		BlockTime: 1_700_000_000 + height, FinalizedHeight: 101,
	}}}
}

func revert(rollbackTo int64) *streamv1.StreamRecord {
	return &streamv1.StreamRecord{Payload: &streamv1.StreamRecord_Revert{Revert: &streamv1.BlockRevert{
		ChainId: testChainID, Chain: "eth", RollbackTo: rollbackTo,
	}}}
}

func encode(t *testing.T, first int64, records ...*streamv1.StreamRecord) []source.Message {
	t.Helper()
	messages := make([]source.Message, len(records))
	for index, record := range records {
		value, err := proto.Marshal(record)
		if err != nil {
			t.Fatalf("Marshal() error = %v", err)
		}
		messages[index] = source.Message{Offset: first + int64(index), Value: value}
	}
	return messages
}

// headerHandler keeps the hash of each applied block and the last
// checkpoint, as a store would.
type headerHandler struct {
	blocks     map[int64]appliedBlock
	checkpoint Checkpoint
	committed  bool
}

type appliedBlock struct {
	hash      string
	finalized bool
}

func newHeaderHandler() *headerHandler {
	return &headerHandler{blocks: make(map[int64]appliedBlock)}
}

func (handler *headerHandler) Restore(context.Context, uint64) (Checkpoint, bool, error) {
	return handler.checkpoint, handler.committed, nil
}

func (handler *headerHandler) Apply(_ context.Context, block *streamv1.BlockEvent) error {
	finalized := block.GetFinalizedHeight()
	handler.blocks[block.GetBlockHeight()] = appliedBlock{
		hash: block.GetBlockHash(), finalized: finalized > 0 && block.GetBlockHeight() <= finalized,
	}
	return nil
}

func (handler *headerHandler) Revert(_ context.Context, rollbackTo int64) error {
	for height := range handler.blocks {
		if height > rollbackTo {
			delete(handler.blocks, height)
		}
	}
	return nil
}

func (handler *headerHandler) Commit(_ context.Context, checkpoint Checkpoint) error {
	handler.checkpoint, handler.committed = checkpoint, true
	return nil
}

type run struct {
	start int64
	err   error
}

func runEngine(t *testing.T, handler *headerHandler, messages []source.Message) run {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := run{start: -1}
	engine, err := New(Options{
		ChainID: testChainID, Chain: "eth", Handler: handler,
		Open: func(start int64) (source.Source, error) {
			result.start = start
			var remaining []source.Message
			for _, message := range messages {
				if message.Offset >= start {
					remaining = append(remaining, message)
				}
			}
			return &fakeSource{messages: remaining, cancel: cancel}, nil
		},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	result.err = engine.Run(ctx)
	return result
}

func heights(handler *headerHandler) map[int64]string {
	got := make(map[int64]string, len(handler.blocks))
	for height, block := range handler.blocks {
		got[height] = block.hash
	}
	return got
}

func TestEngineAppliesBlocksAndReorgs(t *testing.T) {
	handler := newHeaderHandler()
	result := runEngine(t, handler, encode(t, 0,
		block(100, "a100", "a99"),
		block(101, "a101", "a100"),
		block(102, "a102", "a101"),
		revert(100),
		block(101, "b101", "a100"),
		block(102, "b102", "b101"),
	))
	if result.err != nil {
		t.Fatalf("Run() error = %v", result.err)
	}
	if result.start != 0 {
		t.Fatalf("start offset = %d, want 0", result.start)
	}
	got := heights(handler)
	want := map[int64]string{100: "a100", 101: "b101", 102: "b102"}
	if len(got) != len(want) {
		t.Fatalf("blocks = %v, want %v", got, want)
	}
	for height, hash := range want {
		if got[height] != hash {
			t.Fatalf("blocks = %v, want %v", got, want)
		}
	}
	if !handler.blocks[101].finalized || handler.blocks[102].finalized {
		t.Fatalf("finalized flags = %v %v", handler.blocks[101].finalized, handler.blocks[102].finalized)
	}
	checkpoint := handler.checkpoint
	if checkpoint.Offset != 5 || checkpoint.Height != 102 || checkpoint.Hash != "b102" {
		t.Fatalf("checkpoint = %+v", checkpoint)
	}
}

func TestEngineResumesAfterCheckpoint(t *testing.T) {
	handler := newHeaderHandler()
	messages := encode(t, 0, block(100, "a100", "a99"), block(101, "a101", "a100"), block(102, "a102", "a101"))
	handler.checkpoint = Checkpoint{ChainID: testChainID, Offset: 1, Height: 101, Hash: "a101"}
	handler.committed = true
	result := runEngine(t, handler, messages)
	if result.err != nil {
		t.Fatalf("Run() error = %v", result.err)
	}
	if result.start != 2 {
		t.Fatalf("start offset = %d, want 2", result.start)
	}
	if got := heights(handler); len(got) != 1 || got[102] != "a102" {
		t.Fatalf("blocks = %v", got)
	}
}

func TestEngineSkipsRedeliveredRecords(t *testing.T) {
	handler := newHeaderHandler()
	result := runEngine(t, handler, encode(t, 0,
		revert(99), // nothing applied yet
		block(100, "a100", "a99"),
		block(101, "a101", "a100"),
		block(100, "a100", "a99"), // retried publish
		block(101, "a101", "a100"),
		revert(101), // nothing above 101
		block(102, "a102", "a101"),
	))
	if result.err != nil {
		t.Fatalf("Run() error = %v", result.err)
	}
	if got := heights(handler); len(got) != 3 || got[102] != "a102" {
		t.Fatalf("blocks = %v", got)
	}
}

func TestEngineRejectsInconsistentStream(t *testing.T) {
	otherChain := block(101, "a101", "a100")
	otherChain.GetBlock().ChainId = 56
	tests := map[string]struct {
		records []*streamv1.StreamRecord
		want    error
	}{
		"gap":             {[]*streamv1.StreamRecord{block(100, "a100", "a99"), block(102, "a102", "a101")}, ErrHeightGap},
		"parent mismatch": {[]*streamv1.StreamRecord{block(100, "a100", "a99"), block(101, "b101", "b100")}, ErrParentMismatch},
		"chain mismatch":  {[]*streamv1.StreamRecord{block(100, "a100", "a99"), otherChain}, ErrChainMismatch},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			handler := newHeaderHandler()
			result := runEngine(t, handler, encode(t, 0, test.records...))
			if !errors.Is(result.err, test.want) {
				t.Fatalf("Run() error = %v, want %v", result.err, test.want)
			}
			if checkpoint := handler.checkpoint; checkpoint.Offset != 0 {
				t.Fatalf("checkpoint offset = %d, want 0", checkpoint.Offset)
			}
		})
	}
}

func TestEngineAcceptsParentAfterRevert(t *testing.T) {
	handler := newHeaderHandler()
	result := runEngine(t, handler, encode(t, 0,
		block(100, "a100", "a99"),
		block(101, "a101", "a100"),
		revert(100),
		// The hash at 100 is unknown after a revert, so any parent is accepted.
		block(101, "b101", "x100"),
	))
	if result.err != nil {
		t.Fatalf("Run() error = %v", result.err)
	}
}

func TestNewRequiresDependencies(t *testing.T) {
	if _, err := New(Options{ChainID: testChainID}); err == nil {
		t.Fatal("New() error = nil")
	}
	open := func(int64) (source.Source, error) { return nil, errors.New("unused") }
	if _, err := New(Options{ChainID: testChainID, Open: open}); err == nil {
		t.Fatal("New() without a handler error = nil")
	}
}
