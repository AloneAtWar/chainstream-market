package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/AloneAtWar/chainstream-market/internal/source"
	"github.com/AloneAtWar/chainstream-market/internal/storage"
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

type run struct {
	memory *storage.Memory
	start  int64
	err    error
}

func runEngine(t *testing.T, memory *storage.Memory, messages []source.Message) run {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := run{memory: memory, start: -1}
	engine, err := New(Options{
		ChainID: testChainID, Chain: "eth", Processor: HeaderProcessor{},
		Output: memory, Checkpoints: memory,
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

func heights(blocks []storage.BlockOutput) map[int64]string {
	got := make(map[int64]string, len(blocks))
	for _, block := range blocks {
		got[block.Height] = block.Hash
	}
	return got
}

func TestEngineAppliesBlocksAndReorgs(t *testing.T) {
	memory := storage.NewMemory()
	result := runEngine(t, memory, encode(t, 0,
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
	got := heights(memory.Blocks(testChainID))
	want := map[int64]string{100: "a100", 101: "b101", 102: "b102"}
	if len(got) != len(want) {
		t.Fatalf("blocks = %v, want %v", got, want)
	}
	for height, hash := range want {
		if got[height] != hash {
			t.Fatalf("blocks = %v, want %v", got, want)
		}
	}
	blocks := memory.Blocks(testChainID)
	if !blocks[1].Finalized || blocks[2].Finalized {
		t.Fatalf("finalized flags = %v %v", blocks[1].Finalized, blocks[2].Finalized)
	}
	checkpoint, ok, err := memory.Load(context.Background(), testChainID)
	if err != nil || !ok {
		t.Fatalf("Load() = %v, %v", ok, err)
	}
	if checkpoint.Offset != 5 || checkpoint.Height != 102 || checkpoint.Hash != "b102" {
		t.Fatalf("checkpoint = %+v", checkpoint)
	}
}

func TestEngineResumesAfterCheckpoint(t *testing.T) {
	memory := storage.NewMemory()
	messages := encode(t, 0, block(100, "a100", "a99"), block(101, "a101", "a100"), block(102, "a102", "a101"))
	if err := memory.Save(context.Background(), storage.Checkpoint{ChainID: testChainID, Offset: 1, Height: 101, Hash: "a101"}); err != nil {
		t.Fatalf("Save() error = %v", err)
	}
	result := runEngine(t, memory, messages)
	if result.err != nil {
		t.Fatalf("Run() error = %v", result.err)
	}
	if result.start != 2 {
		t.Fatalf("start offset = %d, want 2", result.start)
	}
	if got := heights(memory.Blocks(testChainID)); len(got) != 1 || got[102] != "a102" {
		t.Fatalf("blocks = %v", got)
	}
}

func TestEngineSkipsRedeliveredRecords(t *testing.T) {
	memory := storage.NewMemory()
	result := runEngine(t, memory, encode(t, 0,
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
	if got := heights(memory.Blocks(testChainID)); len(got) != 3 || got[102] != "a102" {
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
			memory := storage.NewMemory()
			result := runEngine(t, memory, encode(t, 0, test.records...))
			if !errors.Is(result.err, test.want) {
				t.Fatalf("Run() error = %v, want %v", result.err, test.want)
			}
			checkpoint, _, _ := memory.Load(context.Background(), testChainID)
			if checkpoint.Offset != 0 {
				t.Fatalf("checkpoint offset = %d, want 0", checkpoint.Offset)
			}
		})
	}
}

func TestEngineAcceptsParentAfterRevert(t *testing.T) {
	memory := storage.NewMemory()
	result := runEngine(t, memory, encode(t, 0,
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
}
