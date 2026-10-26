package biz

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/chainsync"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/model"
	"github.com/AloneAtWar/chainstream-market/internal/pkg/source"
	streamv1 "github.com/AloneAtWar/chainstream-market/proto/stream/v1"
	"google.golang.org/protobuf/proto"
)

const handlerChainID = 1

// HeaderProcessor emits only block headers.
type HeaderProcessor struct{}

func (HeaderProcessor) Apply(_ context.Context, block *streamv1.BlockEvent) (model.BlockOutput, error) {
	return NewBlockOutput(block), nil
}

func (HeaderProcessor) Revert(context.Context, int64) error { return nil }

var _ BlockProcessor = HeaderProcessor{}

// listSource returns its messages from start, then cancels the run.
type listSource struct {
	messages []source.Message
	cancel   context.CancelFunc
	done     bool
}

func (list *listSource) Fetch(ctx context.Context) ([]source.Message, error) {
	if list.done {
		list.cancel()
		<-ctx.Done()
		return nil, ctx.Err()
	}
	list.done = true
	return list.messages, nil
}

func (*listSource) Close() {}

func headerBlock(height int64, hash, parent string) *streamv1.StreamRecord {
	return &streamv1.StreamRecord{Payload: &streamv1.StreamRecord_Block{Block: &streamv1.BlockEvent{
		ChainId: handlerChainID, Chain: "eth", BlockHeight: height, BlockHash: hash, ParentHash: parent,
		BlockTime: 1_700_000_000 + height, FinalizedHeight: 101,
	}}}
}

func headerRevert(rollbackTo int64) *streamv1.StreamRecord {
	return &streamv1.StreamRecord{Payload: &streamv1.StreamRecord_Revert{Revert: &streamv1.BlockRevert{
		ChainId: handlerChainID, Chain: "eth", RollbackTo: rollbackTo,
	}}}
}

func encodeRecords(t *testing.T, records ...*streamv1.StreamRecord) []source.Message {
	t.Helper()
	messages := make([]source.Message, len(records))
	for index, record := range records {
		value, err := proto.Marshal(record)
		if err != nil {
			t.Fatalf("Marshal() error = %v", err)
		}
		messages[index] = source.Message{Offset: int64(index), Value: value}
	}
	return messages
}

// runHandler runs the handler options describe over messages and returns
// the offset the engine opened the topic at.
func runHandler(t *testing.T, options HandlerOptions, messages []source.Message) (int64, error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	options.ChainID = handlerChainID
	handler, err := NewHandler(options)
	if err != nil {
		t.Fatalf("NewHandler() error = %v", err)
	}
	start := int64(-1)
	engine, err := chainsync.New(chainsync.Options{
		ChainID: handlerChainID, Chain: "eth", Handler: handler,
		Open: func(offset int64) (source.Source, error) {
			start = offset
			var remaining []source.Message
			for _, message := range messages {
				if message.Offset >= offset {
					remaining = append(remaining, message)
				}
			}
			return &listSource{messages: remaining, cancel: cancel}, nil
		},
	})
	if err != nil {
		t.Fatalf("chainsync.New() error = %v", err)
	}
	return start, engine.Run(ctx)
}

func TestHandlerWritesOutputAndCheckpoints(t *testing.T) {
	memory := NewMemory()
	_, err := runHandler(t, HandlerOptions{Processor: HeaderProcessor{}, Output: memory, Checkpoints: memory}, encodeRecords(t,
		headerBlock(100, "a100", "a99"), headerBlock(101, "a101", "a100"), headerBlock(102, "a102", "a101"),
		headerRevert(100), headerBlock(101, "b101", "a100"),
	))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	blocks := memory.Blocks(handlerChainID)
	if len(blocks) != 2 || blocks[1].Hash != "b101" || !blocks[1].Finalized {
		t.Fatalf("blocks = %+v", blocks)
	}
	checkpoint, ok, err := memory.Load(context.Background(), handlerChainID)
	if err != nil || !ok || checkpoint.Offset != 4 || checkpoint.Hash != "b101" {
		t.Fatalf("Load() = %+v, %v, %v", checkpoint, ok, err)
	}
}

type recordingNotifier struct {
	events []string
	failAt int
}

func (notifier *recordingNotifier) record(event string) error {
	notifier.events = append(notifier.events, event)
	if len(notifier.events) == notifier.failAt {
		return errors.New("broker down")
	}
	return nil
}

func (notifier *recordingNotifier) NotifyBlock(_ context.Context, output model.BlockOutput, finalized int64) error {
	return notifier.record(fmt.Sprintf("block %d final %d", output.Height, finalized))
}

func (notifier *recordingNotifier) NotifyRevert(_ context.Context, revert model.Revert) error {
	return notifier.record(fmt.Sprintf("revert %d resets %d", revert.RollbackTo, len(revert.Candles)))
}

// resettingProcessor reports one reset per reverted height.
type resettingProcessor struct {
	HeaderProcessor
	applied int64
}

func (processor *resettingProcessor) Apply(ctx context.Context, block *streamv1.BlockEvent) (model.BlockOutput, error) {
	processor.applied = block.GetBlockHeight()
	return processor.HeaderProcessor.Apply(ctx, block)
}

func (processor *resettingProcessor) RevertedCandles(rollbackTo int64) []model.CandleReset {
	return make([]model.CandleReset, processor.applied-rollbackTo)
}

func runNotified(t *testing.T, memory *Memory, notifier *recordingNotifier, messages []source.Message) error {
	t.Helper()
	_, err := runHandler(t, HandlerOptions{
		Processor: &resettingProcessor{}, Output: memory, Checkpoints: memory, Notifier: notifier,
	}, messages)
	return err
}

func TestHandlerNotifiesBlocksAndReverts(t *testing.T) {
	messages := encodeRecords(t,
		headerBlock(100, "0xa100", "0xa099"), headerBlock(101, "0xa101", "0xa100"), headerBlock(102, "0xa102", "0xa101"),
		headerRevert(100), headerBlock(101, "0xb101", "0xa100"),
	)
	notifier := &recordingNotifier{}
	if err := runNotified(t, NewMemory(), notifier, messages); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	want := "block 100 final 101,block 101 final 101,block 102 final 101,revert 100 resets 2,block 101 final 101"
	if got := strings.Join(notifier.events, ","); got != want {
		t.Fatalf("events = %s, want %s", got, want)
	}
}

// A failed notification stops the engine before the checkpoint, so the
// record is applied and notified again on restart.
func TestHandlerRetriesFailedNotifications(t *testing.T) {
	messages := encodeRecords(t, headerBlock(100, "0xa100", "0xa099"), headerBlock(101, "0xa101", "0xa100"))
	memory := NewMemory()
	notifier := &recordingNotifier{failAt: 2}
	if err := runNotified(t, memory, notifier, messages); err == nil || !strings.Contains(err.Error(), "broker down") {
		t.Fatalf("Run() error = %v, want the notifier's", err)
	}
	if checkpoint, _, _ := memory.Load(context.Background(), handlerChainID); checkpoint.Offset != 0 { //nolint:errcheck // memory never fails
		t.Fatalf("checkpoint offset = %d, want 0", checkpoint.Offset)
	}
	notifier.failAt = 0
	if err := runNotified(t, memory, notifier, messages); err != nil {
		t.Fatalf("Run() after failure error = %v", err)
	}
	if got := strings.Join(notifier.events, ","); got != "block 100 final 101,block 101 final 101,block 101 final 101" {
		t.Fatalf("events = %s", got)
	}
}

// countingProcessor persists how many blocks it applied.
type countingProcessor struct {
	HeaderProcessor
	applied  int
	restored bool
}

func (counter *countingProcessor) Apply(ctx context.Context, block *streamv1.BlockEvent) (model.BlockOutput, error) {
	counter.applied++
	return counter.HeaderProcessor.Apply(ctx, block)
}

func (counter *countingProcessor) Restore(state State) error {
	counter.restored = true
	for _, entry := range state.Entries {
		if entry.Kind == "count" {
			return json.Unmarshal(entry.Value, &counter.applied)
		}
	}
	return nil
}

func (counter *countingProcessor) TakeDelta() (StateDelta, error) {
	raw, err := json.Marshal(counter.applied)
	return StateDelta{Entries: []StateEntry{{Kind: "count", Key: "applied", Value: raw}}}, err
}

func runStateful(t *testing.T, memory *Memory, processor *countingProcessor, messages []source.Message) int64 {
	t.Helper()
	start, err := runHandler(t, HandlerOptions{Processor: processor, Output: memory, State: memory}, messages)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	return start
}

func TestHandlerRestoresPersistedState(t *testing.T) {
	memory := NewMemory()
	messages := encodeRecords(t,
		headerBlock(100, "a100", "a99"), headerBlock(101, "a101", "a100"), headerBlock(102, "a102", "a101"),
		headerBlock(103, "a103", "a102"), headerBlock(104, "a104", "a103"),
	)
	first := &countingProcessor{}
	if start := runStateful(t, memory, first, messages[:3]); start != 0 || first.restored {
		t.Fatalf("first run start = %d, restored = %v", start, first.restored)
	}
	second := &countingProcessor{}
	if start := runStateful(t, memory, second, messages); start != 3 || !second.restored {
		t.Fatalf("second run start = %d, restored = %v", start, second.restored)
	}
	if second.applied != 5 {
		t.Fatalf("applied = %d, want 5", second.applied)
	}
	state, ok, err := memory.LoadState(context.Background(), handlerChainID)
	if err != nil || !ok || state.Checkpoint.Offset != 4 || state.Checkpoint.Hash != "a104" {
		t.Fatalf("LoadState() = %+v, %v, %v", state.Checkpoint, ok, err)
	}
	if len(state.Entries) != 1 || string(state.Entries[0].Value) != "5" {
		t.Fatalf("entries = %+v", state.Entries)
	}
}

func TestNewHandlerRequiresStatefulProcessorForState(t *testing.T) {
	memory := NewMemory()
	if _, err := NewHandler(HandlerOptions{ChainID: handlerChainID, Processor: HeaderProcessor{}, Output: memory, State: memory}); err == nil {
		t.Fatal("NewHandler() with a stateless processor and a state store error = nil")
	}
	if _, err := NewHandler(HandlerOptions{ChainID: handlerChainID, Output: memory, Checkpoints: memory}); err == nil {
		t.Fatal("NewHandler() without a processor error = nil")
	}
}
