package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/AloneAtWar/chainstream-market/internal/source"
	"github.com/AloneAtWar/chainstream-market/internal/storage"
	streamv1 "github.com/AloneAtWar/chainstream-market/proto/stream/v1"
)

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

func (notifier *recordingNotifier) NotifyBlock(_ context.Context, output storage.BlockOutput, finalized int64) error {
	return notifier.record(fmt.Sprintf("block %d final %d", output.Height, finalized))
}

func (notifier *recordingNotifier) NotifyRevert(_ context.Context, revert storage.Revert) error {
	return notifier.record(fmt.Sprintf("revert %d resets %d", revert.RollbackTo, len(revert.Candles)))
}

// resettingProcessor reports one reset per reverted height.
type resettingProcessor struct {
	HeaderProcessor
	applied int64
}

func (processor *resettingProcessor) Apply(ctx context.Context, block *streamv1.BlockEvent) (storage.BlockOutput, error) {
	processor.applied = block.GetBlockHeight()
	return processor.HeaderProcessor.Apply(ctx, block)
}

func (processor *resettingProcessor) RevertedCandles(rollbackTo int64) []storage.CandleReset {
	return make([]storage.CandleReset, processor.applied-rollbackTo)
}

func runNotified(t *testing.T, memory *storage.Memory, notifier *recordingNotifier, messages []source.Message) error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	engine, err := New(Options{
		ChainID: testChainID, Chain: "eth", Processor: &resettingProcessor{}, Output: memory, Checkpoints: memory,
		Notifier: notifier,
		Open: func(start int64) (source.Source, error) {
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
	return engine.Run(ctx)
}

func TestEngineNotifiesBlocksAndReverts(t *testing.T) {
	messages := encode(t, 0,
		block(100, "0xa100", "0xa099"), block(101, "0xa101", "0xa100"), block(102, "0xa102", "0xa101"),
		revert(100), block(101, "0xb101", "0xa100"),
	)
	notifier := &recordingNotifier{}
	if err := runNotified(t, storage.NewMemory(), notifier, messages); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	want := "block 100 final 101,block 101 final 101,block 102 final 101,revert 100 resets 2,block 101 final 101"
	if got := strings.Join(notifier.events, ","); got != want {
		t.Fatalf("events = %s, want %s", got, want)
	}
}

// A failed notification stops the engine before the checkpoint, so the
// record is applied and notified again on restart.
func TestEngineRetriesFailedNotifications(t *testing.T) {
	messages := encode(t, 0, block(100, "0xa100", "0xa099"), block(101, "0xa101", "0xa100"))
	memory := storage.NewMemory()
	notifier := &recordingNotifier{failAt: 2}
	if err := runNotified(t, memory, notifier, messages); err == nil || !strings.Contains(err.Error(), "broker down") {
		t.Fatalf("Run() error = %v, want the notifier's", err)
	}
	if checkpoint, _, _ := memory.Load(context.Background(), testChainID); checkpoint.Offset != 0 { //nolint:errcheck // memory never fails
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
