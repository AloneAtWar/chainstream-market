package source

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	streamv1 "github.com/AloneAtWar/chainstream-market/proto/stream/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func blockLine(t *testing.T, height int64) string {
	t.Helper()
	line, err := protojson.Marshal(&streamv1.StreamRecord{Payload: &streamv1.StreamRecord_Block{
		Block: &streamv1.BlockEvent{ChainId: 1, BlockHeight: height},
	}})
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	return string(line) + "\n"
}

func appendFile(t *testing.T, path, text string) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("OpenFile() error = %v", err)
	}
	defer file.Close() //nolint:errcheck // test cleanup
	if _, err := file.WriteString(text); err != nil {
		t.Fatalf("WriteString() error = %v", err)
	}
}

func heights(t *testing.T, messages []Message) []int64 {
	t.Helper()
	var got []int64
	for _, message := range messages {
		var record streamv1.StreamRecord
		if err := proto.Unmarshal(message.Value, &record); err != nil {
			t.Fatalf("Unmarshal() error = %v", err)
		}
		got = append(got, record.GetBlock().GetBlockHeight())
	}
	return got
}

func TestFileFollowsFromStartOffset(t *testing.T) {
	path := filepath.Join(t.TempDir(), "chainstream-eth.jsonl")
	source, err := NewFile(FileOptions{Path: path, PollInterval: 10 * time.Millisecond}, 1)
	if err != nil {
		t.Fatalf("NewFile() error = %v", err)
	}
	defer source.Close()

	// The file does not exist yet.
	short, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, fetchErr := source.Fetch(short); !errors.Is(fetchErr, context.DeadlineExceeded) {
		t.Fatalf("Fetch() before the file exists error = %v", fetchErr)
	}

	appendFile(t, path, blockLine(t, 100)+"\n"+blockLine(t, 101)+blockLine(t, 102))
	ctx, cancelAll := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelAll()
	messages, err := source.Fetch(ctx)
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if got := heights(t, messages); len(got) != 2 || got[0] != 101 || got[1] != 102 {
		t.Fatalf("heights = %v, want [101 102]", got)
	}
	if messages[0].Offset != 1 || messages[1].Offset != 2 || source.HighWatermark() != 3 {
		t.Fatalf("offsets = %d, %d, high watermark %d", messages[0].Offset, messages[1].Offset, source.HighWatermark())
	}

	// A line is only read once its newline is written.
	line := blockLine(t, 103)
	appendFile(t, path, line[:10])
	short, cancel = context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, fetchErr := source.Fetch(short); !errors.Is(fetchErr, context.DeadlineExceeded) {
		t.Fatalf("Fetch() of a partial line error = %v", fetchErr)
	}
	appendFile(t, path, line[10:])
	messages, err = source.Fetch(ctx)
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if got := heights(t, messages); len(got) != 1 || got[0] != 103 || messages[0].Offset != 3 {
		t.Fatalf("heights = %v at offset %d, want [103] at 3", got, messages[0].Offset)
	}
}

func TestFileRejectsBadRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "chainstream-eth.jsonl")
	appendFile(t, path, blockLine(t, 100)+"{not json\n")
	source, err := NewFile(FileOptions{Path: path, PollInterval: 10 * time.Millisecond}, 0)
	if err != nil {
		t.Fatalf("NewFile() error = %v", err)
	}
	defer source.Close()
	if _, err := source.Fetch(context.Background()); err == nil || !strings.Contains(err.Error(), "line 2") {
		t.Fatalf("Fetch() error = %v, want one naming line 2", err)
	}
}

func TestFileRereadsAnUnterminatedLineThatWasReplaced(t *testing.T) {
	path := filepath.Join(t.TempDir(), "chainstream-eth.jsonl")
	appendFile(t, path, blockLine(t, 99))
	source, err := NewFile(FileOptions{Path: path, PollInterval: 10 * time.Millisecond}, 0)
	if err != nil {
		t.Fatalf("NewFile() error = %v", err)
	}
	defer source.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, fetchErr := source.Fetch(ctx); fetchErr != nil {
		t.Fatalf("Fetch() error = %v", fetchErr)
	}

	// chainstream-evm stops part-way through block 100 ...
	stale := safeHeightLine(t, 100, 999)
	appendFile(t, path, stale[:len(stale)-5])
	short, cancelShort := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancelShort()
	if _, fetchErr := source.Fetch(short); !errors.Is(fetchErr, context.DeadlineExceeded) {
		t.Fatalf("Fetch() of a partial line error = %v", fetchErr)
	}
	// ... and on restart cuts that line and writes a longer block 100.
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}
	if truncateErr := os.Truncate(path, info.Size()-int64(len(stale)-5)); truncateErr != nil {
		t.Fatalf("Truncate() error = %v", truncateErr)
	}
	appendFile(t, path, safeHeightLine(t, 100, 1000))

	messages, err := source.Fetch(ctx)
	if err != nil {
		t.Fatalf("Fetch() after the rewrite error = %v", err)
	}
	if len(messages) != 1 || messages[0].Offset != 1 {
		t.Fatalf("messages = %v, want the rewritten block at offset 1", messages)
	}
	var record streamv1.StreamRecord
	if err := proto.Unmarshal(messages[0].Value, &record); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if record.GetBlock().GetBlockHeight() != 100 || record.GetBlock().GetSafeHeight() != 1000 {
		t.Fatalf("record = %v, want block 100 with safe height 1000", &record)
	}
}

func TestFileFailsWhenReadLinesAreRemoved(t *testing.T) {
	path := filepath.Join(t.TempDir(), "chainstream-eth.jsonl")
	appendFile(t, path, blockLine(t, 99)+blockLine(t, 100))
	source, err := NewFile(FileOptions{Path: path, PollInterval: 10 * time.Millisecond}, 0)
	if err != nil {
		t.Fatalf("NewFile() error = %v", err)
	}
	defer source.Close()
	if _, err := source.Fetch(context.Background()); err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if err := os.Truncate(path, int64(len(blockLine(t, 99)))); err != nil {
		t.Fatalf("Truncate() error = %v", err)
	}
	if _, err := source.Fetch(context.Background()); err == nil || !strings.Contains(err.Error(), "shrank") {
		t.Fatalf("Fetch() error = %v, want one saying the file shrank", err)
	}
}

func safeHeightLine(t *testing.T, height, safeHeight int64) string {
	t.Helper()
	line, err := protojson.Marshal(&streamv1.StreamRecord{Payload: &streamv1.StreamRecord_Block{
		Block: &streamv1.BlockEvent{ChainId: 1, BlockHeight: height, SafeHeight: safeHeight},
	}})
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	return string(line) + "\n"
}
