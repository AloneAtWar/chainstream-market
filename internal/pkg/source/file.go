package source

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strings"
	"time"

	streamv1 "github.com/AloneAtWar/chainstream-market/proto/stream/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// maxFileBatch caps the records one Fetch returns from a file.
const maxFileBatch = 256

type FileOptions struct {
	// Path is a file of stream.v1.StreamRecords in protobuf JSON, one per
	// line.
	Path string
	// PollInterval is how often the end of the file is checked for new
	// lines, and a missing file for its creation.
	PollInterval time.Duration
}

// File reads a chain topic's records from a file and follows it as it
// grows, as chainstream-evm's file output appends to it. A record's offset is
// its index among the file's non-blank lines, so checkpoints work as they do
// with Kafka. Values are re-encoded as binary protobuf, as Kafka carries them.
type File struct {
	options FileOptions
	start   int64

	file   *os.File
	reader *bufio.Reader
	// consumed is the length of the complete lines read so far. A line whose
	// newline has not been written yet is read again from there next time,
	// because chainstream-evm may cut such a line after a failed write and
	// write a different record in its place.
	consumed      int64
	next          int64
	line          int
	highWatermark int64
}

// NewFile reads options.Path from offset start. The file may not exist yet.
func NewFile(options FileOptions, start int64) (*File, error) {
	if strings.TrimSpace(options.Path) == "" {
		return nil, errors.New("file source: path is required")
	}
	if start < 0 {
		return nil, fmt.Errorf("file source: invalid start offset %d", start)
	}
	if options.PollInterval <= 0 {
		return nil, errors.New("file source: poll interval must be positive")
	}
	return &File{options: options, start: start, highWatermark: -1}, nil
}

// Fetch blocks until at least one record at or after the start offset is
// available or ctx is done.
func (source *File) Fetch(ctx context.Context) ([]Message, error) {
	for {
		messages, err := source.read()
		if err != nil || len(messages) > 0 {
			return messages, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(source.options.PollInterval):
		}
	}
}

// read returns the complete records written since the last call.
func (source *File) read() ([]Message, error) {
	if source.file == nil {
		file, err := os.Open(source.options.Path)
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		if err != nil {
			return nil, fmt.Errorf("file source: %w", err)
		}
		source.file, source.reader = file, bufio.NewReaderSize(file, 1<<20)
	}
	var messages []Message
	for len(messages) < maxFileBatch {
		chunk, err := source.reader.ReadBytes('\n')
		if errors.Is(err, io.EOF) {
			if rewindErr := source.rewindPartial(len(chunk)); rewindErr != nil {
				return nil, rewindErr
			}
			source.highWatermark = source.next
			return messages, nil
		}
		if err != nil {
			return nil, fmt.Errorf("file source: read %s: %w", source.options.Path, err)
		}
		source.consumed += int64(len(chunk))
		line := bytes.TrimSpace(chunk)
		source.line++
		if len(line) == 0 {
			continue
		}
		offset := source.next
		source.next++
		if offset < source.start {
			continue
		}
		value, err := encodeRecord(line)
		if err != nil {
			return nil, fmt.Errorf("file source: %s line %d: %w", source.options.Path, source.line, err)
		}
		messages = append(messages, Message{Offset: offset, Value: value})
	}
	return messages, nil
}

// rewindPartial runs at the end of the file. It drops the partial bytes of an
// unterminated line so the next read starts again at the line's beginning, and
// fails if the file no longer holds the lines already read.
func (source *File) rewindPartial(partial int) error {
	info, err := source.file.Stat()
	if err != nil {
		return fmt.Errorf("file source: stat %s: %w", source.options.Path, err)
	}
	if info.Size() < source.consumed {
		return fmt.Errorf("file source: %s shrank to %d bytes, below the %d already read",
			source.options.Path, info.Size(), source.consumed)
	}
	if partial == 0 {
		return nil
	}
	if _, err := source.file.Seek(source.consumed, io.SeekStart); err != nil {
		return fmt.Errorf("file source: seek %s: %w", source.options.Path, err)
	}
	source.reader.Reset(source.file)
	return nil
}

// encodeRecord turns a StreamRecord in protobuf JSON into binary protobuf.
func encodeRecord(line []byte) ([]byte, error) {
	var record streamv1.StreamRecord
	if err := protojson.Unmarshal(line, &record); err != nil {
		return nil, err
	}
	return proto.Marshal(&record)
}

// HighWatermark returns the offset after the last record read when the read
// last reached the end of the file, or -1 before it did.
func (source *File) HighWatermark() int64 {
	return source.highWatermark
}

func (source *File) Close() {
	if source.file != nil {
		_ = source.file.Close()
	}
}

var (
	_ Source = (*File)(nil)
	_ Lagger = (*File)(nil)
)
