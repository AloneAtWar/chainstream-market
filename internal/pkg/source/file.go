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
//
// File 从文件读取链 topic 并随文件增长持续跟读（配合 chainstream-evm 的
// file 输出，format=protojson）。记录的 offset 是它在非空行中的序号，因此检查点用法
// 与 Kafka 相同；值被重新编码为二进制 protobuf，与 Kafka 中的格式一致。
// 只有写完换行符的行才会被读取：未结束的行下次从行首重读，因为 chainstream-evm 在写入
// 失败后会截掉这半行并在原位置写入另一条记录。
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
	// scratch holds the bytes of a line read again by stableLine.
	scratch []byte
	// afterRead, when a test sets it, runs after each read from the file.
	afterRead func()
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
		source.file = file
		source.reader = bufio.NewReaderSize(source.input(), 1<<20)
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
		chunk, err = source.stableLine(chunk)
		if err != nil {
			return nil, err
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
//
// rewindPartial 在读到文件末尾时丢弃未结束行的部分字节，下次从该行开头重读；
// 若文件比已读部分还短（被截断），返回错误停止。
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
	source.reader.Reset(source.input())
	return nil
}

// stableLine reads the line that ends chunk again from the file and returns
// what the file holds there. One ReadBytes can span several reads of the file,
// so chunk may join the start of an unterminated line with the end of the
// record chainstream-evm wrote in its place after cutting that line. A newline
// in the file is never removed once written, and neither are the bytes before
// it, so the bytes read after chunk's newline was seen are final. If they
// differ from chunk, the read goes on from the end of the line they hold.
//
// stableLine 在看到换行后从文件中按原位置再读一遍这一行：一次 ReadBytes 可能跨越多次
// 文件读取，拼接了“被截掉的半行开头”和“替换记录的结尾”。换行一旦写入就不会再变，
// 所以重读得到的内容才是最终结果；与原读取不同时以重读结果为准。
func (source *File) stableLine(chunk []byte) ([]byte, error) {
	if cap(source.scratch) < len(chunk) {
		source.scratch = make([]byte, len(chunk))
	}
	stable := source.scratch[:len(chunk)]
	if _, err := source.file.ReadAt(stable, source.consumed); err != nil {
		return nil, fmt.Errorf("file source: read %s again at byte %d: %w", source.options.Path, source.consumed, err)
	}
	if bytes.Equal(stable, chunk) {
		return chunk, nil
	}
	end := bytes.IndexByte(stable, '\n')
	if end < 0 {
		return nil, fmt.Errorf("file source: %s no longer holds the line read at byte %d",
			source.options.Path, source.consumed)
	}
	stable = bytes.Clone(stable[:end+1])
	if _, err := source.file.Seek(source.consumed+int64(len(stable)), io.SeekStart); err != nil {
		return nil, fmt.Errorf("file source: seek %s: %w", source.options.Path, err)
	}
	source.reader.Reset(source.input())
	return stable, nil
}

// input is what the buffered reader reads the file through.
func (source *File) input() io.Reader {
	if source.afterRead == nil {
		return source.file
	}
	return readerFunc(func(buf []byte) (int, error) {
		n, err := source.file.Read(buf)
		source.afterRead()
		return n, err
	})
}

type readerFunc func([]byte) (int, error)

func (read readerFunc) Read(buf []byte) (int, error) {
	return read(buf)
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
