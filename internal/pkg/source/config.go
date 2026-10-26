package source

import (
	"fmt"
	"path/filepath"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/config"
)

// Sources opens chain topics through the driver source.driver names.
type Sources struct {
	open func(topic string, start int64) (Source, error)
}

// NewSources reads chain topics with the driver source.driver names: each
// topic on the kafka brokers, or <file.dir>/<topic>.jsonl.
func NewSources(sourceConfig *config.Source, kafka *config.Kafka) (*Sources, error) {
	switch driver := sourceConfig.GetDriver(); driver {
	case config.SourceKafka:
		options := KafkaOptions{
			Brokers: kafka.GetBrokers(), ClientID: kafka.GetClientId(),
			FetchMaxBytes: kafka.GetFetchMaxBytes(), FetchMaxWait: kafka.GetFetchMaxWait().AsDuration(),
		}
		return &Sources{open: func(topic string, start int64) (Source, error) {
			topicOptions := options
			topicOptions.Topic = topic
			return NewKafka(topicOptions, start)
		}}, nil
	case config.SourceFile:
		file := sourceConfig.GetFile()
		return &Sources{open: func(topic string, start int64) (Source, error) {
			return NewFile(FileOptions{
				Path: filepath.Join(file.GetDir(), topic+".jsonl"), PollInterval: file.GetPollInterval().AsDuration(),
			}, start)
		}}, nil
	default:
		return nil, fmt.Errorf("source.driver %q is not supported; use one of: %s, %s", driver, config.SourceFile, config.SourceKafka)
	}
}

// Opener returns the engine's opener of topic.
func (sources *Sources) Opener(topic string) func(start int64) (Source, error) {
	return func(start int64) (Source, error) { return sources.open(topic, start) }
}
