package live

import (
	"fmt"
	"log/slog"

	"github.com/AloneAtWar/chainstream-market/internal/pkg/config"
)

// OpenSink opens the transport live.driver names for market-engine to
// publish through.
func OpenSink(live *config.Live, kafka *config.Kafka) (Sink, error) {
	switch live.GetDriver() {
	case config.LiveKafka:
		return NewKafkaSink(kafkaOptions(kafka, nil))
	case config.LiveRedis:
		return NewRedisSink(redisOptions(live.GetRedis()))
	default:
		return nil, unsupported(live)
	}
}

// OpenFeed opens the transport live.driver names for market-api to follow.
func OpenFeed(live *config.Live, kafka *config.Kafka, logger *slog.Logger) (Feed, error) {
	switch live.GetDriver() {
	case config.LiveKafka:
		return NewKafkaFeed(kafkaOptions(kafka, logger))
	case config.LiveRedis:
		return NewRedisFeed(redisOptions(live.GetRedis()))
	default:
		return nil, unsupported(live)
	}
}

func unsupported(live *config.Live) error {
	return fmt.Errorf("live.driver %q is not supported; use one of: %s, %s", live.GetDriver(), config.LiveKafka, config.LiveRedis)
}

func kafkaOptions(kafka *config.Kafka, logger *slog.Logger) KafkaOptions {
	return KafkaOptions{
		Brokers: kafka.GetBrokers(), ClientID: kafka.GetClientId(),
		FetchMaxBytes: kafka.GetFetchMaxBytes(), FetchMaxWait: kafka.GetFetchMaxWait().AsDuration(), Logger: logger,
	}
}

func redisOptions(redis *config.Redis) RedisOptions {
	return RedisOptions{
		Addr: redis.GetAddr(), Username: redis.GetUsername(), Password: redis.GetPassword(),
		DB: int(redis.GetDatabase()), DialTimeout: redis.GetDialTimeout().AsDuration(),
	}
}
