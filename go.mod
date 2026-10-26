module github.com/AloneAtWar/chainstream-market

go 1.26.10

replace github.com/AloneAtWar/chainstream-market/proto => ./proto

require (
	github.com/AloneAtWar/chainstream-market/proto v0.0.0-00010101000000-000000000000
	github.com/ClickHouse/clickhouse-go/v2 v2.49.0
	github.com/alicebob/miniredis/v2 v2.37.0
	github.com/coder/websocket v1.8.14
	github.com/go-kratos/kratos/v2 v2.9.2
	github.com/google/wire v0.7.0
	github.com/jackc/pgx/v5 v5.11.0
	github.com/prometheus/client_golang v1.23.2
	github.com/redis/go-redis/v9 v9.20.0
	github.com/shopspring/decimal v1.4.0
	github.com/twmb/franz-go v1.22.1
	github.com/twmb/franz-go/pkg/kfake v0.0.0-20261022190701-123ec885bc97
	google.golang.org/protobuf v1.36.11
	gopkg.in/yaml.v3 v3.0.1
	gorm.io/driver/clickhouse v0.7.0
	gorm.io/driver/postgres v1.6.3
	gorm.io/gorm v1.31.2
)

require (
	dario.cat/mergo v1.0.2 // indirect
	github.com/ClickHouse/ch-go v0.74.0 // indirect
	github.com/andybalholm/brotli v1.2.6 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/fsnotify/fsnotify v1.6.0 // indirect
	github.com/go-faster/city v1.0.1 // indirect
	github.com/go-faster/errors v0.7.1 // indirect
	github.com/go-playground/form/v4 v4.2.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/hashicorp/go-version v1.9.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	github.com/klauspost/compress v1.20.3 // indirect
	github.com/kylelemons/godebug v1.1.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/paulmach/orb v0.13.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.33 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.66.1 // indirect
	github.com/prometheus/procfs v0.16.1 // indirect
	github.com/rogpeppe/go-internal v1.16.0 // indirect
	github.com/segmentio/asm v1.2.1 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.15.0 // indirect
	github.com/yuin/gopher-lua v1.1.1 // indirect
	go.opentelemetry.io/otel v1.46.0 // indirect
	go.opentelemetry.io/otel/trace v1.46.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	go.yaml.in/yaml/v2 v2.4.2 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.29.0 // indirect
)
