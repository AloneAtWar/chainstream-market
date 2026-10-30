// Package config loads each component's configuration and holds the
// sections more than one of them reads. A component defines its own
// protobuf config message, embedding the sections of common.proto that it
// needs, and loads it with Load.
//
// config 包加载每个组件的配置（Kratos config，支持 ${NAME} / ${NAME:default} 环境变量
// 占位），并定义多个组件共用的配置段（common.proto）：链数据源、Kafka、数据库、实时推送、
// 指标和链列表。
package config

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-kratos/kratos/v2/config"
	"github.com/go-kratos/kratos/v2/config/env"
	"github.com/go-kratos/kratos/v2/config/file"
	"github.com/go-kratos/kratos/v2/encoding"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/durationpb"
	"gopkg.in/yaml.v3"
)

// Config is a component's configuration message.
type Config interface {
	proto.Message
	// ApplyDefaults fills every section and replaces unset settings with
	// their defaults, so readers may use the fields directly.
	ApplyDefaults()
	// Validate reports the first invalid setting of a defaulted config.
	Validate() error
}

// Load reads, defaults and validates the configuration at path, a file or a
// directory of them, into target. Each ${NAME} or ${NAME:default} in it is
// replaced with the environment variable NAME, or default when NAME is
// unset. The returned func closes the config's sources.
func Load(path string, target Config) (func() error, error) {
	// The environment goes first, so the file wins where a variable shares
	// the name of a top-level key.
	return load(target, env.NewSource(), file.NewSource(path))
}

// Parse decodes, defaults and validates raw YAML into target, without the
// environment.
func Parse(raw []byte, target Config) error {
	closeConfig, err := load(target, bytesSource(raw))
	if err != nil {
		return err
	}
	return closeConfig()
}

func load(target Config, sources ...config.Source) (func() error, error) {
	c := config.New(config.WithSource(sources...), config.WithDecoder(decoder(target)))
	if err := c.Load(); err != nil {
		return nil, errors.Join(fmt.Errorf("conf: load: %w", err), c.Close())
	}
	if err := scan(c, target); err != nil {
		return nil, errors.Join(err, c.Close())
	}
	target.ApplyDefaults()
	if err := target.Validate(); err != nil {
		return nil, errors.Join(err, c.Close())
	}
	return c.Close, nil
}

// scan reads the resolved config into target. Placeholders are resolved
// after decoding, so values that came from the environment are normalized
// here once more before protojson reads them.
func scan(c config.Config, target Config) error {
	var values map[string]any
	if err := c.Scan(&values); err != nil {
		return fmt.Errorf("conf: scan: %w", err)
	}
	normalize(values, target.ProtoReflect().Descriptor())
	raw, err := json.Marshal(values)
	if err != nil {
		return fmt.Errorf("conf: scan: %w", err)
	}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(raw, target); err != nil {
		return fmt.Errorf("conf: decode: %w", err)
	}
	return nil
}

// decoder is Kratos' default decoder, except that YAML is decoded against
// message's descriptor: a scalar read into a string field keeps its text, so
// an unquoted 0x1f stays an address instead of becoming 31.
func decoder(message proto.Message) config.Decoder {
	return func(kv *config.KeyValue, target map[string]any) error {
		return decode(kv, target, message.ProtoReflect().Descriptor())
	}
}

func decode(kv *config.KeyValue, target map[string]any, descriptor protoreflect.MessageDescriptor) error {
	switch kv.Format {
	case "":
		keys := strings.Split(kv.Key, ".")
		for _, key := range keys[:len(keys)-1] {
			sub := make(map[string]any)
			target[key] = sub
			target = sub
		}
		target[keys[len(keys)-1]] = kv.Value
		return nil
	case "yaml", "yml":
		var document yaml.Node
		if err := yaml.Unmarshal(kv.Value, &document); err != nil {
			return fmt.Errorf("conf: decode %s: %w", kv.Key, err)
		}
		if len(document.Content) == 0 {
			return nil
		}
		values, err := fromYAML(document.Content[0], descriptor)
		if err != nil {
			return fmt.Errorf("conf: decode %s: %w", kv.Key, err)
		}
		mapping, ok := values.(map[string]any)
		if !ok {
			return fmt.Errorf("conf: decode %s: the document is not a mapping", kv.Key)
		}
		for key, value := range mapping {
			target[key] = value
		}
		return nil
	default:
		codec := encoding.GetCodec(kv.Format)
		if codec == nil {
			return fmt.Errorf("conf: unsupported format %q of %s", kv.Format, kv.Key)
		}
		return codec.Unmarshal(kv.Value, &target)
	}
}

// fromYAML converts a mapping node read into message, keeping the text of
// scalars read into string fields.
func fromYAML(node *yaml.Node, message protoreflect.MessageDescriptor) (any, error) {
	node = resolveAlias(node)
	if node.Kind != yaml.MappingNode {
		return decodeNode(node)
	}
	values := make(map[string]any, len(node.Content)/2)
	for index := 0; index+1 < len(node.Content); index += 2 {
		key, value := node.Content[index].Value, node.Content[index+1]
		field := lookupField(message, key)
		var err error
		if field == nil {
			values[key], err = decodeNode(value)
		} else {
			values[key], err = fieldFromYAML(value, field)
		}
		if err != nil {
			return nil, fmt.Errorf("%s: %w", key, err)
		}
	}
	return values, nil
}

func fieldFromYAML(node *yaml.Node, field protoreflect.FieldDescriptor) (any, error) {
	node = resolveAlias(node)
	switch {
	case field.IsMap() && node.Kind == yaml.MappingNode:
		values := make(map[string]any, len(node.Content)/2)
		for index := 0; index+1 < len(node.Content); index += 2 {
			value, err := valueFromYAML(node.Content[index+1], field.MapValue())
			if err != nil {
				return nil, err
			}
			values[node.Content[index].Value] = value
		}
		return values, nil
	case field.IsList() && node.Kind == yaml.SequenceNode:
		values := make([]any, len(node.Content))
		for index, element := range node.Content {
			value, err := valueFromYAML(element, field)
			if err != nil {
				return nil, err
			}
			values[index] = value
		}
		return values, nil
	default:
		return valueFromYAML(node, field)
	}
}

func valueFromYAML(node *yaml.Node, field protoreflect.FieldDescriptor) (any, error) {
	node = resolveAlias(node)
	if node.Kind == yaml.ScalarNode && node.Tag == "!!null" {
		return nil, nil
	}
	switch {
	case field.Kind() == protoreflect.StringKind && node.Kind == yaml.ScalarNode:
		return node.Value, nil
	case field.Kind() == protoreflect.MessageKind && isDuration(field.Message()) && node.Kind == yaml.ScalarNode:
		return node.Value, nil
	case field.Kind() == protoreflect.MessageKind:
		return fromYAML(node, field.Message())
	default:
		return decodeNode(node)
	}
}

func decodeNode(node *yaml.Node) (any, error) {
	var value any
	if err := node.Decode(&value); err != nil {
		return nil, err
	}
	return value, nil
}

func resolveAlias(node *yaml.Node) *yaml.Node {
	for node.Kind == yaml.AliasNode && node.Alias != nil {
		node = node.Alias
	}
	return node
}

func lookupField(message protoreflect.MessageDescriptor, key string) protoreflect.FieldDescriptor {
	if field := message.Fields().ByName(protoreflect.Name(key)); field != nil {
		return field
	}
	return message.Fields().ByJSONName(key)
}

func isDuration(message protoreflect.MessageDescriptor) bool {
	return message.FullName() == (&durationpb.Duration{}).ProtoReflect().Descriptor().FullName()
}

// normalize rewrites values in place into what protojson reads: durations
// written as Go durations, such as 5m, become seconds, and booleans written
// as strings, as placeholders resolve to, become booleans.
func normalize(values map[string]any, message protoreflect.MessageDescriptor) {
	for key, value := range values {
		field := lookupField(message, key)
		if field == nil {
			continue
		}
		switch {
		case field.IsMap():
			if mapping, ok := value.(map[string]any); ok {
				for mapKey, mapValue := range mapping {
					mapping[mapKey] = normalizeValue(mapValue, field.MapValue())
				}
			}
		case field.IsList():
			if list, ok := value.([]any); ok {
				for index, element := range list {
					list[index] = normalizeValue(element, field)
				}
			}
		default:
			values[key] = normalizeValue(value, field)
		}
	}
}

func normalizeValue(value any, field protoreflect.FieldDescriptor) any {
	if field.Kind() == protoreflect.MessageKind {
		if isDuration(field.Message()) {
			if text, ok := value.(string); ok {
				if duration, err := time.ParseDuration(strings.TrimSpace(text)); err == nil {
					return strconv.FormatFloat(duration.Seconds(), 'f', -1, 64) + "s"
				}
			}
			return value
		}
		if mapping, ok := value.(map[string]any); ok {
			normalize(mapping, field.Message())
		}
		return value
	}
	if field.Kind() == protoreflect.BoolKind {
		if text, ok := value.(string); ok {
			if parsed, err := strconv.ParseBool(strings.TrimSpace(text)); err == nil {
				return parsed
			}
		}
	}
	return value
}

// bytesSource is a config source of one YAML document that never changes.
type bytesSource []byte

func (source bytesSource) Load() ([]*config.KeyValue, error) {
	return []*config.KeyValue{{Key: "config.yaml", Value: source, Format: "yaml"}}, nil
}

func (bytesSource) Watch() (config.Watcher, error) { return newIdleWatcher(), nil }

// idleWatcher reports no changes until it is stopped.
type idleWatcher struct{ done chan struct{} }

func newIdleWatcher() *idleWatcher { return &idleWatcher{done: make(chan struct{})} }

func (watcher *idleWatcher) Next() ([]*config.KeyValue, error) {
	<-watcher.done
	return nil, context.Canceled
}

func (watcher *idleWatcher) Stop() error {
	close(watcher.done)
	return nil
}
