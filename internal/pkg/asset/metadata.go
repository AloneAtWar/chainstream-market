package asset

import (
	"bytes"
	"encoding/json"
)

// ParseMetadata reads name, symbol and decimals from chainstream-evm's
// asset.Info JSON. ERC-20 and ERC-721 fields are {"Value": v, "Known": b}
// and only known values count; native assets use plain values.
func ParseMetadata(raw []byte) (name, symbol *string, decimals *uint8) {
	var fields map[string]json.RawMessage
	if len(raw) == 0 || json.Unmarshal(raw, &fields) != nil {
		return nil, nil, nil
	}
	var nameValue, symbolValue string
	var decimalsValue uint8
	if field(fields["Name"], &nameValue) {
		name = &nameValue
	}
	if field(fields["Symbol"], &symbolValue) {
		symbol = &symbolValue
	}
	if field(fields["Decimals"], &decimalsValue) {
		decimals = &decimalsValue
	}
	return name, symbol, decimals
}

func field[T any](raw json.RawMessage, target *T) bool {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || string(raw) == "null" {
		return false
	}
	if raw[0] != '{' {
		return json.Unmarshal(raw, target) == nil
	}
	var optional struct {
		Value json.RawMessage
		Known bool
	}
	if json.Unmarshal(raw, &optional) != nil || !optional.Known {
		return false
	}
	return json.Unmarshal(optional.Value, target) == nil
}
