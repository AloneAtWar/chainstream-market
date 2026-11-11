package rpc

import (
	"bytes"
	"crypto/sha256"
	"errors"

	"github.com/btcsuite/btcutil/base58"
	"github.com/ethereum/go-ethereum/common"
)

// tronAddressPrefix is the version byte of a TRON address payload.
const tronAddressPrefix = 0x41

// tronHexAddress decodes a base58check TRON address (T...) into the 20-byte
// address the TVM's ABI uses.
//
// tronHexAddress 把 base58check 编码的 TRON 地址（T...）解码成 ABI 使用的 20 字节地址。
func tronHexAddress(address string) (common.Address, error) {
	decoded := base58.Decode(address)
	if len(decoded) != 1+common.AddressLength+4 || decoded[0] != tronAddressPrefix {
		return common.Address{}, errors.New("not a TRON address")
	}
	payload, checksum := decoded[:1+common.AddressLength], decoded[1+common.AddressLength:]
	first := sha256.Sum256(payload)
	second := sha256.Sum256(first[:])
	if !bytes.Equal(second[:4], checksum) {
		return common.Address{}, errors.New("bad TRON address checksum")
	}
	return common.BytesToAddress(payload[1:]), nil
}
