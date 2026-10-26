// Code generated via abigen V2 - DO NOT EDIT.
// This file is a generated binding and any manual changes will be lost.

package aggregatorproxy

import (
	"bytes"
	"errors"
	"math/big"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind/v2"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// Reference imports to suppress errors if they are not otherwise used.
var (
	_ = bytes.Equal
	_ = errors.New
	_ = big.NewInt
	_ = common.Big1
	_ = types.BloomLookup
	_ = abi.ConvertType
)

// ProxyMetaData contains all meta data concerning the Proxy contract.
var ProxyMetaData = bind.MetaData{
	ABI: "[{\"inputs\":[{\"internalType\":\"address\",\"name\":\"_aggregator\",\"type\":\"address\"},{\"internalType\":\"address\",\"name\":\"_accessController\",\"type\":\"address\"}],\"stateMutability\":\"nonpayable\",\"type\":\"constructor\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":true,\"internalType\":\"int256\",\"name\":\"current\",\"type\":\"int256\"},{\"indexed\":true,\"internalType\":\"uint256\",\"name\":\"roundId\",\"type\":\"uint256\"},{\"indexed\":false,\"internalType\":\"uint256\",\"name\":\"updatedAt\",\"type\":\"uint256\"}],\"name\":\"AnswerUpdated\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":true,\"internalType\":\"uint256\",\"name\":\"roundId\",\"type\":\"uint256\"},{\"indexed\":true,\"internalType\":\"address\",\"name\":\"startedBy\",\"type\":\"address\"},{\"indexed\":false,\"internalType\":\"uint256\",\"name\":\"startedAt\",\"type\":\"uint256\"}],\"name\":\"NewRound\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":true,\"internalType\":\"address\",\"name\":\"from\",\"type\":\"address\"},{\"indexed\":true,\"internalType\":\"address\",\"name\":\"to\",\"type\":\"address\"}],\"name\":\"OwnershipTransferRequested\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":true,\"internalType\":\"address\",\"name\":\"from\",\"type\":\"address\"},{\"indexed\":true,\"internalType\":\"address\",\"name\":\"to\",\"type\":\"address\"}],\"name\":\"OwnershipTransferred\",\"type\":\"event\"},{\"inputs\":[],\"name\":\"acceptOwnership\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"accessController\",\"outputs\":[{\"internalType\":\"contractAccessControllerInterface\",\"name\":\"\",\"type\":\"address\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"aggregator\",\"outputs\":[{\"internalType\":\"address\",\"name\":\"\",\"type\":\"address\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"_aggregator\",\"type\":\"address\"}],\"name\":\"confirmAggregator\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"decimals\",\"outputs\":[{\"internalType\":\"uint8\",\"name\":\"\",\"type\":\"uint8\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"description\",\"outputs\":[{\"internalType\":\"string\",\"name\":\"\",\"type\":\"string\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"uint256\",\"name\":\"_roundId\",\"type\":\"uint256\"}],\"name\":\"getAnswer\",\"outputs\":[{\"internalType\":\"int256\",\"name\":\"\",\"type\":\"int256\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"uint80\",\"name\":\"_roundId\",\"type\":\"uint80\"}],\"name\":\"getRoundData\",\"outputs\":[{\"internalType\":\"uint80\",\"name\":\"roundId\",\"type\":\"uint80\"},{\"internalType\":\"int256\",\"name\":\"answer\",\"type\":\"int256\"},{\"internalType\":\"uint256\",\"name\":\"startedAt\",\"type\":\"uint256\"},{\"internalType\":\"uint256\",\"name\":\"updatedAt\",\"type\":\"uint256\"},{\"internalType\":\"uint80\",\"name\":\"answeredInRound\",\"type\":\"uint80\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"uint256\",\"name\":\"_roundId\",\"type\":\"uint256\"}],\"name\":\"getTimestamp\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"latestAnswer\",\"outputs\":[{\"internalType\":\"int256\",\"name\":\"\",\"type\":\"int256\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"latestRound\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"latestRoundData\",\"outputs\":[{\"internalType\":\"uint80\",\"name\":\"roundId\",\"type\":\"uint80\"},{\"internalType\":\"int256\",\"name\":\"answer\",\"type\":\"int256\"},{\"internalType\":\"uint256\",\"name\":\"startedAt\",\"type\":\"uint256\"},{\"internalType\":\"uint256\",\"name\":\"updatedAt\",\"type\":\"uint256\"},{\"internalType\":\"uint80\",\"name\":\"answeredInRound\",\"type\":\"uint80\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"latestTimestamp\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"owner\",\"outputs\":[{\"internalType\":\"address\",\"name\":\"\",\"type\":\"address\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"uint16\",\"name\":\"\",\"type\":\"uint16\"}],\"name\":\"phaseAggregators\",\"outputs\":[{\"internalType\":\"contractAggregatorV2V3Interface\",\"name\":\"\",\"type\":\"address\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"phaseId\",\"outputs\":[{\"internalType\":\"uint16\",\"name\":\"\",\"type\":\"uint16\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"_aggregator\",\"type\":\"address\"}],\"name\":\"proposeAggregator\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"proposedAggregator\",\"outputs\":[{\"internalType\":\"contractAggregatorV2V3Interface\",\"name\":\"\",\"type\":\"address\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"uint80\",\"name\":\"_roundId\",\"type\":\"uint80\"}],\"name\":\"proposedGetRoundData\",\"outputs\":[{\"internalType\":\"uint80\",\"name\":\"roundId\",\"type\":\"uint80\"},{\"internalType\":\"int256\",\"name\":\"answer\",\"type\":\"int256\"},{\"internalType\":\"uint256\",\"name\":\"startedAt\",\"type\":\"uint256\"},{\"internalType\":\"uint256\",\"name\":\"updatedAt\",\"type\":\"uint256\"},{\"internalType\":\"uint80\",\"name\":\"answeredInRound\",\"type\":\"uint80\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"proposedLatestRoundData\",\"outputs\":[{\"internalType\":\"uint80\",\"name\":\"roundId\",\"type\":\"uint80\"},{\"internalType\":\"int256\",\"name\":\"answer\",\"type\":\"int256\"},{\"internalType\":\"uint256\",\"name\":\"startedAt\",\"type\":\"uint256\"},{\"internalType\":\"uint256\",\"name\":\"updatedAt\",\"type\":\"uint256\"},{\"internalType\":\"uint80\",\"name\":\"answeredInRound\",\"type\":\"uint80\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"_accessController\",\"type\":\"address\"}],\"name\":\"setController\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"_to\",\"type\":\"address\"}],\"name\":\"transferOwnership\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"version\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\"}]",
	ID:  "Proxy",
}

// Proxy is an auto generated Go binding around an Ethereum contract.
type Proxy struct {
	abi abi.ABI
}

// NewProxy creates a new instance of Proxy.
func NewProxy() *Proxy {
	parsed, err := ProxyMetaData.ParseABI()
	if err != nil {
		panic(errors.New("invalid ABI: " + err.Error()))
	}
	return &Proxy{abi: *parsed}
}

// Instance creates a wrapper for a deployed contract instance at the given address.
// Use this to create the instance object passed to abigen v2 library functions Call, Transact, etc.
func (c *Proxy) Instance(backend bind.ContractBackend, addr common.Address) *bind.BoundContract {
	return bind.NewBoundContract(addr, c.abi, backend, backend, backend)
}

// PackConstructor is the Go binding used to pack the parameters required for
// contract deployment.
//
// Solidity: constructor(address _aggregator, address _accessController) returns()
func (proxy *Proxy) PackConstructor(_aggregator common.Address, _accessController common.Address) []byte {
	enc, err := proxy.abi.Pack("", _aggregator, _accessController)
	if err != nil {
		panic(err)
	}
	return enc
}

// PackAcceptOwnership is the Go binding used to pack the parameters required for calling
// the contract method with ID 0x79ba5097.  This method will panic if any
// invalid/nil inputs are passed.
//
// Solidity: function acceptOwnership() returns()
func (proxy *Proxy) PackAcceptOwnership() []byte {
	enc, err := proxy.abi.Pack("acceptOwnership")
	if err != nil {
		panic(err)
	}
	return enc
}

// TryPackAcceptOwnership is the Go binding used to pack the parameters required for calling
// the contract method with ID 0x79ba5097.  This method will return an error
// if any inputs are invalid/nil.
//
// Solidity: function acceptOwnership() returns()
func (proxy *Proxy) TryPackAcceptOwnership() ([]byte, error) {
	return proxy.abi.Pack("acceptOwnership")
}

// PackAccessController is the Go binding used to pack the parameters required for calling
// the contract method with ID 0xbc43cbaf.  This method will panic if any
// invalid/nil inputs are passed.
//
// Solidity: function accessController() view returns(address)
func (proxy *Proxy) PackAccessController() []byte {
	enc, err := proxy.abi.Pack("accessController")
	if err != nil {
		panic(err)
	}
	return enc
}

// TryPackAccessController is the Go binding used to pack the parameters required for calling
// the contract method with ID 0xbc43cbaf.  This method will return an error
// if any inputs are invalid/nil.
//
// Solidity: function accessController() view returns(address)
func (proxy *Proxy) TryPackAccessController() ([]byte, error) {
	return proxy.abi.Pack("accessController")
}

// UnpackAccessController is the Go binding that unpacks the parameters returned
// from invoking the contract method with ID 0xbc43cbaf.
//
// Solidity: function accessController() view returns(address)
func (proxy *Proxy) UnpackAccessController(data []byte) (common.Address, error) {
	out, err := proxy.abi.Unpack("accessController", data)
	if err != nil {
		return *new(common.Address), err
	}
	out0 := *abi.ConvertType(out[0], new(common.Address)).(*common.Address)
	return out0, nil
}

// PackAggregator is the Go binding used to pack the parameters required for calling
// the contract method with ID 0x245a7bfc.  This method will panic if any
// invalid/nil inputs are passed.
//
// Solidity: function aggregator() view returns(address)
func (proxy *Proxy) PackAggregator() []byte {
	enc, err := proxy.abi.Pack("aggregator")
	if err != nil {
		panic(err)
	}
	return enc
}

// TryPackAggregator is the Go binding used to pack the parameters required for calling
// the contract method with ID 0x245a7bfc.  This method will return an error
// if any inputs are invalid/nil.
//
// Solidity: function aggregator() view returns(address)
func (proxy *Proxy) TryPackAggregator() ([]byte, error) {
	return proxy.abi.Pack("aggregator")
}

// UnpackAggregator is the Go binding that unpacks the parameters returned
// from invoking the contract method with ID 0x245a7bfc.
//
// Solidity: function aggregator() view returns(address)
func (proxy *Proxy) UnpackAggregator(data []byte) (common.Address, error) {
	out, err := proxy.abi.Unpack("aggregator", data)
	if err != nil {
		return *new(common.Address), err
	}
	out0 := *abi.ConvertType(out[0], new(common.Address)).(*common.Address)
	return out0, nil
}

// PackConfirmAggregator is the Go binding used to pack the parameters required for calling
// the contract method with ID 0xa928c096.  This method will panic if any
// invalid/nil inputs are passed.
//
// Solidity: function confirmAggregator(address _aggregator) returns()
func (proxy *Proxy) PackConfirmAggregator(aggregator common.Address) []byte {
	enc, err := proxy.abi.Pack("confirmAggregator", aggregator)
	if err != nil {
		panic(err)
	}
	return enc
}

// TryPackConfirmAggregator is the Go binding used to pack the parameters required for calling
// the contract method with ID 0xa928c096.  This method will return an error
// if any inputs are invalid/nil.
//
// Solidity: function confirmAggregator(address _aggregator) returns()
func (proxy *Proxy) TryPackConfirmAggregator(aggregator common.Address) ([]byte, error) {
	return proxy.abi.Pack("confirmAggregator", aggregator)
}

// PackDecimals is the Go binding used to pack the parameters required for calling
// the contract method with ID 0x313ce567.  This method will panic if any
// invalid/nil inputs are passed.
//
// Solidity: function decimals() view returns(uint8)
func (proxy *Proxy) PackDecimals() []byte {
	enc, err := proxy.abi.Pack("decimals")
	if err != nil {
		panic(err)
	}
	return enc
}

// TryPackDecimals is the Go binding used to pack the parameters required for calling
// the contract method with ID 0x313ce567.  This method will return an error
// if any inputs are invalid/nil.
//
// Solidity: function decimals() view returns(uint8)
func (proxy *Proxy) TryPackDecimals() ([]byte, error) {
	return proxy.abi.Pack("decimals")
}

// UnpackDecimals is the Go binding that unpacks the parameters returned
// from invoking the contract method with ID 0x313ce567.
//
// Solidity: function decimals() view returns(uint8)
func (proxy *Proxy) UnpackDecimals(data []byte) (uint8, error) {
	out, err := proxy.abi.Unpack("decimals", data)
	if err != nil {
		return *new(uint8), err
	}
	out0 := *abi.ConvertType(out[0], new(uint8)).(*uint8)
	return out0, nil
}

// PackDescription is the Go binding used to pack the parameters required for calling
// the contract method with ID 0x7284e416.  This method will panic if any
// invalid/nil inputs are passed.
//
// Solidity: function description() view returns(string)
func (proxy *Proxy) PackDescription() []byte {
	enc, err := proxy.abi.Pack("description")
	if err != nil {
		panic(err)
	}
	return enc
}

// TryPackDescription is the Go binding used to pack the parameters required for calling
// the contract method with ID 0x7284e416.  This method will return an error
// if any inputs are invalid/nil.
//
// Solidity: function description() view returns(string)
func (proxy *Proxy) TryPackDescription() ([]byte, error) {
	return proxy.abi.Pack("description")
}

// UnpackDescription is the Go binding that unpacks the parameters returned
// from invoking the contract method with ID 0x7284e416.
//
// Solidity: function description() view returns(string)
func (proxy *Proxy) UnpackDescription(data []byte) (string, error) {
	out, err := proxy.abi.Unpack("description", data)
	if err != nil {
		return *new(string), err
	}
	out0 := *abi.ConvertType(out[0], new(string)).(*string)
	return out0, nil
}

// PackGetAnswer is the Go binding used to pack the parameters required for calling
// the contract method with ID 0xb5ab58dc.  This method will panic if any
// invalid/nil inputs are passed.
//
// Solidity: function getAnswer(uint256 _roundId) view returns(int256)
func (proxy *Proxy) PackGetAnswer(roundId *big.Int) []byte {
	enc, err := proxy.abi.Pack("getAnswer", roundId)
	if err != nil {
		panic(err)
	}
	return enc
}

// TryPackGetAnswer is the Go binding used to pack the parameters required for calling
// the contract method with ID 0xb5ab58dc.  This method will return an error
// if any inputs are invalid/nil.
//
// Solidity: function getAnswer(uint256 _roundId) view returns(int256)
func (proxy *Proxy) TryPackGetAnswer(roundId *big.Int) ([]byte, error) {
	return proxy.abi.Pack("getAnswer", roundId)
}

// UnpackGetAnswer is the Go binding that unpacks the parameters returned
// from invoking the contract method with ID 0xb5ab58dc.
//
// Solidity: function getAnswer(uint256 _roundId) view returns(int256)
func (proxy *Proxy) UnpackGetAnswer(data []byte) (*big.Int, error) {
	out, err := proxy.abi.Unpack("getAnswer", data)
	if err != nil {
		return new(big.Int), err
	}
	out0 := abi.ConvertType(out[0], new(big.Int)).(*big.Int)
	return out0, nil
}

// PackGetRoundData is the Go binding used to pack the parameters required for calling
// the contract method with ID 0x9a6fc8f5.  This method will panic if any
// invalid/nil inputs are passed.
//
// Solidity: function getRoundData(uint80 _roundId) view returns(uint80 roundId, int256 answer, uint256 startedAt, uint256 updatedAt, uint80 answeredInRound)
func (proxy *Proxy) PackGetRoundData(roundId *big.Int) []byte {
	enc, err := proxy.abi.Pack("getRoundData", roundId)
	if err != nil {
		panic(err)
	}
	return enc
}

// TryPackGetRoundData is the Go binding used to pack the parameters required for calling
// the contract method with ID 0x9a6fc8f5.  This method will return an error
// if any inputs are invalid/nil.
//
// Solidity: function getRoundData(uint80 _roundId) view returns(uint80 roundId, int256 answer, uint256 startedAt, uint256 updatedAt, uint80 answeredInRound)
func (proxy *Proxy) TryPackGetRoundData(roundId *big.Int) ([]byte, error) {
	return proxy.abi.Pack("getRoundData", roundId)
}

// GetRoundDataOutput serves as a container for the return parameters of contract
// method GetRoundData.
type GetRoundDataOutput struct {
	RoundId         *big.Int
	Answer          *big.Int
	StartedAt       *big.Int
	UpdatedAt       *big.Int
	AnsweredInRound *big.Int
}

// UnpackGetRoundData is the Go binding that unpacks the parameters returned
// from invoking the contract method with ID 0x9a6fc8f5.
//
// Solidity: function getRoundData(uint80 _roundId) view returns(uint80 roundId, int256 answer, uint256 startedAt, uint256 updatedAt, uint80 answeredInRound)
func (proxy *Proxy) UnpackGetRoundData(data []byte) (GetRoundDataOutput, error) {
	out, err := proxy.abi.Unpack("getRoundData", data)
	outstruct := new(GetRoundDataOutput)
	if err != nil {
		return *outstruct, err
	}
	outstruct.RoundId = abi.ConvertType(out[0], new(big.Int)).(*big.Int)
	outstruct.Answer = abi.ConvertType(out[1], new(big.Int)).(*big.Int)
	outstruct.StartedAt = abi.ConvertType(out[2], new(big.Int)).(*big.Int)
	outstruct.UpdatedAt = abi.ConvertType(out[3], new(big.Int)).(*big.Int)
	outstruct.AnsweredInRound = abi.ConvertType(out[4], new(big.Int)).(*big.Int)
	return *outstruct, nil
}

// PackGetTimestamp is the Go binding used to pack the parameters required for calling
// the contract method with ID 0xb633620c.  This method will panic if any
// invalid/nil inputs are passed.
//
// Solidity: function getTimestamp(uint256 _roundId) view returns(uint256)
func (proxy *Proxy) PackGetTimestamp(roundId *big.Int) []byte {
	enc, err := proxy.abi.Pack("getTimestamp", roundId)
	if err != nil {
		panic(err)
	}
	return enc
}

// TryPackGetTimestamp is the Go binding used to pack the parameters required for calling
// the contract method with ID 0xb633620c.  This method will return an error
// if any inputs are invalid/nil.
//
// Solidity: function getTimestamp(uint256 _roundId) view returns(uint256)
func (proxy *Proxy) TryPackGetTimestamp(roundId *big.Int) ([]byte, error) {
	return proxy.abi.Pack("getTimestamp", roundId)
}

// UnpackGetTimestamp is the Go binding that unpacks the parameters returned
// from invoking the contract method with ID 0xb633620c.
//
// Solidity: function getTimestamp(uint256 _roundId) view returns(uint256)
func (proxy *Proxy) UnpackGetTimestamp(data []byte) (*big.Int, error) {
	out, err := proxy.abi.Unpack("getTimestamp", data)
	if err != nil {
		return new(big.Int), err
	}
	out0 := abi.ConvertType(out[0], new(big.Int)).(*big.Int)
	return out0, nil
}

// PackLatestAnswer is the Go binding used to pack the parameters required for calling
// the contract method with ID 0x50d25bcd.  This method will panic if any
// invalid/nil inputs are passed.
//
// Solidity: function latestAnswer() view returns(int256)
func (proxy *Proxy) PackLatestAnswer() []byte {
	enc, err := proxy.abi.Pack("latestAnswer")
	if err != nil {
		panic(err)
	}
	return enc
}

// TryPackLatestAnswer is the Go binding used to pack the parameters required for calling
// the contract method with ID 0x50d25bcd.  This method will return an error
// if any inputs are invalid/nil.
//
// Solidity: function latestAnswer() view returns(int256)
func (proxy *Proxy) TryPackLatestAnswer() ([]byte, error) {
	return proxy.abi.Pack("latestAnswer")
}

// UnpackLatestAnswer is the Go binding that unpacks the parameters returned
// from invoking the contract method with ID 0x50d25bcd.
//
// Solidity: function latestAnswer() view returns(int256)
func (proxy *Proxy) UnpackLatestAnswer(data []byte) (*big.Int, error) {
	out, err := proxy.abi.Unpack("latestAnswer", data)
	if err != nil {
		return new(big.Int), err
	}
	out0 := abi.ConvertType(out[0], new(big.Int)).(*big.Int)
	return out0, nil
}

// PackLatestRound is the Go binding used to pack the parameters required for calling
// the contract method with ID 0x668a0f02.  This method will panic if any
// invalid/nil inputs are passed.
//
// Solidity: function latestRound() view returns(uint256)
func (proxy *Proxy) PackLatestRound() []byte {
	enc, err := proxy.abi.Pack("latestRound")
	if err != nil {
		panic(err)
	}
	return enc
}

// TryPackLatestRound is the Go binding used to pack the parameters required for calling
// the contract method with ID 0x668a0f02.  This method will return an error
// if any inputs are invalid/nil.
//
// Solidity: function latestRound() view returns(uint256)
func (proxy *Proxy) TryPackLatestRound() ([]byte, error) {
	return proxy.abi.Pack("latestRound")
}

// UnpackLatestRound is the Go binding that unpacks the parameters returned
// from invoking the contract method with ID 0x668a0f02.
//
// Solidity: function latestRound() view returns(uint256)
func (proxy *Proxy) UnpackLatestRound(data []byte) (*big.Int, error) {
	out, err := proxy.abi.Unpack("latestRound", data)
	if err != nil {
		return new(big.Int), err
	}
	out0 := abi.ConvertType(out[0], new(big.Int)).(*big.Int)
	return out0, nil
}

// PackLatestRoundData is the Go binding used to pack the parameters required for calling
// the contract method with ID 0xfeaf968c.  This method will panic if any
// invalid/nil inputs are passed.
//
// Solidity: function latestRoundData() view returns(uint80 roundId, int256 answer, uint256 startedAt, uint256 updatedAt, uint80 answeredInRound)
func (proxy *Proxy) PackLatestRoundData() []byte {
	enc, err := proxy.abi.Pack("latestRoundData")
	if err != nil {
		panic(err)
	}
	return enc
}

// TryPackLatestRoundData is the Go binding used to pack the parameters required for calling
// the contract method with ID 0xfeaf968c.  This method will return an error
// if any inputs are invalid/nil.
//
// Solidity: function latestRoundData() view returns(uint80 roundId, int256 answer, uint256 startedAt, uint256 updatedAt, uint80 answeredInRound)
func (proxy *Proxy) TryPackLatestRoundData() ([]byte, error) {
	return proxy.abi.Pack("latestRoundData")
}

// LatestRoundDataOutput serves as a container for the return parameters of contract
// method LatestRoundData.
type LatestRoundDataOutput struct {
	RoundId         *big.Int
	Answer          *big.Int
	StartedAt       *big.Int
	UpdatedAt       *big.Int
	AnsweredInRound *big.Int
}

// UnpackLatestRoundData is the Go binding that unpacks the parameters returned
// from invoking the contract method with ID 0xfeaf968c.
//
// Solidity: function latestRoundData() view returns(uint80 roundId, int256 answer, uint256 startedAt, uint256 updatedAt, uint80 answeredInRound)
func (proxy *Proxy) UnpackLatestRoundData(data []byte) (LatestRoundDataOutput, error) {
	out, err := proxy.abi.Unpack("latestRoundData", data)
	outstruct := new(LatestRoundDataOutput)
	if err != nil {
		return *outstruct, err
	}
	outstruct.RoundId = abi.ConvertType(out[0], new(big.Int)).(*big.Int)
	outstruct.Answer = abi.ConvertType(out[1], new(big.Int)).(*big.Int)
	outstruct.StartedAt = abi.ConvertType(out[2], new(big.Int)).(*big.Int)
	outstruct.UpdatedAt = abi.ConvertType(out[3], new(big.Int)).(*big.Int)
	outstruct.AnsweredInRound = abi.ConvertType(out[4], new(big.Int)).(*big.Int)
	return *outstruct, nil
}

// PackLatestTimestamp is the Go binding used to pack the parameters required for calling
// the contract method with ID 0x8205bf6a.  This method will panic if any
// invalid/nil inputs are passed.
//
// Solidity: function latestTimestamp() view returns(uint256)
func (proxy *Proxy) PackLatestTimestamp() []byte {
	enc, err := proxy.abi.Pack("latestTimestamp")
	if err != nil {
		panic(err)
	}
	return enc
}

// TryPackLatestTimestamp is the Go binding used to pack the parameters required for calling
// the contract method with ID 0x8205bf6a.  This method will return an error
// if any inputs are invalid/nil.
//
// Solidity: function latestTimestamp() view returns(uint256)
func (proxy *Proxy) TryPackLatestTimestamp() ([]byte, error) {
	return proxy.abi.Pack("latestTimestamp")
}

// UnpackLatestTimestamp is the Go binding that unpacks the parameters returned
// from invoking the contract method with ID 0x8205bf6a.
//
// Solidity: function latestTimestamp() view returns(uint256)
func (proxy *Proxy) UnpackLatestTimestamp(data []byte) (*big.Int, error) {
	out, err := proxy.abi.Unpack("latestTimestamp", data)
	if err != nil {
		return new(big.Int), err
	}
	out0 := abi.ConvertType(out[0], new(big.Int)).(*big.Int)
	return out0, nil
}

// PackOwner is the Go binding used to pack the parameters required for calling
// the contract method with ID 0x8da5cb5b.  This method will panic if any
// invalid/nil inputs are passed.
//
// Solidity: function owner() view returns(address)
func (proxy *Proxy) PackOwner() []byte {
	enc, err := proxy.abi.Pack("owner")
	if err != nil {
		panic(err)
	}
	return enc
}

// TryPackOwner is the Go binding used to pack the parameters required for calling
// the contract method with ID 0x8da5cb5b.  This method will return an error
// if any inputs are invalid/nil.
//
// Solidity: function owner() view returns(address)
func (proxy *Proxy) TryPackOwner() ([]byte, error) {
	return proxy.abi.Pack("owner")
}

// UnpackOwner is the Go binding that unpacks the parameters returned
// from invoking the contract method with ID 0x8da5cb5b.
//
// Solidity: function owner() view returns(address)
func (proxy *Proxy) UnpackOwner(data []byte) (common.Address, error) {
	out, err := proxy.abi.Unpack("owner", data)
	if err != nil {
		return *new(common.Address), err
	}
	out0 := *abi.ConvertType(out[0], new(common.Address)).(*common.Address)
	return out0, nil
}

// PackPhaseAggregators is the Go binding used to pack the parameters required for calling
// the contract method with ID 0xc1597304.  This method will panic if any
// invalid/nil inputs are passed.
//
// Solidity: function phaseAggregators(uint16 ) view returns(address)
func (proxy *Proxy) PackPhaseAggregators(arg0 uint16) []byte {
	enc, err := proxy.abi.Pack("phaseAggregators", arg0)
	if err != nil {
		panic(err)
	}
	return enc
}

// TryPackPhaseAggregators is the Go binding used to pack the parameters required for calling
// the contract method with ID 0xc1597304.  This method will return an error
// if any inputs are invalid/nil.
//
// Solidity: function phaseAggregators(uint16 ) view returns(address)
func (proxy *Proxy) TryPackPhaseAggregators(arg0 uint16) ([]byte, error) {
	return proxy.abi.Pack("phaseAggregators", arg0)
}

// UnpackPhaseAggregators is the Go binding that unpacks the parameters returned
// from invoking the contract method with ID 0xc1597304.
//
// Solidity: function phaseAggregators(uint16 ) view returns(address)
func (proxy *Proxy) UnpackPhaseAggregators(data []byte) (common.Address, error) {
	out, err := proxy.abi.Unpack("phaseAggregators", data)
	if err != nil {
		return *new(common.Address), err
	}
	out0 := *abi.ConvertType(out[0], new(common.Address)).(*common.Address)
	return out0, nil
}

// PackPhaseId is the Go binding used to pack the parameters required for calling
// the contract method with ID 0x58303b10.  This method will panic if any
// invalid/nil inputs are passed.
//
// Solidity: function phaseId() view returns(uint16)
func (proxy *Proxy) PackPhaseId() []byte {
	enc, err := proxy.abi.Pack("phaseId")
	if err != nil {
		panic(err)
	}
	return enc
}

// TryPackPhaseId is the Go binding used to pack the parameters required for calling
// the contract method with ID 0x58303b10.  This method will return an error
// if any inputs are invalid/nil.
//
// Solidity: function phaseId() view returns(uint16)
func (proxy *Proxy) TryPackPhaseId() ([]byte, error) {
	return proxy.abi.Pack("phaseId")
}

// UnpackPhaseId is the Go binding that unpacks the parameters returned
// from invoking the contract method with ID 0x58303b10.
//
// Solidity: function phaseId() view returns(uint16)
func (proxy *Proxy) UnpackPhaseId(data []byte) (uint16, error) {
	out, err := proxy.abi.Unpack("phaseId", data)
	if err != nil {
		return *new(uint16), err
	}
	out0 := *abi.ConvertType(out[0], new(uint16)).(*uint16)
	return out0, nil
}

// PackProposeAggregator is the Go binding used to pack the parameters required for calling
// the contract method with ID 0xf8a2abd3.  This method will panic if any
// invalid/nil inputs are passed.
//
// Solidity: function proposeAggregator(address _aggregator) returns()
func (proxy *Proxy) PackProposeAggregator(aggregator common.Address) []byte {
	enc, err := proxy.abi.Pack("proposeAggregator", aggregator)
	if err != nil {
		panic(err)
	}
	return enc
}

// TryPackProposeAggregator is the Go binding used to pack the parameters required for calling
// the contract method with ID 0xf8a2abd3.  This method will return an error
// if any inputs are invalid/nil.
//
// Solidity: function proposeAggregator(address _aggregator) returns()
func (proxy *Proxy) TryPackProposeAggregator(aggregator common.Address) ([]byte, error) {
	return proxy.abi.Pack("proposeAggregator", aggregator)
}

// PackProposedAggregator is the Go binding used to pack the parameters required for calling
// the contract method with ID 0xe8c4be30.  This method will panic if any
// invalid/nil inputs are passed.
//
// Solidity: function proposedAggregator() view returns(address)
func (proxy *Proxy) PackProposedAggregator() []byte {
	enc, err := proxy.abi.Pack("proposedAggregator")
	if err != nil {
		panic(err)
	}
	return enc
}

// TryPackProposedAggregator is the Go binding used to pack the parameters required for calling
// the contract method with ID 0xe8c4be30.  This method will return an error
// if any inputs are invalid/nil.
//
// Solidity: function proposedAggregator() view returns(address)
func (proxy *Proxy) TryPackProposedAggregator() ([]byte, error) {
	return proxy.abi.Pack("proposedAggregator")
}

// UnpackProposedAggregator is the Go binding that unpacks the parameters returned
// from invoking the contract method with ID 0xe8c4be30.
//
// Solidity: function proposedAggregator() view returns(address)
func (proxy *Proxy) UnpackProposedAggregator(data []byte) (common.Address, error) {
	out, err := proxy.abi.Unpack("proposedAggregator", data)
	if err != nil {
		return *new(common.Address), err
	}
	out0 := *abi.ConvertType(out[0], new(common.Address)).(*common.Address)
	return out0, nil
}

// PackProposedGetRoundData is the Go binding used to pack the parameters required for calling
// the contract method with ID 0x6001ac53.  This method will panic if any
// invalid/nil inputs are passed.
//
// Solidity: function proposedGetRoundData(uint80 _roundId) view returns(uint80 roundId, int256 answer, uint256 startedAt, uint256 updatedAt, uint80 answeredInRound)
func (proxy *Proxy) PackProposedGetRoundData(roundId *big.Int) []byte {
	enc, err := proxy.abi.Pack("proposedGetRoundData", roundId)
	if err != nil {
		panic(err)
	}
	return enc
}

// TryPackProposedGetRoundData is the Go binding used to pack the parameters required for calling
// the contract method with ID 0x6001ac53.  This method will return an error
// if any inputs are invalid/nil.
//
// Solidity: function proposedGetRoundData(uint80 _roundId) view returns(uint80 roundId, int256 answer, uint256 startedAt, uint256 updatedAt, uint80 answeredInRound)
func (proxy *Proxy) TryPackProposedGetRoundData(roundId *big.Int) ([]byte, error) {
	return proxy.abi.Pack("proposedGetRoundData", roundId)
}

// ProposedGetRoundDataOutput serves as a container for the return parameters of contract
// method ProposedGetRoundData.
type ProposedGetRoundDataOutput struct {
	RoundId         *big.Int
	Answer          *big.Int
	StartedAt       *big.Int
	UpdatedAt       *big.Int
	AnsweredInRound *big.Int
}

// UnpackProposedGetRoundData is the Go binding that unpacks the parameters returned
// from invoking the contract method with ID 0x6001ac53.
//
// Solidity: function proposedGetRoundData(uint80 _roundId) view returns(uint80 roundId, int256 answer, uint256 startedAt, uint256 updatedAt, uint80 answeredInRound)
func (proxy *Proxy) UnpackProposedGetRoundData(data []byte) (ProposedGetRoundDataOutput, error) {
	out, err := proxy.abi.Unpack("proposedGetRoundData", data)
	outstruct := new(ProposedGetRoundDataOutput)
	if err != nil {
		return *outstruct, err
	}
	outstruct.RoundId = abi.ConvertType(out[0], new(big.Int)).(*big.Int)
	outstruct.Answer = abi.ConvertType(out[1], new(big.Int)).(*big.Int)
	outstruct.StartedAt = abi.ConvertType(out[2], new(big.Int)).(*big.Int)
	outstruct.UpdatedAt = abi.ConvertType(out[3], new(big.Int)).(*big.Int)
	outstruct.AnsweredInRound = abi.ConvertType(out[4], new(big.Int)).(*big.Int)
	return *outstruct, nil
}

// PackProposedLatestRoundData is the Go binding used to pack the parameters required for calling
// the contract method with ID 0x8f6b4d91.  This method will panic if any
// invalid/nil inputs are passed.
//
// Solidity: function proposedLatestRoundData() view returns(uint80 roundId, int256 answer, uint256 startedAt, uint256 updatedAt, uint80 answeredInRound)
func (proxy *Proxy) PackProposedLatestRoundData() []byte {
	enc, err := proxy.abi.Pack("proposedLatestRoundData")
	if err != nil {
		panic(err)
	}
	return enc
}

// TryPackProposedLatestRoundData is the Go binding used to pack the parameters required for calling
// the contract method with ID 0x8f6b4d91.  This method will return an error
// if any inputs are invalid/nil.
//
// Solidity: function proposedLatestRoundData() view returns(uint80 roundId, int256 answer, uint256 startedAt, uint256 updatedAt, uint80 answeredInRound)
func (proxy *Proxy) TryPackProposedLatestRoundData() ([]byte, error) {
	return proxy.abi.Pack("proposedLatestRoundData")
}

// ProposedLatestRoundDataOutput serves as a container for the return parameters of contract
// method ProposedLatestRoundData.
type ProposedLatestRoundDataOutput struct {
	RoundId         *big.Int
	Answer          *big.Int
	StartedAt       *big.Int
	UpdatedAt       *big.Int
	AnsweredInRound *big.Int
}

// UnpackProposedLatestRoundData is the Go binding that unpacks the parameters returned
// from invoking the contract method with ID 0x8f6b4d91.
//
// Solidity: function proposedLatestRoundData() view returns(uint80 roundId, int256 answer, uint256 startedAt, uint256 updatedAt, uint80 answeredInRound)
func (proxy *Proxy) UnpackProposedLatestRoundData(data []byte) (ProposedLatestRoundDataOutput, error) {
	out, err := proxy.abi.Unpack("proposedLatestRoundData", data)
	outstruct := new(ProposedLatestRoundDataOutput)
	if err != nil {
		return *outstruct, err
	}
	outstruct.RoundId = abi.ConvertType(out[0], new(big.Int)).(*big.Int)
	outstruct.Answer = abi.ConvertType(out[1], new(big.Int)).(*big.Int)
	outstruct.StartedAt = abi.ConvertType(out[2], new(big.Int)).(*big.Int)
	outstruct.UpdatedAt = abi.ConvertType(out[3], new(big.Int)).(*big.Int)
	outstruct.AnsweredInRound = abi.ConvertType(out[4], new(big.Int)).(*big.Int)
	return *outstruct, nil
}

// PackSetController is the Go binding used to pack the parameters required for calling
// the contract method with ID 0x92eefe9b.  This method will panic if any
// invalid/nil inputs are passed.
//
// Solidity: function setController(address _accessController) returns()
func (proxy *Proxy) PackSetController(accessController common.Address) []byte {
	enc, err := proxy.abi.Pack("setController", accessController)
	if err != nil {
		panic(err)
	}
	return enc
}

// TryPackSetController is the Go binding used to pack the parameters required for calling
// the contract method with ID 0x92eefe9b.  This method will return an error
// if any inputs are invalid/nil.
//
// Solidity: function setController(address _accessController) returns()
func (proxy *Proxy) TryPackSetController(accessController common.Address) ([]byte, error) {
	return proxy.abi.Pack("setController", accessController)
}

// PackTransferOwnership is the Go binding used to pack the parameters required for calling
// the contract method with ID 0xf2fde38b.  This method will panic if any
// invalid/nil inputs are passed.
//
// Solidity: function transferOwnership(address _to) returns()
func (proxy *Proxy) PackTransferOwnership(to common.Address) []byte {
	enc, err := proxy.abi.Pack("transferOwnership", to)
	if err != nil {
		panic(err)
	}
	return enc
}

// TryPackTransferOwnership is the Go binding used to pack the parameters required for calling
// the contract method with ID 0xf2fde38b.  This method will return an error
// if any inputs are invalid/nil.
//
// Solidity: function transferOwnership(address _to) returns()
func (proxy *Proxy) TryPackTransferOwnership(to common.Address) ([]byte, error) {
	return proxy.abi.Pack("transferOwnership", to)
}

// PackVersion is the Go binding used to pack the parameters required for calling
// the contract method with ID 0x54fd4d50.  This method will panic if any
// invalid/nil inputs are passed.
//
// Solidity: function version() view returns(uint256)
func (proxy *Proxy) PackVersion() []byte {
	enc, err := proxy.abi.Pack("version")
	if err != nil {
		panic(err)
	}
	return enc
}

// TryPackVersion is the Go binding used to pack the parameters required for calling
// the contract method with ID 0x54fd4d50.  This method will return an error
// if any inputs are invalid/nil.
//
// Solidity: function version() view returns(uint256)
func (proxy *Proxy) TryPackVersion() ([]byte, error) {
	return proxy.abi.Pack("version")
}

// UnpackVersion is the Go binding that unpacks the parameters returned
// from invoking the contract method with ID 0x54fd4d50.
//
// Solidity: function version() view returns(uint256)
func (proxy *Proxy) UnpackVersion(data []byte) (*big.Int, error) {
	out, err := proxy.abi.Unpack("version", data)
	if err != nil {
		return new(big.Int), err
	}
	out0 := abi.ConvertType(out[0], new(big.Int)).(*big.Int)
	return out0, nil
}

// ProxyAnswerUpdated represents a AnswerUpdated event raised by the Proxy contract.
type ProxyAnswerUpdated struct {
	Current   *big.Int
	RoundId   *big.Int
	UpdatedAt *big.Int
	Raw       *types.Log // Blockchain specific contextual infos
}

const ProxyAnswerUpdatedEventName = "AnswerUpdated"

// ContractEventName returns the user-defined event name.
func (ProxyAnswerUpdated) ContractEventName() string {
	return ProxyAnswerUpdatedEventName
}

// UnpackAnswerUpdatedEvent is the Go binding that unpacks the event data emitted
// by contract.
//
// Solidity: event AnswerUpdated(int256 indexed current, uint256 indexed roundId, uint256 updatedAt)
func (proxy *Proxy) UnpackAnswerUpdatedEvent(log *types.Log) (*ProxyAnswerUpdated, error) {
	event := "AnswerUpdated"
	if len(log.Topics) == 0 {
		return nil, bind.ErrNoEventSignature
	}
	if log.Topics[0] != proxy.abi.Events[event].ID {
		return nil, bind.ErrEventSignatureMismatch
	}
	out := new(ProxyAnswerUpdated)
	if len(log.Data) > 0 {
		if err := proxy.abi.UnpackIntoInterface(out, event, log.Data); err != nil {
			return nil, err
		}
	}
	var indexed abi.Arguments
	for _, arg := range proxy.abi.Events[event].Inputs {
		if arg.Indexed {
			indexed = append(indexed, arg)
		}
	}
	if err := abi.ParseTopics(out, indexed, log.Topics[1:]); err != nil {
		return nil, err
	}
	out.Raw = log
	return out, nil
}

// ProxyNewRound represents a NewRound event raised by the Proxy contract.
type ProxyNewRound struct {
	RoundId   *big.Int
	StartedBy common.Address
	StartedAt *big.Int
	Raw       *types.Log // Blockchain specific contextual infos
}

const ProxyNewRoundEventName = "NewRound"

// ContractEventName returns the user-defined event name.
func (ProxyNewRound) ContractEventName() string {
	return ProxyNewRoundEventName
}

// UnpackNewRoundEvent is the Go binding that unpacks the event data emitted
// by contract.
//
// Solidity: event NewRound(uint256 indexed roundId, address indexed startedBy, uint256 startedAt)
func (proxy *Proxy) UnpackNewRoundEvent(log *types.Log) (*ProxyNewRound, error) {
	event := "NewRound"
	if len(log.Topics) == 0 {
		return nil, bind.ErrNoEventSignature
	}
	if log.Topics[0] != proxy.abi.Events[event].ID {
		return nil, bind.ErrEventSignatureMismatch
	}
	out := new(ProxyNewRound)
	if len(log.Data) > 0 {
		if err := proxy.abi.UnpackIntoInterface(out, event, log.Data); err != nil {
			return nil, err
		}
	}
	var indexed abi.Arguments
	for _, arg := range proxy.abi.Events[event].Inputs {
		if arg.Indexed {
			indexed = append(indexed, arg)
		}
	}
	if err := abi.ParseTopics(out, indexed, log.Topics[1:]); err != nil {
		return nil, err
	}
	out.Raw = log
	return out, nil
}

// ProxyOwnershipTransferRequested represents a OwnershipTransferRequested event raised by the Proxy contract.
type ProxyOwnershipTransferRequested struct {
	From common.Address
	To   common.Address
	Raw  *types.Log // Blockchain specific contextual infos
}

const ProxyOwnershipTransferRequestedEventName = "OwnershipTransferRequested"

// ContractEventName returns the user-defined event name.
func (ProxyOwnershipTransferRequested) ContractEventName() string {
	return ProxyOwnershipTransferRequestedEventName
}

// UnpackOwnershipTransferRequestedEvent is the Go binding that unpacks the event data emitted
// by contract.
//
// Solidity: event OwnershipTransferRequested(address indexed from, address indexed to)
func (proxy *Proxy) UnpackOwnershipTransferRequestedEvent(log *types.Log) (*ProxyOwnershipTransferRequested, error) {
	event := "OwnershipTransferRequested"
	if len(log.Topics) == 0 {
		return nil, bind.ErrNoEventSignature
	}
	if log.Topics[0] != proxy.abi.Events[event].ID {
		return nil, bind.ErrEventSignatureMismatch
	}
	out := new(ProxyOwnershipTransferRequested)
	if len(log.Data) > 0 {
		if err := proxy.abi.UnpackIntoInterface(out, event, log.Data); err != nil {
			return nil, err
		}
	}
	var indexed abi.Arguments
	for _, arg := range proxy.abi.Events[event].Inputs {
		if arg.Indexed {
			indexed = append(indexed, arg)
		}
	}
	if err := abi.ParseTopics(out, indexed, log.Topics[1:]); err != nil {
		return nil, err
	}
	out.Raw = log
	return out, nil
}

// ProxyOwnershipTransferred represents a OwnershipTransferred event raised by the Proxy contract.
type ProxyOwnershipTransferred struct {
	From common.Address
	To   common.Address
	Raw  *types.Log // Blockchain specific contextual infos
}

const ProxyOwnershipTransferredEventName = "OwnershipTransferred"

// ContractEventName returns the user-defined event name.
func (ProxyOwnershipTransferred) ContractEventName() string {
	return ProxyOwnershipTransferredEventName
}

// UnpackOwnershipTransferredEvent is the Go binding that unpacks the event data emitted
// by contract.
//
// Solidity: event OwnershipTransferred(address indexed from, address indexed to)
func (proxy *Proxy) UnpackOwnershipTransferredEvent(log *types.Log) (*ProxyOwnershipTransferred, error) {
	event := "OwnershipTransferred"
	if len(log.Topics) == 0 {
		return nil, bind.ErrNoEventSignature
	}
	if log.Topics[0] != proxy.abi.Events[event].ID {
		return nil, bind.ErrEventSignatureMismatch
	}
	out := new(ProxyOwnershipTransferred)
	if len(log.Data) > 0 {
		if err := proxy.abi.UnpackIntoInterface(out, event, log.Data); err != nil {
			return nil, err
		}
	}
	var indexed abi.Arguments
	for _, arg := range proxy.abi.Events[event].Inputs {
		if arg.Indexed {
			indexed = append(indexed, arg)
		}
	}
	if err := abi.ParseTopics(out, indexed, log.Topics[1:]); err != nil {
		return nil, err
	}
	out.Raw = log
	return out, nil
}
