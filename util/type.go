package util

type Key int64
type Value []byte
type H256 [32]byte

const (
	H256_SIZE = 32
)

type KeyValue struct {
	Key   Key
	Value Value
}
