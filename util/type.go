package util

type Key int
type Value []byte
type H256 [32]byte

const (
	H256_SIZE = 32
)

type KeyValue struct {
	Key   Key
	Value Value
}
