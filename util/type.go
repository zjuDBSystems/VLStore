package util

type Key int64
type Value []byte
type H256 [32]byte

type KeyValue struct {
	Key   Key
	Value Value
}
