package util

import "encoding/binary"

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

func (kv *KeyValue) ComputeHash(hasher Hasher) H256 {
	bytes := make([]byte, 0)
	keyBytes := make([]byte, 8)
    binary.LittleEndian.PutUint64(keyBytes, uint64(kv.Key))
	bytes = append(bytes, keyBytes...)
	bytes = append(bytes, kv.Value...)
	return hasher.HashBytes(bytes)
}
