package util

import (
	"crypto/sha256"
	"encoding/binary"

	"github.com/zeebo/blake3"
)

// 哈希算法类型
type Algorithm int

const (
	SHA256 Algorithm = iota
	BLAKE3
)

// 哈希接口
type Hasher interface {
	HashValue(value Value) H256
	HashKey(key Key) H256
	HashBytes(bytes []byte) H256
}

// SHA256实现
type SHA256Hasher struct{}

func (h *SHA256Hasher) HashValue(value Value) H256 {
	return sha256.Sum256(value);
}

func (h *SHA256Hasher) HashKey(key Key) H256 {
    keyBytes := make([]byte, 8)
    binary.LittleEndian.PutUint64(keyBytes, uint64(key))
    return sha256.Sum256(keyBytes)
}

func (h *SHA256Hasher) HashBytes(bytes []byte) H256 {
	return sha256.Sum256(bytes)
}

// BLAKE3实现
type Blake3Hasher struct {
	hasher *blake3.Hasher
}

func NewBlake3Hasher() *Blake3Hasher {
	h := blake3.New() // 使用默认配置
	return &Blake3Hasher{hasher: h}
}

func (h *Blake3Hasher) HashValue(value Value) H256 {
	h.hasher.Reset()
    h.hasher.Write(value)
    var result H256
    copy(result[:], h.hasher.Sum(nil))
    return result
}

func (h *Blake3Hasher) HashKey(key Key) H256 {
	h.hasher.Reset()
    keyBytes := make([]byte, 8)
    binary.LittleEndian.PutUint64(keyBytes, uint64(key))
    h.hasher.Write(keyBytes)
    var result H256
    copy(result[:], h.hasher.Sum(nil))
    return result
}

func (h *Blake3Hasher) HashBytes(bytes []byte) H256 {
	h.hasher.Reset()
	h.hasher.Write(bytes)
	var result H256
	copy(result[:], h.hasher.Sum(nil))
	return result
}

// 哈希工厂方法
func NewHasher(algo Algorithm) Hasher {
	switch algo {
	case SHA256:
		return &SHA256Hasher{}
	case BLAKE3:
		return NewBlake3Hasher()
	default:
		panic("unsupported hash algorithm")
	}
}