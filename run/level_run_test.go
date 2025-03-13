package run

import (
	"VLStore/util"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"testing"
)

// 生成随机KeyValue数据的辅助函数
func generateRandomKeyValues(n int, seed int64) []util.KeyValue {
	r := rand.New(rand.NewSource(seed))
	keyValues := make([]util.KeyValue, 0, n)

	for i := 0; i < n; i++ {
		key := util.Key(i)
		// 创建随机值，长度为32字节
		value := make([]byte, 32)
		r.Read(value)

		keyValues = append(keyValues, util.KeyValue{
			Key:   key,
			Value: value,
		})
	}

	// 排序KeyValue
	sort.Slice(keyValues, func(i, j int) bool {
		return keyValues[i].Key < keyValues[j].Key
	})

	return keyValues
}


// 测试LevelRun的哈希功能
func TestLevelRunHash(t *testing.T) {
	dirName := "test_storage"

	// 如果测试目录存在，则删除
	if _, err := os.Stat(dirName); err == nil {
		os.RemoveAll(dirName)
	}

	// 创建测试目录
	err := os.MkdirAll(dirName, 0755)
	if err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}
	defer os.RemoveAll(dirName) // 测试结束后清理

	// 创建随机KeyValue数据
	r := rand.New(rand.NewSource(1))
	keyValues := make([]util.KeyValue, 0)

	// 添加最小键和最大键
	minKey := util.Key(0)
	maxKey := util.Key(^uint(0) >> 1) // 最大int值

	minValue := make([]byte, 32)
	maxValue := make([]byte, 32)

	keyValues = append(keyValues, util.KeyValue{Key: minKey, Value: minValue})

	// 添加一些中间键值
	key1 := util.Key(34)
	key2 := util.Key(36)

	value1 := make([]byte, 32)
	value2 := make([]byte, 32)

	binary.LittleEndian.PutUint64(value1, 34)
	binary.LittleEndian.PutUint64(value2, 36)

	keyValues = append(keyValues, util.KeyValue{Key: key1, Value: value1})
	keyValues = append(keyValues, util.KeyValue{Key: key2, Value: value2})
	keyValues = append(keyValues, util.KeyValue{Key: maxKey, Value: maxValue})

	// 排序KeyValue
	sort.Slice(keyValues, func(i, j int) bool {
		return keyValues[i].Key < keyValues[j].Key
	})

	// 创建内存迭代器
	it := &InMemKeyValueIterator{
		KeyValues:   keyValues,
		CurValuePos: 0,
	}

	// 设置参数
	epsilon := 23
	fanout := 5
	n := 2
	runID := 1
	levelID := 0
	k := 2

	// 创建配置
	configs := util.NewConfigs(
		fanout,
		epsilon,
		dirName,
		n,
		k,
	)

	// 构建运行
	run, err := ConstructRunByInMemoryTree(it, runID, levelID, dirName, epsilon, fanout, configs.MaxNumOfStatesInARun(levelID), 1, k)
	if err != nil {
		t.Fatalf("Failed to construct run: %v", err)
	}

	fmt.Printf("Run digest: %x\n", run.Digest)

	// 持久化过滤器
	err = run.PersistFilter(levelID, configs)
	if err != nil {
		t.Fatalf("Failed to persist filter: %v", err)
	}

	// 释放运行
	run = nil

	// 加载运行
	loadedRun, err := Load(runID, levelID, configs)
	if err != nil {
		t.Fatalf("Failed to load run: %v", err)
	}

	fmt.Printf("Loaded run digest: %x\n", loadedRun.Digest)

	// 搜索键
	result := loadedRun.SearchRun(key1, configs)
	if result == nil {
		t.Errorf("Failed to find key %d", key1)
	} else {
		fmt.Printf("Found key %d, value: %x\n", key1, result.Value)
	}

	// 搜索随机键（应该不存在）
	randomKey := util.Key(r.Int63())
	result = loadedRun.SearchRun(randomKey, configs)
	if result != nil {
		t.Errorf("Unexpectedly found random key %d", randomKey)
	}
}

// 测试内存合并和运行构建
func TestInMemoryMergeAndRunConstruction(t *testing.T) {
	k := 2
	n := 10
	seed := int64(1)
	epsilon := 46
	fanout := 2
	dirName := "test_storage"

	// 如果测试目录存在，则删除
	if _, err := os.Stat(dirName); err == nil {
		os.RemoveAll(dirName)
	}

	// 创建测试目录
	err := os.MkdirAll(dirName, 0755)
	if err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}
	defer os.RemoveAll(dirName) // 测试结束后清理

	// 创建多个内存迭代器
	iterators := make([]*InMemKeyValueIterator, 0, k)

	for i := 0; i < k; i++ {
		keyValues := generateRandomKeyValues(n, seed+int64(i))

		// 添加最小键和最大键
		minKey := util.Key(0)
		maxKey := util.Key(^uint(0) >> 1) // 最大int值

		minValue := make([]byte, 32)
		maxValue := make([]byte, 32)

		// 确保最小键和最大键在列表的开始和结束
		keyValues = append([]util.KeyValue{{Key: minKey, Value: minValue}}, keyValues...)
		keyValues = append(keyValues, util.KeyValue{Key: maxKey, Value: maxValue})

		// 重新排序
		sort.Slice(keyValues, func(i, j int) bool {
			return keyValues[i].Key < keyValues[j].Key
		})

		it := &InMemKeyValueIterator{
			KeyValues:   keyValues,
			CurValuePos: 0,
		}
		iterators = append(iterators, it)
	}

	// 创建配置
	configs := util.NewConfigs(
		fanout,
		epsilon,
		dirName,
		n,
		k,
	)

	// 构建运行
	runID := 1
	levelID := 0

	run, err := ConstructRunByInMemoryTree(iterators[0], runID, levelID, dirName, epsilon, fanout, configs.MaxNumOfStatesInARun(levelID), 1, k)
	if err != nil {
		t.Fatalf("Failed to construct run: %v", err)
	}

	// 持久化过滤器
	err = run.PersistFilter(levelID, configs)
	if err != nil {
		t.Fatalf("Failed to persist filter: %v", err)
	}

	// 释放运行
	run = nil

	// 加载运行
	loadedRun, err := Load(runID, levelID, configs)
	if err != nil {
		t.Fatalf("Failed to load run: %v", err)
	}

	fmt.Printf("Loaded run: %+v\n", loadedRun)

	// 验证摘要计算
	mhtRoot := loadedRun.MHTReader.Root

	var bytes []byte
	bytes = append(bytes, mhtRoot[:]...)

	if loadedRun.FilterHash != nil {
		bytes = append(bytes, (*loadedRun.FilterHash)[:]...)
	}

	computedDigest := util.NewHasher(util.BLAKE3).HashBytes(bytes)

	if computedDigest != loadedRun.Digest {
		t.Errorf("Digest mismatch: expected %x, got %x", loadedRun.Digest, computedDigest)
	}
}
