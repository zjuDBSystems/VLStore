package run

import (
	"VLStore/util"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"testing"
)

// 生成随机字节数组
func randomBytes(length int, seed int64) []byte {
	r := rand.New(rand.NewSource(seed))
	bytes := make([]byte, length)
	for i := range bytes {
		bytes[i] = byte(r.Intn(256))
	}
	return bytes
}

// 生成随机Key
func randomKey(seed int64) util.Key {
	// 使用seed来生成一个确定性的随机数
	r := rand.New(rand.NewSource(seed))
	return util.Key(r.Int63())
}

// 生成运行
func generateRun(levelID int, runID int, n int, configs *util.Configs, seed int64) (*LevelRun, error) {
	// 创建键值对切片
	keyValues := make([]util.KeyValue, 0, n+2) // +2 是为了添加最小和最大键

	// 生成n个随机键值对
	for i := 0; i < n; i++ {
		key := randomKey(seed + int64(i))
		value := randomBytes(32, seed+int64(i)) // 生成32字节的随机值
		keyValues = append(keyValues, util.KeyValue{Key: key, Value: value})
	}

	// 添加最小键和最大键
	minKey := util.Key(0)
	maxKey := util.Key(^uint(0) >> 1) // 最大int值

	minValue := make([]byte, 32)
	maxValue := make([]byte, 32)

	keyValues = append(keyValues, util.KeyValue{Key: minKey, Value: minValue})
	keyValues = append(keyValues, util.KeyValue{Key: maxKey, Value: maxValue})

	// 排序键值对
	sort.Slice(keyValues, func(i, j int) bool {
		return keyValues[i].Key < keyValues[j].Key
	})

	// 创建内存迭代器
	it := NewInMemKeyValueIterator(keyValues)

	// 构建运行
	run, err := ConstructRunByInMemoryTree(keyValues, it, runID, levelID, configs.DirName, configs.Epsilon, configs.Fanout, configs.MaxNumOfStatesInARun(levelID), 1, configs.SizeRatio)
	if err != nil {
		return nil, err
	}

	return run, nil
}

// 测试Level的创建、持久化和加载
func TestLevel(t *testing.T) {
	levelID := 0
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
		t.Fatalf("创建测试目录失败: %v", err)
	}
	defer os.RemoveAll(dirName) // 测试结束后清理

	n := 100
	configs := util.NewConfigs(
		fanout,
		fanout,
		epsilon,
		dirName,
		n,
		10, // sizeRatio
	)

	// 创建新的Level
	level := NewLevel(levelID)

	// 添加两个运行
	for i := 0; i < 2; i++ {
		run, err := generateRun(levelID, i, n, configs, int64(i+1))
		if err != nil {
			t.Fatalf("生成运行失败: %v", err)
		}
		level.RunVec = append(level.RunVec, run)
	}

	// 持久化Level
	level.Persist(configs)

	// 释放Level
	level = nil

	// 加载Level
	loadedLevel, err := LoadLevel(levelID, configs)
	if err != nil {
		t.Fatalf("加载Level失败: %v", err)
	}

	// 打印加载的Level信息
	fmt.Printf("加载的Level: ID=%d, 运行数量=%d\n", loadedLevel.LevelID, len(loadedLevel.RunVec))

	// 验证加载的Level
	if loadedLevel.LevelID != levelID {
		t.Errorf("加载的Level ID不匹配: 期望 %d, 实际 %d", levelID, loadedLevel.LevelID)
	}

	if len(loadedLevel.RunVec) != 2 {
		t.Errorf("加载的运行数量不匹配: 期望 2, 实际 %d", len(loadedLevel.RunVec))
	}
}
