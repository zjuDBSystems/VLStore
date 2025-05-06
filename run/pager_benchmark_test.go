package run

import (
	"VLStore/util"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"
)

const (
	TEST_DATA_COUNT = 1000000 // 测试数据量
	VALUE_SIZE      = 1024    // 1KB大小的value
	TEST_FILE_DIR   = "test_files"
)

// 准备测试环境
func setupTestEnv() {
	// 创建测试文件目录
	if _, err := os.Stat(TEST_FILE_DIR); os.IsNotExist(err) {
		err = os.Mkdir(TEST_FILE_DIR, 0755)
		if err != nil {
			panic(err)
		}
	}
}

// 清理测试环境
func cleanupTestEnv() {
	// 删除测试文件目录
	// err := os.RemoveAll(TEST_FILE_DIR)
	// if err != nil {
	// 	panic(err)
	// }
}

// 生成测试数据
func generateTestData(count int, valueSize int) []util.KeyValue {
	data := make([]util.KeyValue, count)
	for i := 0; i < count; i++ {
		key := util.Key(i)
		value := make([]byte, valueSize)
		// 填充随机数据
		rand.Read(value)
		data[i] = util.KeyValue{Key: key, Value: value}
	}
	return data
}

// 测试函数，用于运行所有吞吐量测试并输出结果
func TestPagerWriteThroughput(t *testing.T) {
	setupTestEnv()
	defer cleanupTestEnv()

	// 生成测试数据
	testData := generateTestData(TEST_DATA_COUNT, VALUE_SIZE)

	// 测试ValuePager

	// 测试MHTpager
	t.Run("MHTpager", func(t *testing.T) {
		hasher := util.NewHasher(util.SHA256)
		fileName := fmt.Sprintf("%s/mht_pager_test.dat", TEST_FILE_DIR)
		constructor, err := NewStreamMHTConstructor(fileName, 4, hasher)
		if err != nil {
			t.Fatal(err)
		}
		

		startTime := time.Now()

		// 顺序写入数据的哈希值
		for _, kv := range testData {
			hash := kv.ComputeHash(hasher)
			constructor.Append(hash)
		}

		// 构建MHT
		constructor.BuildMHT()

		elapsedTime := time.Since(startTime)

		// 计算吞吐量 (ops/s)
		throughput := float64(TEST_DATA_COUNT) / elapsedTime.Seconds()

		t.Logf("MHTpager写入吞吐量: %.2f ops/s", throughput)

		// 关闭文件
		constructor.OutputMHTWriter.File.Close()
	})

	// 测试ModelPager
	t.Run("ModelPager", func(t *testing.T) {
		// 提取键
		keys := make([]util.Key, TEST_DATA_COUNT)
		for i, kv := range testData {
			keys[i] = kv.Key
		}

		// 设置误差上限
		epsilon := 23

		fileName := fmt.Sprintf("%s/model_pager_test.dat", TEST_FILE_DIR)

		// 创建StreamModelConstructor
		constructor, err := NewStreamModelConstructor(fileName, epsilon)
		if err != nil {
			t.Fatal(err)
		}

		startTime := time.Now()

		// 顺序添加键
		for _, key := range keys {
			err := constructor.AppendKey(key)
			if err != nil {
				t.Fatal(err)
			}
		}

		// 完成构建
		err = constructor.FinalizeAppend()
		if err != nil {
			t.Fatal(err)
		}

		elapsedTime := time.Since(startTime)

		// 计算吞吐量 (ops/s)
		throughput := float64(TEST_DATA_COUNT) / elapsedTime.Seconds()

		t.Logf("ModelPager写入吞吐量: %.2f ops/s", throughput)

		// 关闭文件
		constructor.OutputModelWriter.File.Close()
	})
}
