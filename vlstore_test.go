package vlstore

import (
	"VLStore/util"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"
)

func TestVLStore(t *testing.T) {
	// 创建临时目录用于测试
	testDir := "test_vlstore_dir"
	err := os.MkdirAll(testDir, 0755)
	if err != nil {
		t.Fatalf("创建测试目录失败: %v", err)
	}
	defer os.RemoveAll(testDir) // 测试结束后清理

	// 创建配置
	fanout := 4
	epsilon := 23
	baseStateNum := 100
	sizeRatio := 2
	configs := util.NewConfigs(fanout, epsilon, testDir, baseStateNum, sizeRatio)

	// 创建VLStore实例
	store := NewVLStore(configs)

	// 测试插入和查询
	testInsertAndSearch(t, store)
}

func testInsertAndSearch(t *testing.T, store *VLStore) {
	// 测试数据
	testData := []struct {
		key   util.Key
		value util.Value
	}{
		{1, []byte("value1")},
		{2, []byte("value2")},
		{3, []byte("value3")},
		{100, []byte("value100")},
		{200, []byte("value200")},
	}

	// 插入测试数据
	for _, data := range testData {
		store.Insert(data.key, data.value)
	}

	// 验证查询结果
	for _, data := range testData {
		value := store.Search(data.key)
		if value == nil {
			t.Errorf("键 %d 未找到", data.key)
			continue
		}
		if string(value) != string(data.value) {
			t.Errorf("键 %d 的值不匹配, 期望: %s, 实际: %s", data.key, string(data.value), string(value))
		}
	}

	// 测试查询不存在的键
	nonExistentKey := util.Key(999)
	value := store.Search(nonExistentKey)
	if value != nil {
		t.Errorf("不存在的键 %d 返回了值: %s", nonExistentKey, string(value))
	}
}

// 测试大量数据的插入和查询
func TestVLStoreWithLargeData(t *testing.T) {
	// 创建临时目录用于测试
	testDir := "test_vlstore_large_dir"
	err := os.MkdirAll(testDir, 0755)
	if err != nil {
		t.Fatalf("创建测试目录失败: %v", err)
	}
	defer os.RemoveAll(testDir) // 测试结束后清理

	// 创建配置 - 使用较小的baseStateNum以触发磁盘级别存储
	fanout := 4
	epsilon := 23
	baseStateNum := 100 // 较小的值，使内存树快速填满
	sizeRatio := 2
	configs := util.NewConfigs(fanout, epsilon, testDir, baseStateNum, sizeRatio)

	// 创建VLStore实例
	store := NewVLStore(configs)

	// 插入足够多的数据以触发磁盘级别存储
	numEntries := 10000 // 超过baseStateNum，确保有数据写入磁盘
	for i := 0; i < numEntries; i++ {
		key := util.Key(i)
		value := []byte(fmt.Sprintf("value%d", i))
		store.Insert(key, value)
	}

	// 验证所有数据都可以被查询到
	for i := 0; i < numEntries; i++ {
		key := util.Key(i)
		expectedValue := []byte(fmt.Sprintf("value%d", i))
		value := store.Search(key)

		if value == nil {
			t.Errorf("键 %d 未找到", key)
			continue
		}

		if string(value) != string(expectedValue) {
			t.Errorf("键 %d 的值不匹配, 期望: %s, 实际: %s", key, string(expectedValue), string(value))
		}
	}
}

// 测试仅内存级别的操作，避免触发未实现的check_and_merge功能
func TestVLStoreInMemoryOnly(t *testing.T) {
	// 创建临时目录用于测试
	testDir := "test_vlstore_memory_dir"
	err := os.MkdirAll(testDir, 0755)
	if err != nil {
		t.Fatalf("创建测试目录失败: %v", err)
	}
	defer os.RemoveAll(testDir) // 测试结束后清理

	// 创建配置 - 使用较大的baseStateNum以确保所有操作都在内存中
	fanout := 4
	epsilon := 10
	baseStateNum := 1000 // 足够大，确保测试数据不会触发磁盘存储
	sizeRatio := 2
	configs := util.NewConfigs(fanout, epsilon, testDir, baseStateNum, sizeRatio)

	// 创建VLStore实例
	store := NewVLStore(configs)

	// 测试数据 - 确保数量小于baseStateNum
	numEntries := 50 // 远小于baseStateNum

	// 插入测试数据
	for i := 0; i < numEntries; i++ {
		key := util.Key(i)
		value := []byte(fmt.Sprintf("mem_value%d", i))
		store.Insert(key, value)
	}

	// 验证所有数据都可以被查询到
	for i := 0; i < numEntries; i++ {
		key := util.Key(i)
		expectedValue := []byte(fmt.Sprintf("mem_value%d", i))
		value := store.Search(key)

		if value == nil {
			t.Errorf("键 %d 未找到", key)
			continue
		}

		if string(value) != string(expectedValue) {
			t.Errorf("键 %d 的值不匹配, 期望: %s, 实际: %s", key, string(expectedValue), string(value))
		}
	}

	// 验证内存树中的键值对数量
	if store.MemTable.KeyNum() != numEntries {
		t.Errorf("内存树中的键值对数量不匹配, 期望: %d, 实际: %d", numEntries, store.MemTable.KeyNum())
	}
}

// 测试VLStore的初始化和配置参数
func TestVLStoreInitialization(t *testing.T) {
	// 创建临时目录用于测试
	testDir := "test_vlstore_init_dir"
	err := os.MkdirAll(testDir, 0755)
	if err != nil {
		t.Fatalf("创建测试目录失败: %v", err)
	}
	defer os.RemoveAll(testDir) // 测试结束后清理

	// 测试不同的配置参数
	testCases := []struct {
		name         string
		fanout       int
		epsilon      int
		baseStateNum int
		sizeRatio    int
	}{
		{"默认配置", 4, 10, 100, 2},
		{"高扇出", 8, 10, 100, 2},
		{"低扇出", 2, 10, 100, 2},
		{"高误差界限", 4, 20, 100, 2},
		{"低误差界限", 4, 5, 100, 2},
		{"大基础状态数", 4, 10, 200, 2},
		{"小基础状态数", 4, 10, 50, 2},
		{"高大小比率", 4, 10, 100, 4},
		{"低大小比率", 4, 10, 100, 1},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 创建配置
			configs := util.NewConfigs(tc.fanout, tc.epsilon, testDir, tc.baseStateNum, tc.sizeRatio)

			// 创建VLStore实例
			store := NewVLStore(configs)

			// 验证配置参数是否正确设置
			if store.Configs.Fanout != tc.fanout {
				t.Errorf("Fanout不匹配, 期望: %d, 实际: %d", tc.fanout, store.Configs.Fanout)
			}
			if store.Configs.Epsilon != tc.epsilon {
				t.Errorf("Epsilon不匹配, 期望: %d, 实际: %d", tc.epsilon, store.Configs.Epsilon)
			}
			if store.Configs.BaseStateNum != tc.baseStateNum {
				t.Errorf("BaseStateNum不匹配, 期望: %d, 实际: %d", tc.baseStateNum, store.Configs.BaseStateNum)
			}
			if store.Configs.SizeRatio != tc.sizeRatio {
				t.Errorf("SizeRatio不匹配, 期望: %d, 实际: %d", tc.sizeRatio, store.Configs.SizeRatio)
			}
			if store.Configs.DirName != testDir {
				t.Errorf("DirName不匹配, 期望: %s, 实际: %s", testDir, store.Configs.DirName)
			}

			// 验证内存树是否正确初始化
			if store.MemTable == nil {
				t.Error("MemTable未初始化")
			}

			// 验证LevelVec是否正确初始化
			if store.LevelVec == nil {
				t.Error("LevelVec未初始化")
			}

			// 验证runIDCnt是否正确初始化
			if store.runIDCnt != 0 {
				t.Errorf("runIDCnt不匹配, 期望: 0, 实际: %d", store.runIDCnt)
			}

			// 测试基本的插入和查询功能
			key := util.Key(1)
			value := []byte("test_value")
			store.Insert(key, value)

			retrievedValue := store.Search(key)
			if retrievedValue == nil || string(retrievedValue) != string(value) {
				t.Errorf("基本查询测试失败, 期望: %s, 实际: %v", string(value), retrievedValue)
			}
		})
	}
}

// 测试VLStore的写入吞吐量 - 严格顺序写入，单线程，value大小为1KB
func TestVLStoreWriteThroughput(t *testing.T) {
	// 创建临时目录用于测试
	testDir := "test_vlstore_throughput_dir"
	err := os.MkdirAll(testDir, 0755)
	if err != nil {
		t.Fatalf("创建测试目录失败: %v", err)
	}
	defer os.RemoveAll(testDir) // 测试结束后清理

	// 测试不同配置下的写入吞吐量
	testCases := []struct {
		name         string
		fanout       int
		epsilon      int
		baseStateNum int
		sizeRatio    int
		numEntries   int // 要插入的键值对数量
	}{
		//{"小数据量", 4, 23, 1000, 2, 1000},
		//{"小数据量", 4, 23, 1000, 2, 5000},
		//{"小数据量", 4, 23, 1000, 2, 8000},
		//{"中数据量", 4, 23, 1000, 2, 10000},
		//{"中数据量", 4, 23, 1000, 2, 10000},
		//{"大数据量", 4, 23, 1000, 2, 50000},
		{"大数据量", 4, 23, 64000, 2, 1000000},
		//{"高扇出", 8, 23, 1000, 2, 50000},
		//{"低扇出", 2, 23, 1000, 2, 50000},
	}

	// 固定值大小为1KB
	valueSize := 1024

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 创建配置
			configs := util.NewConfigs(tc.fanout, tc.epsilon, testDir, tc.baseStateNum, tc.sizeRatio)

			// 创建VLStore实例
			store := NewVLStore(configs)

			// 生成顺序键值对
			keyValues := generateSequentialKeyValues(tc.numEntries, valueSize)

			// 测量写入时间
			startTime := time.Now()

			// 单线程顺序写入
			for _, kv := range keyValues {
				store.Insert(kv.Key, kv.Value)
			}

			elapsedTime := time.Since(startTime)

			// 计算吞吐量（每秒操作数）
			throughput := float64(tc.numEntries) / elapsedTime.Seconds()

			// 计算总数据量（MB）
			totalDataMB := float64(tc.numEntries*(8+valueSize)) / (1024 * 1024) // 8字节的键 + 值的大小

			t.Logf("配置: %s", tc.name)
			t.Logf("插入 %d 个键值对，每个值大小 %d 字节", tc.numEntries, valueSize)
			t.Logf("总数据量: %.2f MB", totalDataMB)
			t.Logf("耗时: %.2f 秒", elapsedTime.Seconds())
			t.Logf("吞吐量: %.2f 操作/秒", throughput)
			t.Logf("数据吞吐量: %.2f MB/秒", totalDataMB/elapsedTime.Seconds())

			// 验证插入是否成功（随机抽样检查）
			sampleSize := 100
			if tc.numEntries < sampleSize {
				sampleSize = tc.numEntries
			}

			for i := 0; i < sampleSize; i++ {
				idx := rand.Intn(tc.numEntries)
				key := keyValues[idx].Key
				expectedValue := keyValues[idx].Value
				value := store.Search(key)

				if value == nil {
					t.Errorf("键 %d 未找到", key)
					continue
				}

				if string(value) != string(expectedValue) {
					t.Errorf("键 %d 的值不匹配", key)
				}
			}
		})
	}
}

// 生成顺序键值对
func generateSequentialKeyValues(count int, valueSize int) []util.KeyValue {
	result := make([]util.KeyValue, count)

	// 生成固定内容的值，避免随机生成的开销
	value := make([]byte, valueSize)
	for i := 0; i < valueSize; i++ {
		value[i] = byte(i % 256)
	}

	// 生成顺序键值对
	for i := 0; i < count; i++ {
		// 复制值以避免共享同一个底层数组
		valueCopy := make([]byte, valueSize)
		copy(valueCopy, value)

		result[i] = util.KeyValue{
			Key:   util.Key(i), // 顺序键
			Value: valueCopy,
		}
	}

	return result
}

// 测试VLStore的批量写入性能 - 严格顺序写入，单线程，value大小为1KB
func TestVLStoreBatchInsert(t *testing.T) {
	// 创建临时目录用于测试
	testDir := "test_vlstore_batch_dir"
	err := os.MkdirAll(testDir, 0755)
	if err != nil {
		t.Fatalf("创建测试目录失败: %v", err)
	}
	defer os.RemoveAll(testDir) // 测试结束后清理

	// 测试不同批量大小的写入性能
	batchSizes := []int{100, 500, 1000, 5000}
	totalEntries := 10000
	valueSize := 1024 // 固定值大小为1KB

	for _, batchSize := range batchSizes {
		t.Run(fmt.Sprintf("批量大小-%d", batchSize), func(t *testing.T) {
			// 创建配置 - 使用较大的baseStateNum以确保所有操作都在内存中
			fanout := 4
			epsilon := 23
			baseStateNum := totalEntries + 1000 // 确保不会触发磁盘存储
			sizeRatio := 2
			configs := util.NewConfigs(fanout, epsilon, testDir, baseStateNum, sizeRatio)

			// 创建VLStore实例
			store := NewVLStore(configs)

			// 生成顺序键值对
			keyValues := generateSequentialKeyValues(totalEntries, valueSize)

			// 分批插入并测量时间
			startTime := time.Now()

			for i := 0; i < totalEntries; i += batchSize {
				end := i + batchSize
				if end > totalEntries {
					end = totalEntries
				}

				// 批量插入
				for j := i; j < end; j++ {
					store.Insert(keyValues[j].Key, keyValues[j].Value)
				}
			}

			elapsedTime := time.Since(startTime)

			// 计算吞吐量
			throughput := float64(totalEntries) / elapsedTime.Seconds()

			// 计算总数据量（MB）
			totalDataMB := float64(totalEntries*(8+valueSize)) / (1024 * 1024) // 8字节的键 + 值的大小

			t.Logf("批量大小: %d", batchSize)
			t.Logf("总插入数量: %d", totalEntries)
			t.Logf("每个值大小: %d 字节", valueSize)
			t.Logf("总数据量: %.2f MB", totalDataMB)
			t.Logf("耗时: %.2f 秒", elapsedTime.Seconds())
			t.Logf("吞吐量: %.2f 操作/秒", throughput)
			t.Logf("数据吞吐量: %.2f MB/秒", totalDataMB/elapsedTime.Seconds())

			// 验证插入是否成功（随机抽样检查）
			sampleSize := 100
			for i := 0; i < sampleSize; i++ {
				idx := rand.Intn(totalEntries)
				key := keyValues[idx].Key
				expectedValue := keyValues[idx].Value
				value := store.Search(key)

				if value == nil {
					t.Errorf("键 %d 未找到", key)
					continue
				}

				if string(value) != string(expectedValue) {
					t.Errorf("键 %d 的值不匹配", key)
				}
			}
		})
	}
}

// 测试VLStore在不同数据量下的写入性能 - 严格顺序写入，单线程，value大小为1KB
func TestVLStoreWriteScalability(t *testing.T) {
	// 创建临时目录用于测试
	testDir := "test_vlstore_scalability_dir"
	err := os.MkdirAll(testDir, 0755)
	if err != nil {
		t.Fatalf("创建测试目录失败: %v", err)
	}
	defer os.RemoveAll(testDir) // 测试结束后清理

	// 测试不同数据量的写入性能
	dataSizes := []int{1000, 5000, 10000, 50000, 100000}
	valueSize := 1024 // 固定值大小为1KB

	// 固定配置
	fanout := 4
	epsilon := 23
	sizeRatio := 2

	for _, dataSize := range dataSizes {
		t.Run(fmt.Sprintf("数据量-%d", dataSize), func(t *testing.T) {
			// 创建配置 - baseStateNum设置为足够大，避免触发磁盘存储
			baseStateNum := dataSize + 1000
			configs := util.NewConfigs(fanout, epsilon, testDir, baseStateNum, sizeRatio)

			// 创建VLStore实例
			store := NewVLStore(configs)

			// 生成顺序键值对
			keyValues := generateSequentialKeyValues(dataSize, valueSize)

			// 测量写入时间
			startTime := time.Now()

			// 单线程顺序写入
			for _, kv := range keyValues {
				store.Insert(kv.Key, kv.Value)
			}

			elapsedTime := time.Since(startTime)

			// 计算吞吐量
			throughput := float64(dataSize) / elapsedTime.Seconds()

			// 计算总数据量（MB）
			totalDataMB := float64(dataSize*(8+valueSize)) / (1024 * 1024) // 8字节的键 + 值的大小

			t.Logf("数据量: %d 条记录", dataSize)
			t.Logf("每个值大小: %d 字节", valueSize)
			t.Logf("总数据量: %.2f MB", totalDataMB)
			t.Logf("耗时: %.2f 秒", elapsedTime.Seconds())
			t.Logf("吞吐量: %.2f 操作/秒", throughput)
			t.Logf("数据吞吐量: %.2f MB/秒", totalDataMB/elapsedTime.Seconds())

			// 验证插入是否成功（随机抽样检查）
			sampleSize := 100
			if dataSize < sampleSize {
				sampleSize = dataSize
			}

			for i := 0; i < sampleSize; i++ {
				idx := rand.Intn(dataSize)
				key := keyValues[idx].Key
				expectedValue := keyValues[idx].Value
				value := store.Search(key)

				if value == nil {
					t.Errorf("键 %d 未找到", key)
					continue
				}

				if string(value) != string(expectedValue) {
					t.Errorf("键 %d 的值不匹配", key)
				}
			}
		})
	}
}
