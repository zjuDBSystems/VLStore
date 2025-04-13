package vlstore

import (
	"VLStore/util"
	"fmt"
	"math/rand"
	"os"
	"syscall"
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
		{"大数据量", 4, 4, 64000, 2, 1000000},
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

// TestVLStoreVerifyQuery tests the verified range query functionality of VLStore
func TestVLStoreVerifyQuery(t *testing.T) {
	// 创建临时目录用于测试
	testDir := "test_vlstore_verify_dir"
	err := os.MkdirAll(testDir, 0755)
	if err != nil {
		t.Fatalf("创建测试目录失败: %v", err)
	}
	defer os.RemoveAll(testDir) // 测试结束后清理

	// 创建配置
	fanout := 4
	epsilon := 23
	baseStateNum := 100 // 较小的值，使内存树快速填满，触发磁盘存储
	sizeRatio := 100
	configs := util.NewConfigs(fanout, epsilon, testDir, baseStateNum, sizeRatio)

	// 创建VLStore实例
	store := NewVLStore(configs)

	// 插入足够多的数据以触发磁盘级别存储
	numEntries := 550
	for i := 0; i < numEntries; i++ {
		key := util.Key(i)
		value := []byte(fmt.Sprintf("value%d", i))
		store.Insert(key, value)
	}

	time.Sleep(5 * time.Second)

	// 测试不同范围的查询
	testCases := []struct {
		name     string
		startKey util.Key
		endKey   util.Key
		expected int // 预期结果数量
	}{
		{"空查询", util.Key(1000), util.Key(2000), 0},
		{"单点查询", util.Key(100), util.Key(100), 1},
		{"范围查询1", util.Key(50), util.Key(60), 11},
		{"范围查询2", util.Key(100), util.Key(150), 51},
		{"范围查询3", util.Key(450), util.Key(480), 31},
		{"范围查询4", util.Key(12), util.Key(342), 331},
		{"范围查询5", util.Key(0), util.Key(550), 550},
		{"范围查询6", util.Key(99), util.Key(199), 101},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 获取范围查询的证明
			proof := store.SearchWithProof(tc.startKey, tc.endKey)

			// 计算根哈希
			rootHash := store.ComputeDigest()

			// 验证证明并获取结果
			valid, results := store.VerifyAndCollectResult(tc.startKey, tc.endKey, proof, rootHash, fanout)

			// // 打印results 中的key
			// for _, kv := range results {
			// 	fmt.Print(kv.Key, " ")
			// }
			// fmt.Println()

			// 验证结果是否有效
			if !valid {
				t.Errorf("范围查询证明验证失败: 范围 [%d, %d]", tc.startKey, tc.endKey)
			}

			// 验证结果数量
			if len(results) != tc.expected {
				t.Errorf("范围查询结果数量不匹配: 期望 %d, 实际 %d", tc.expected, len(results))
			}

			// 验证结果是否按键排序
			for i := 1; i < len(results); i++ {
				if results[i-1].Key >= results[i].Key {
					t.Errorf("范围查询结果未按键排序: %d >= %d", results[i-1].Key, results[i].Key)
				}
			}

			// 验证结果是否在查询范围内
			for _, kv := range results {
				if kv.Key < tc.startKey || kv.Key > tc.endKey {
					t.Errorf("范围查询结果超出范围: 键 %d 不在 [%d, %d] 范围内", kv.Key, tc.startKey, tc.endKey)
				}
			}

			// 验证结果的值是否正确
			for _, kv := range results {
				expectedValue := []byte(fmt.Sprintf("value%d", kv.Key))
				if string(kv.Value) != string(expectedValue) {
					t.Errorf("键 %d 的值不匹配, 期望: %s, 实际: %s", kv.Key, string(expectedValue), string(kv.Value))
				}
			}
		})
	}
}

// 测试VLStore的范围查询性能
func TestVLStoreRangeQueryPerformance(t *testing.T) {
	// 创建临时目录用于测试
	testDir := "test_vlstore_query_perf_dir"
	err := os.MkdirAll(testDir, 0755)
	if err != nil {
		t.Fatalf("创建测试目录失败: %v", err)
	}
	defer os.RemoveAll(testDir)

	// 创建配置
	fanout := 4
	epsilon := 4
	baseStateNum := 64000
	sizeRatio := 2
	configs := util.NewConfigs(fanout, epsilon, testDir, baseStateNum, sizeRatio)

	// 创建VLStore实例
	store := NewVLStore(configs)

	// 数据量
	numEntries := 100000
	valueSize := 1024
	// 生成顺序键值对
	keyValues := generateSequentialKeyValues(numEntries, valueSize)

	// 单线程顺序写入
	for _, kv := range keyValues {
		store.Insert(kv.Key, kv.Value)
	}

	time.Sleep(5 * time.Second)
	// 测试不同范围大小的查询
	rangeSizes := []int{1, 10, 100, 500, 800, 1000, 3000, 5000, 8000, 10000}
	numQueries := 100 // 每个范围大小执行的查询次数

	for _, rangeSize := range rangeSizes {
		t.Run(fmt.Sprintf("RangeSize_%d", rangeSize), func(t *testing.T) {
			var totalLatency time.Duration
			//var totalProofSize float64
			startCPU := getCPUSample()

			// 执行多次查询并测量
			for i := 0; i < numQueries; i++ {
				// 随机选择一个范围起点，确保不超出数据范围
				startKey := util.Key(rand.Intn(numEntries - rangeSize))
				endKey := startKey + util.Key(rangeSize)
				

				//fmt.Println("startKey", startKey, "endKey", endKey)
				// 测量单次查询延迟和单次proof的size
				queryStart := time.Now()
				store.SearchWithProof(startKey, endKey)
				store.ComputeDigest()
				//proof := store.SearchWithProof(startKey, endKey)
				//rootHash := store.ComputeDigest()
				//_, _ = store.VerifyAndCollectResult(startKey, endKey, proof, rootHash, fanout)
				queryLatency := time.Since(queryStart)
				// 计算proof的size
				//proofSize := unsafe.Sizeof(*proof)

				totalLatency += queryLatency
				//totalProofSize += float64(proofSize)
			}

			endCPU := getCPUSample()
			cpuUsage := calculateCPUUsage(startCPU, endCPU)

			avgLatency := totalLatency / time.Duration(numQueries)
			//avgProofSize := totalProofSize / float64(numQueries)
			t.Logf("范围大小: %d, 平均查询延迟: %v, 平均CPU使用率: %.2f%%",
				rangeSize, avgLatency, cpuUsage)
		})
	}
}

// 获取CPU使用样本
func getCPUSample() time.Time {
	// 简单实现，使用时间作为CPU采样点
	return time.Now()
}

// 计算CPU使用率
func calculateCPUUsage(start, end time.Time) float64 {
	// 在生产环境中，应该使用runtime/pprof或其他工具获取实际CPU使用率
	// 这里仅为示例，返回一个模拟值
	elapsed := end.Sub(start).Seconds()

	// 使用runtime包获取CPU使用情况
	var rusage syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &rusage)

	// 用户态CPU时间 + 系统态CPU时间（秒）
	userTime := time.Duration(rusage.Utime.Sec)*time.Second +
		time.Duration(rusage.Utime.Usec)*time.Microsecond
	sysTime := time.Duration(rusage.Stime.Sec)*time.Second +
		time.Duration(rusage.Stime.Usec)*time.Microsecond

	totalCPUTime := userTime + sysTime
	cpuUsage := float64(totalCPUTime) / float64(time.Duration(elapsed)*time.Second) * 100

	return cpuUsage
}
