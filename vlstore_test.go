package vlstore

import (
	"VLStore/util"
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strconv"
	"strings"
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
	configs := util.NewConfigs(fanout, fanout, epsilon, testDir, baseStateNum)

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
	epsilon := 86
	baseStateNum := 100 // 较小的值，使内存树快速填满
	configs := util.NewConfigs(fanout, fanout, epsilon, testDir, baseStateNum)

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
		numEntries   int // 要插入的键值对数量
	}{
		//{"小数据量", 4, 23, 1000, 1000},
		//{"小数据量", 4, 23, 1000, 5000},
		//{"小数据量", 4, 23, 1000, 8000},
		//{"中数据量", 4, 23, 1000, 10000},
		//{"大数据量", 4, 23, 1000, 50000},
		{"大数据量", 4, 23, 64000, 1000000},
		//{"高扇出", 8, 23, 1000, 50000},
		//{"低扇出", 2, 23, 1000,  50000},
	}

	// 固定值大小为1KB
	valueSize := 1024

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 创建配置
			configs := util.NewConfigs(tc.fanout, tc.fanout, tc.epsilon, testDir, tc.baseStateNum)

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
	configs := util.NewConfigs(fanout, fanout, epsilon, testDir, baseStateNum)

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
	configs := util.NewConfigs(fanout, fanout, epsilon, testDir, baseStateNum)

	// 创建VLStore实例
	store := NewVLStore(configs)

	// 数据量
	numEntries := 1000000
	valueSize := 1024
	// 生成顺序键值对
	keyValues := generateSequentialKeyValues(numEntries, valueSize)

	// 单线程顺序写入
	for _, kv := range keyValues {
		store.Insert(kv.Key, kv.Value)
	}

	time.Sleep(5 * time.Second)
	// 测试不同范围大小的查询
	rangeSizes := []int{0, 10, 100, 1000, 10000}
	//rangeSizes := []int{1000}
	numQueries := 10 // 每个范围大小执行的查询次数

	// CPU profile
	f, err := os.Create("cpu.prof")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	if err := pprof.StartCPUProfile(f); err != nil {
		t.Fatal(err)
	}
	defer pprof.StopCPUProfile()

	for _, rangeSize := range rangeSizes {
		t.Run(fmt.Sprintf("RangeSize_%d", rangeSize), func(t *testing.T) {
			var totalLatency time.Duration
			//var totalProofSize float64

			// 执行多次查询并测量
			startKey := util.Key(rand.Intn(numEntries - rangeSize))
			endKey := startKey + util.Key(rangeSize)
			for i := 0; i < numQueries; i++ {
				// 随机选择一个范围起点，确保不超出数据范围
				//startKey := util.Key(rand.Intn(numEntries - rangeSize))
				//endKey := startKey + util.Key(rangeSize)

				//fmt.Println("startKey", startKey, "endKey", endKey)
				// 测量单次查询延迟和单次proof的size
				queryStart := time.Now()
				//fmt.Println("search ", i)
				store.SearchWithProof(startKey, endKey)
				fmt.Println()
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

			avgLatency := totalLatency / time.Duration(numQueries)
			//avgProofSize := totalProofSize / float64(numQueries)
			t.Logf("范围大小: %d, 平均查询延迟: %v",
				rangeSize, avgLatency)
		})
	}
}

// 测试VLStore的写入QPS和延迟
func TestVLStoreWriteQPSAndLatencyT(t *testing.T) {
	// 创建临时目录用于测试
	testDir := "test_vlstore_write_qps_dir"
	//testDir := "/pcissd/test_vlstore_write_qps_dir"  // 使用PCIe SSD
	err := os.MkdirAll(testDir, 0755)
	if err != nil {
		t.Fatalf("创建测试目录失败: %v", err)
	}
	defer os.RemoveAll(testDir)

	// 测试不同配置下的写入QPS和延迟
	testCases := []struct {
		name         string
		treeFanout   int
		fanout       int
		epsilon      int
		baseStateNum int
	}{
		{"大数据量", 8, 2, 86, 64000},
	}

	// 固定值大小为1KB
	valueSize := 1024 * 1

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 创建配置
			repeat := 20
			var sumQPS float64
			var sumAvgLatency time.Duration

			M := 1000000

			baseNum := M * 100
			statNum := 10000

			configs := util.NewConfigs(tc.treeFanout, tc.fanout, tc.epsilon, testDir, tc.baseStateNum)
			// 创建VLStore实例
			store := NewVLStore(configs)

			// 生成顺序键值对（包含基准写入和统计区间写入）
			keyValues := generateSequentialKeyValues(baseNum+repeat*statNum+10, valueSize)

			// 先插入基准数据
			for i := 0; i < baseNum; i++ {
				store.Insert(keyValues[i].Key, keyValues[i].Value)
			}

			for run := 0; run < repeat; run++ {
				// 记录每次写入的延迟
				var totalLatency time.Duration
				var maxLatency time.Duration
				var minLatency time.Duration = time.Hour // 初始化为一个很大的值

				// 测量总体写入时间和单次写入延迟
				overallStart := time.Now()

				for i := baseNum + run*statNum; i < baseNum+(run+1)*statNum; i++ {
					writeStart := time.Now()
					store.Insert(keyValues[i].Key, keyValues[i].Value)
					writeLatency := time.Since(writeStart)

					totalLatency += writeLatency
					if writeLatency > maxLatency {
						maxLatency = writeLatency
					}
					if writeLatency < minLatency {
						minLatency = writeLatency
					}
				}

				overallTime := time.Since(overallStart)

				// 计算指标
				avgLatency := totalLatency / time.Duration(statNum)
				qps := float64(statNum) / overallTime.Seconds()
				sumQPS += qps
				sumAvgLatency += avgLatency
			}

			avgQPS := sumQPS / float64(repeat)
			avgLatencyAcrossRuns := time.Duration(int64(sumAvgLatency) / int64(repeat))

			t.Logf("[Avg over %d runs] 扇出: %d, 基础键值对数量: %d, 每次写入键值对数量: %d", repeat, tc.treeFanout, baseNum, statNum)
			t.Logf("[Avg over %d runs] 平均写入QPS: %.2f 操作/秒", repeat, avgQPS)
			t.Logf("[Avg over %d runs] 平均延迟: %v", repeat, avgLatencyAcrossRuns)
		})
	}
}

// 测试VLStore的范围查询QPS
func TestVLStoreRangeQueryQPST(t *testing.T) {
	// 创建临时目录用于测试
	testDir := "test_vlstore_range_qps_dir"
	err := os.MkdirAll(testDir, 0755)
	if err != nil {
		t.Fatalf("创建测试目录失败: %v", err)
	}
	defer os.RemoveAll(testDir)

	// 创建配置
	treeFanout := 8
	fanout := 2
	epsilon := 86
	baseStateNum := 64000
	configs := util.NewConfigs(treeFanout, fanout, epsilon, testDir, baseStateNum)

	// 创建VLStore实例
	store := NewVLStore(configs)

	// 数据准备（顺序生成）
	numEntries := 1000000
	valueSize := 1024
	// 生成顺序键值对
	keyValues := generateSequentialKeyValues(numEntries, valueSize)

	t.Logf("开始插入 %d 个键值对", numEntries)
	//单线程顺序写入
	for _, kv := range keyValues {
		store.Insert(kv.Key, kv.Value)
		// if (i+1)%10000 == 0 {
		// 	t.Logf("已插入 %d/%d 个键值对", i+1, numEntries)
		// }
	}

	// 等待异步操作完成
	time.Sleep(5 * time.Second)
	t.Logf("数据插入完成，开始测试范围查询QPS")

	// 测试不同范围大小的查询QPS
	testCases := []struct {
		name       string
		rangeSize  int
		numQueries int
	}{
		{"1", 10000, 20},
		//{"2", 20000, 20},
		//{"3", 50000, 3},
		//{"4", 100000, 3},
		// {"5", numEntries / 100, 20},
		// {"6", numEntries / 50, 20},
		// {"7", numEntries / 20, 20},
		// {"8", numEntries / 10, 20},
		// {"9", numEntries / 5, 20},
		// {"10", numEntries / 2, 20},
		//{"9", 1, 10},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var totalLatency time.Duration
			var totalVerifyLantency time.Duration
			var maxLatency time.Duration
			var minLatency time.Duration = time.Hour
			var totalResultCount int

			for i := 0; i < tc.numQueries; i++ {
				// 随机选择一个范围起点，确保不超出数据范围
				startKey := util.Key(rand.Intn(numEntries - tc.rangeSize))
				endKey := startKey + util.Key(tc.rangeSize-1)

				// 测量单次查询延迟
				queryStart := time.Now()
				proof := store.SearchWithProof(startKey, endKey)
				queryLatency := time.Since(queryStart)
				rootHash := store.ComputeDigest()
				verifyStart := time.Now()
				valid, results := store.VerifyAndCollectResult(startKey, endKey, proof, rootHash, fanout)
				verifyLatency := time.Since(verifyStart)

				t.Logf("第 %d 次查询时间: %v", i+1, queryLatency)
				t.Logf("第 %d 次验证时间: %v", i+1, verifyLatency)

				// 验证查询结果
				//println(results[0].Key, results[len(results)-1].Key)
				if !valid {
					t.Errorf("第 %d 次查询验证失败，范围 [%d, %d]", i+1, startKey, endKey)
				}

				totalLatency += queryLatency
				totalVerifyLantency += verifyLatency
				totalResultCount += len(results)
				if queryLatency > maxLatency {
					maxLatency = queryLatency
				}
				if queryLatency < minLatency {
					minLatency = queryLatency
				}
			}

			// 计算指标
			avgLatency := totalLatency / time.Duration(tc.numQueries)
			avgVerifyLantency := totalVerifyLantency / time.Duration(tc.numQueries)
			qps := float64(tc.numQueries) / totalLatency.Seconds()
			avgResultCount := float64(totalResultCount) / float64(tc.numQueries)

			t.Logf("范围大小: %d", tc.rangeSize)
			t.Logf("查询次数: %d", tc.numQueries)
			t.Logf("总耗时: %.2f 秒", totalLatency.Seconds())
			t.Logf("范围查询QPS: %.2f 查询/秒", qps*float64(tc.rangeSize))
			//t.Logf("范围查询QPS: %.2f 查询/秒", qps)
			t.Logf("平均延迟: %v", avgLatency)
			t.Logf("最小延迟: %v", minLatency)
			t.Logf("最大延迟: %v", maxLatency)
			t.Logf("平均返回结果数: %.1f", avgResultCount)
			t.Logf("平均验证延迟: %v", avgVerifyLantency)
		})
	}
}

// 使用 HealthApp 数据构造 1M 写入，测试范围查询QPS
func TestVLStoreRangeQueryQPSFromHealthApp(t *testing.T) {
	// 创建临时目录用于测试
	testDir := "test_vlstore_range_qps_healthapp_dir"
	err := os.MkdirAll(testDir, 0755)
	if err != nil {
		t.Fatalf("创建测试目录失败: %v", err)
	}
	defer os.RemoveAll(testDir)

	// 读取并解析 HealthApp 日志
	logPath := "HealthApp_2k.log"
	keyValues, err := parseHealthAppLog(logPath)
	// for i := 0; i < len(keyValues); i++{
	// 	println(keyValues[i].Key)
	// }
	if err != nil {
		t.Fatalf("读取日志失败: %v", err)
	}
	if len(keyValues) == 0 {
		t.Fatalf("日志无有效数据: %s", logPath)
	}

	// 复制600次并每次时间戳+10ms，构造更大的数据集
	keyValues = replicateKeyValuesWithDelta(keyValues, 500)
	numEntries := len(keyValues)

	// 配置（参数可根据需要调整）
	treeFanout := 8
	fanout := 128
	epsilon := 86
	baseStateNum := 64000
	configs := util.NewConfigs(treeFanout, fanout, epsilon, testDir, baseStateNum)

	store := NewVLStore(configs)

	for i := 0; i < numEntries; i++ {
		store.Insert(keyValues[i].Key, keyValues[i].Value)
	}

	// 测试不同范围大小的查询QPS
	testCases := []struct {
		name       string
		rangeSize  int
		numQueries int
	}{
		//{"1", 0, 1000},
		{"2", 10000, 10},
		{"3", 20000, 10},
		{"4", 50000, 10},
		{"5", 100000, 10},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var totalLatency time.Duration
			var totalVerifyLantency time.Duration
			var maxLatency time.Duration
			var minLatency time.Duration = time.Hour
			var totalResultCount int

			for i := 0; i < tc.numQueries; i++ {
				startIndex := rand.Intn(numEntries - tc.rangeSize - 1)
				startKey := keyValues[startIndex].Key
				endKey := keyValues[startIndex+tc.rangeSize].Key

				queryStart := time.Now()
				proof := store.SearchWithProof(startKey, endKey)
				queryLatency := time.Since(queryStart)
				rootHash := store.ComputeDigest()
				verifyStart := time.Now()
				valid, results := store.VerifyAndCollectResult(startKey, endKey, proof, rootHash, fanout)
				verifyLatency := time.Since(verifyStart)

				//println(results[0].Key, results[len(results)-1].Key)
				if !valid {
					t.Errorf("第 %d 次查询验证失败，范围 [%d, %d]", i+1, startKey, endKey)
				}

				totalLatency += queryLatency
				totalVerifyLantency += verifyLatency
				totalResultCount += len(results)
				if queryLatency > maxLatency {
					maxLatency = queryLatency
				}
				if queryLatency < minLatency {
					minLatency = queryLatency
				}
			}

			avgLatency := totalLatency / time.Duration(tc.numQueries)
			avgVerifyLantency := totalVerifyLantency / time.Duration(tc.numQueries)
			qps := float64(tc.numQueries) / totalLatency.Seconds()
			avgResultCount := float64(totalResultCount) / float64(tc.numQueries)

			t.Logf("范围大小: %d", tc.rangeSize)
			t.Logf("查询次数: %d", tc.numQueries)
			t.Logf("总耗时: %.2f 秒", totalLatency.Seconds())
			t.Logf("范围查询QPS: %.2f 查询/秒", qps*float64(tc.rangeSize))
			t.Logf("平均延迟: %v", avgLatency)
			t.Logf("最小延迟: %v", minLatency)
			t.Logf("最大延迟: %v", maxLatency)
			t.Logf("平均返回结果数: %.1f", avgResultCount)
			t.Logf("平均验证延迟: %v", avgVerifyLantency)
		})
	}
}

func TestVLStoreWriteQPSAndLatencyFromHealthApp(t *testing.T) {
	// 创建临时目录用于测试
	testDir := "test_vlstore_write_qps_healthapp_dir"
	err := os.MkdirAll(testDir, 0755)
	if err != nil {
		t.Fatalf("创建测试目录失败: %v", err)
	}
	defer os.RemoveAll(testDir)

	// 读取并解析 HealthApp 日志
	logPath := "HealthApp_2k.log"
	keyValues, err := parseHealthAppLog(logPath)
	if err != nil {
		t.Fatalf("读取日志失败: %v", err)
	}
	if len(keyValues) == 0 {
		t.Fatalf("日志无有效数据: %s", logPath)
	}

	// 复制600次构造更大的数据集
	keyValues = replicateKeyValuesWithDelta(keyValues, 600)
	// 将keyvalues中的内容按照 key value形式写入文件中
	file, err := os.Create("healthapp_2k_600.log")
	if err != nil {
		t.Fatalf("创建文件失败: %v", err)
	}
	defer file.Close()
	for _, kv := range keyValues {
		file.WriteString(fmt.Sprintf("%d %s\n", kv.Key, kv.Value))
	}

	// 配置（参数可根据需要调整）
	treeFanout := 8
	fanout := 2
	epsilon := 23
	baseStateNum := 64000
	configs := util.NewConfigs(treeFanout, fanout, epsilon, testDir, baseStateNum)

	store := NewVLStore(configs)

	// 基准与统计参数
	repeat := 10
	baseNum := 1000000
	statNum := 10000
	totalNeed := baseNum + repeat*statNum
	if len(keyValues) < totalNeed {
		t.Fatalf("数据不足: 需要 %d 条, 实际 %d 条", totalNeed, len(keyValues))
	}

	// 先插入基准数据（100万条）
	for i := 0; i < baseNum; i++ {
		store.Insert(keyValues[i].Key, keyValues[i].Value)
	}

	var sumQPS float64
	var sumAvgLatency time.Duration

	// 10 轮每轮 10000 条
	for run := 0; run < repeat; run++ {
		startIdx := baseNum + run*statNum
		endIdx := startIdx + statNum

		var totalLatency time.Duration
		var maxLatency time.Duration
		var minLatency time.Duration = time.Hour

		overallStart := time.Now()
		for i := startIdx; i < endIdx; i++ {
			writeStart := time.Now()
			store.Insert(keyValues[i].Key, keyValues[i].Value)
			writeLatency := time.Since(writeStart)
			totalLatency += writeLatency
			if writeLatency > maxLatency {
				maxLatency = writeLatency
			}
			if writeLatency < minLatency {
				minLatency = writeLatency
			}
		}
		overallTime := time.Since(overallStart)

		avgLatency := totalLatency / time.Duration(statNum)
		qps := float64(statNum) / overallTime.Seconds()

		sumQPS += qps
		sumAvgLatency += avgLatency
	}

	avgQPS := sumQPS / float64(repeat)
	avgLatencyAcrossRuns := time.Duration(int64(sumAvgLatency) / int64(repeat))

	t.Logf("[HealthApp 平均] 写入QPS: %.2f ops/s", avgQPS)
	t.Logf("[HealthApp 平均] 平均延迟: %v", avgLatencyAcrossRuns)
}

// 将 kvs 扩展为原来的 copies 倍, 但key代表的时间戳要递增
func replicateKeyValuesWithDelta(kvs []util.KeyValue, copies int) []util.KeyValue {
	if len(kvs) == 0 || copies <= 0 {
		return kvs
	}

	result := make([]util.KeyValue, 0, len(kvs)*copies)

	// 计算时间间隔 - 假设原始数据的时间跨度，用于计算每次复制的时间偏移
	var timeSpan util.Key
	if len(kvs) > 1 {
		// 使用最后一个和第一个时间戳的差值作为时间跨度
		timeSpan = kvs[len(kvs)-1].Key - kvs[0].Key + 1
	} else {
		// 如果只有一个元素，使用固定的时间间隔（比如10毫秒）
		timeSpan = 10
	}

	// 复制 copies 次
	for i := 0; i < copies; i++ {
		timeOffset := util.Key(i) * timeSpan

		for _, kv := range kvs {
			newKV := util.KeyValue{
				Key:   kv.Key + timeOffset,
				Value: kv.Value,
			}
			result = append(result, newKV)
		}
	}

	return result
}

// 解析 HealthApp_2k.log：
// - 使用第1列时间戳作为 Key（毫秒）
// - 整行作为 Value
func parseHealthAppLog(path string) ([]util.KeyValue, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	// 处理超长行（默认64K），此处放宽到1MB
	buf := make([]byte, 0, 1024*1024)
	scanner.Buffer(buf, 1024*1024)

	kvs := make([]util.KeyValue, 0)
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) == 0 {
			continue
		}
		// 取第一段时间戳
		parts := strings.SplitN(line, "|", 2)
		if len(parts) == 0 {
			continue
		}
		tsStr := parts[0]
		//println(tsStr)
		// Go 的 time.Parse 只支持用 .000 解析毫秒，这里把最后一个冒号替换为点
		if idx := strings.LastIndexByte(tsStr, ':'); idx != -1 {
			tsStr = tsStr[:idx] + "." + tsStr[idx+1:]
		}
		// 使用包含毫秒的小数点布局
		layoutWithMs := "20060102-15:04:05.000"
		tm, err := time.Parse(layoutWithMs, tsStr)
		if err != nil {
			// 跳过无法解析的行
			continue
		}
		key := util.Key(tm.UnixMilli())
		value := util.Value([]byte(line))
		kvs = append(kvs, util.KeyValue{Key: key, Value: value})
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return kvs, nil
}

func TestVLStoreWriteQPSAndLatencyFromZookeeper(t *testing.T) {
	// 创建临时目录用于测试
	testDir := "test_vlstore_write_qps_zookeeper_dir"
	err := os.MkdirAll(testDir, 0755)
	if err != nil {
		t.Fatalf("创建测试目录失败: %v", err)
	}
	defer os.RemoveAll(testDir)

	// 读取并解析 Zookeeper 日志
	logPath := "Zookeeper_2k_600.log"
	keyValues, err := parseZookeeperLog(logPath)
	if err != nil {
		t.Fatalf("读取日志失败: %v", err)
	}
	if len(keyValues) == 0 {
		t.Fatalf("日志无有效数据: %s", logPath)
	}

	// 配置（参数可根据需要调整）
	treeFanout := 8
	fanout := 2
	epsilon := 23
	baseStateNum := 64000
	configs := util.NewConfigs(treeFanout, fanout, epsilon, testDir, baseStateNum)

	store := NewVLStore(configs)

	// 基准与统计参数
	repeat := 10
	baseNum := 1000000
	statNum := 10000
	totalNeed := baseNum + repeat*statNum
	if len(keyValues) < totalNeed {
		t.Fatalf("数据不足: 需要 %d 条, 实际 %d 条", totalNeed, len(keyValues))
	}

	// 先插入基准数据（100万条）
	for i := 0; i < baseNum; i++ {
		store.Insert(keyValues[i].Key, keyValues[i].Value)
	}

	var sumQPS float64
	var sumAvgLatency time.Duration

	// 10 轮每轮 10000 条
	for run := 0; run < repeat; run++ {
		startIdx := baseNum + run*statNum
		endIdx := startIdx + statNum

		var totalLatency time.Duration
		var maxLatency time.Duration
		var minLatency time.Duration = time.Hour

		overallStart := time.Now()
		for i := startIdx; i < endIdx; i++ {
			writeStart := time.Now()
			store.Insert(keyValues[i].Key, keyValues[i].Value)
			writeLatency := time.Since(writeStart)
			totalLatency += writeLatency
			if writeLatency > maxLatency {
				maxLatency = writeLatency
			}
			if writeLatency < minLatency {
				minLatency = writeLatency
			}
		}
		overallTime := time.Since(overallStart)

		avgLatency := totalLatency / time.Duration(statNum)
		qps := float64(statNum) / overallTime.Seconds()

		sumQPS += qps
		sumAvgLatency += avgLatency
	}

	avgQPS := sumQPS / float64(repeat)
	avgLatencyAcrossRuns := time.Duration(int64(sumAvgLatency) / int64(repeat))

	t.Logf("[Zookeeper 平均] 写入QPS: %.2f ops/s", avgQPS)
	t.Logf("[Zookeeper 平均] 平均延迟: %v", avgLatencyAcrossRuns)
}

// 解析 Zookeeper_2k_600.log：
// - 文件格式为 "key value"，其中key是时间戳毫秒数，value是完整日志行
func parseZookeeperLog(path string) ([]util.KeyValue, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	// 处理超长行（默认64K），此处放宽到1MB
	buf := make([]byte, 0, 1024*1024)
	scanner.Buffer(buf, 1024*1024)

	kvs := make([]util.KeyValue, 0)
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) == 0 {
			continue
		}

		// 按空格分割，第一部分是key（时间戳），剩余部分是value
		parts := strings.SplitN(line, " ", 2)
		if len(parts) < 2 {
			continue // 跳过格式不正确的行
		}

		// 解析key（时间戳毫秒数）
		keyStr := parts[0]
		key, err := strconv.ParseInt(keyStr, 10, 64)
		if err != nil {
			continue // 跳过无法解析的行
		}

		// value是剩余的部分
		value := util.Value([]byte(parts[1]))
		kvs = append(kvs, util.KeyValue{Key: util.Key(key), Value: value})
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return kvs, nil
}

// 使用 Zookeeper 数据构造 1M 写入，测试范围查询QPS
func TestVLStoreRangeQueryQPSFromZookeeper(t *testing.T) {
	// 创建临时目录用于测试
	testDir := "test_vlstore_range_qps_zookeeper_dir"
	err := os.MkdirAll(testDir, 0755)
	if err != nil {
		t.Fatalf("创建测试目录失败: %v", err)
	}
	defer os.RemoveAll(testDir)

	// 读取并解析 Zookeeper 日志
	logPath := "zookeeper_2k_600.log"
	keyValues, err := parseZookeeperLog(logPath)
	if err != nil {
		t.Fatalf("读取日志失败: %v", err)
	}
	if len(keyValues) == 0 {
		t.Fatalf("日志无有效数据: %s", logPath)
	}

	numEntries := 1000000

	// 配置（参数可根据需要调整）
	treeFanout := 8
	fanout := 2
	epsilon := 86
	baseStateNum := 64000
	configs := util.NewConfigs(treeFanout, fanout, epsilon, testDir, baseStateNum)

	store := NewVLStore(configs)

	for i := 0; i < numEntries; i++ {
		store.Insert(util.Key(i), keyValues[i].Value)
	}

	// 测试不同范围大小的查询QPS
	testCases := []struct {
		name       string
		rangeSize  int
		numQueries int
	}{
		{"1", 1, 1000},
		//{"2", 10000, 100},
		// {"3", 20000, 20},
		// {"4", 50000, 20},
		// {"5", 100000, 20},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var totalLatency time.Duration
			var totalVerifyLantency time.Duration
			var maxLatency time.Duration
			var minLatency time.Duration = time.Hour
			var totalResultCount int

			for i := 0; i < tc.numQueries; i++ {
				startIndex := rand.Intn(numEntries - tc.rangeSize - 1)
				// startKey := keyValues[startIndex].Key
				// endKey := keyValues[startIndex+tc.rangeSize].Key
				startKey := util.Key(startIndex)
				endKey := util.Key(startIndex + tc.rangeSize - 1)

				queryStart := time.Now()
				proof := store.SearchWithProof(startKey, endKey)
				queryLatency := time.Since(queryStart)
				rootHash := store.ComputeDigest()
				verifyStart := time.Now()
				valid, results := store.VerifyAndCollectResult(startKey, endKey, proof, rootHash, fanout)
				verifyLatency := time.Since(verifyStart)

				// // 判断读取结构是否正确
				// t.Logf("第 %d 次读取结果数: %d", i+1, len(results))
				// // 打印目标key和value
				// for j := 0; j < tc.rangeSize; j++ {
				// 	t.Logf("目标key: %d, value: %s", keyValues[startIndex+j].Key, keyValues[startIndex+j].Value)
				// 	t.Logf("读取key: %d, value: %s", results[j].Key, results[j].Value)
				// }

				//打印startKey和endKey
				t.Logf("startKey: %d, endKey: %d", startKey, endKey)
				// 打印results中的第一个key和最后一个key
				//t.Logf("results[0].Key: %d, results[len(results)-1].Key: %d", results[0].Key, results[len(results)-1].Key)

				if !valid {
					t.Errorf("第 %d 次查询验证失败，范围 [%d, %d]", i+1, startKey, endKey)
				}

				// 打印第i次的queryLatency
				t.Logf("第 %d 次查询queryLatency: %v", i+1, queryLatency)

				totalLatency += queryLatency
				totalVerifyLantency += verifyLatency
				totalResultCount += len(results)
				if queryLatency > maxLatency {
					maxLatency = queryLatency
				}
				if queryLatency < minLatency {
					minLatency = queryLatency
				}
			}

			avgLatency := totalLatency / time.Duration(tc.numQueries)
			avgVerifyLantency := totalVerifyLantency / time.Duration(tc.numQueries)
			qps := float64(tc.numQueries) / totalLatency.Seconds()
			avgResultCount := float64(totalResultCount) / float64(tc.numQueries)

			t.Logf("范围大小: %d", tc.rangeSize)
			t.Logf("查询次数: %d", tc.numQueries)
			t.Logf("总耗时: %.2f 秒", totalLatency.Seconds())
			t.Logf("范围查询QPS: %.2f 查询/秒", qps*float64(tc.rangeSize))
			t.Logf("平均延迟: %v", avgLatency)
			t.Logf("最小延迟: %v", minLatency)
			t.Logf("最大延迟: %v", maxLatency)
			t.Logf("平均返回结果数: %.1f", avgResultCount)
			t.Logf("平均验证延迟: %v", avgVerifyLantency)
		})
	}
}

// 测试VLStore在不同数据量下的磁盘占用空间
func TestVLStoreDiskUsage(t *testing.T) {
	// 测试不同数据量下的磁盘占用
	testCases := []struct {
		name       string
		numEntries int // 数据条数
	}{
		{"0.1M", 100000},  // 0.1M
		{"0.2M", 200000},   // 1M
		{"0.5M", 500000},   // 5M
		{"1M", 1000000}, // 10M
	}

	// 固定值大小为1KB
	valueSize := 1024

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 为每个测试用例创建独立的目录，加上时间戳避免冲突
			testDir := fmt.Sprintf("test_vlstore_disk_usage_%s_%d", tc.name, time.Now().UnixNano())
			err := os.MkdirAll(testDir, 0755)
			if err != nil {
				t.Fatalf("创建测试目录失败: %v", err)
			}
			defer func() {
				// 等待更长时间确保异步操作完成
				time.Sleep(10 * time.Second)
				os.RemoveAll(testDir)
			}()

			// 创建配置
			treeFanout := 8
			fanout := 2
			epsilon := 86
			baseStateNum := 64000
			configs := util.NewConfigs(treeFanout, fanout, epsilon, testDir, baseStateNum)

			// 创建VLStore实例
			store := NewVLStore(configs)

			// 生成顺序键值对
			keyValues := generateSequentialKeyValues(tc.numEntries, valueSize)

			// 记录开始时的目录大小
			initialSize, err := getDirSize(testDir)
			if err != nil {
				t.Fatalf("获取初始目录大小失败: %v", err)
			}

			// 记录插入开始时间
			startTime := time.Now()

			// 单线程顺序写入
			for _, kv := range keyValues {
				store.Insert(kv.Key, kv.Value)
			}

			// 等待异步操作完成，大数据量需要更长时间
			waitTime := 10 * time.Second
			time.Sleep(waitTime)

			insertTime := time.Since(startTime)

			// 记录插入后的目录大小
			finalSize, err := getDirSize(testDir)
			if err != nil {
				t.Fatalf("获取最终目录大小失败: %v", err)
			}

			// 计算实际占用的磁盘空间
			diskUsage := finalSize - initialSize

			// 计算原始数据大小（键8字节 + 值的大小）
			rawDataSize := int64(tc.numEntries * (8 + valueSize))

			// 计算空间放大倍数
			amplificationFactor := float64(diskUsage) / float64(rawDataSize)

			// 输出结果
			t.Logf("数据量: %s (%d 条记录)", tc.name, tc.numEntries)
			t.Logf("插入耗时: %.2f 秒", insertTime.Seconds())
			t.Logf("原始数据大小: %.2f MB", float64(rawDataSize)/(1024*1024))
			t.Logf("磁盘占用空间: %.2f MB", float64(diskUsage)/(1024*1024))
			t.Logf("空间放大倍数: %.2fx", amplificationFactor)
			t.Logf("平均每条记录磁盘占用: %.2f 字节", float64(diskUsage)/float64(tc.numEntries))

			// 验证一些随机数据确保写入成功
			sampleSize := 100
			if tc.numEntries < sampleSize {
				sampleSize = tc.numEntries
			}

			successCount := 0
			for i := 0; i < sampleSize; i++ {
				idx := rand.Intn(tc.numEntries)
				key := keyValues[idx].Key
				expectedValue := keyValues[idx].Value
				value := store.Search(key)

				if value != nil && string(value) == string(expectedValue) {
					successCount++
				}
			}

			t.Logf("随机验证成功率: %d/%d (%.1f%%)", successCount, sampleSize, float64(successCount)*100/float64(sampleSize))
		})
	}
}

// getDirSize 计算目录及其所有子目录和文件的总大小
func getDirSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}
