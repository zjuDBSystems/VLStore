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
		//{"空结果查询", util.Key(1000), util.Key(2000), 0},
		//{"单个结果查询", util.Key(100), util.Key(100), 1},
		{"小范围查询", util.Key(50), util.Key(60), 11},
		{"中范围查询", util.Key(100), util.Key(150), 51},
		{"大范围查询", util.Key(450), util.Key(480), 31},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 获取范围查询的证明
			proof := store.SearchWithProof(tc.startKey, tc.endKey)

			// 计算根哈希
			rootHash := store.ComputeDigest()

			// 验证证明并获取结果
			valid, results := store.VerifyAndCollectResult(tc.startKey, tc.endKey, proof, rootHash, fanout)

			// 打印results 中的key
			for _, kv := range results {
				fmt.Print(kv.Key, " ")
			}
			fmt.Println()

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

// TestVLStoreVerifyQueryTampering tests the tamper-resistance of the verified range query
func TestVLStoreVerifyQueryTampering(t *testing.T) {
	// 创建临时目录用于测试
	testDir := "test_vlstore_verify_tamper"
	err := os.MkdirAll(testDir, 0755)
	if err != nil {
		t.Fatalf("创建测试目录失败: %v", err)
	}
	defer os.RemoveAll(testDir) // 测试结束后清理

	// 创建配置
	fanout := 4
	epsilon := 23
	baseStateNum := 100
	sizeRatio := 100
	configs := util.NewConfigs(fanout, epsilon, testDir, baseStateNum, sizeRatio)

	// 创建VLStore实例
	store := NewVLStore(configs)

	// 插入测试数据
	numEntries := 300
	for i := 0; i < numEntries; i++ {
		key := util.Key(i)
		value := []byte(fmt.Sprintf("value%d", i))
		store.Insert(key, value)
	}

	// 定义查询范围
	startKey := util.Key(50)
	endKey := util.Key(150)

	// 获取正确的证明
	proof := store.SearchWithProof(startKey, endKey)
	rootHash := store.ComputeDigest()

	// 验证正确的证明应该通过
	valid, _ := store.VerifyAndCollectResult(startKey, endKey, proof, rootHash, fanout)
	if !valid {
		t.Errorf("有效的范围查询证明验证失败")
	}

	// 篡改结果 - 如果有结果
	if len(proof.memTableProofVec) > 0 && len(proof.memTableProofVec[0].results) > 0 {
		// 保存原始值
		original := proof.memTableProofVec[0].results[0].Value

		// 篡改值
		tamperedValue := []byte("tampered")
		proof.memTableProofVec[0].results[0].Value = tamperedValue

		// 验证篡改后的证明应该失败
		valid, _ = store.VerifyAndCollectResult(startKey, endKey, proof, rootHash, fanout)
		if valid {
			t.Errorf("篡改结果后的证明验证竟然通过了")
		}

		// 恢复原始值
		proof.memTableProofVec[0].results[0].Value = original
	}

	// 篡改哈希值 - 如果有哈希值
	if len(proof.levelProofVec) > 0 && len(proof.levelProofVec[0].runProofVec) > 0 {
		// 保存原始哈希
		var originalHash util.H256
		var runProof *LevelRunProofOrHash

		// 找到一个包含哈希的runProof
		for _, levelProof := range proof.levelProofVec {
			for _, rp := range levelProof.runProofVec {
				if rp.isHash {
					runProof = rp
					originalHash = rp.hash
					break
				}
			}
			if runProof != nil {
				break
			}
		}

		if runProof != nil {
			// 篡改哈希
			tamperedHash := util.H256{}
			copy(tamperedHash[:], originalHash[:])
			tamperedHash[0] = ^tamperedHash[0] // 翻转第一个字节
			runProof.hash = tamperedHash

			// 验证篡改后的证明应该失败
			valid, _ = store.VerifyAndCollectResult(startKey, endKey, proof, rootHash, fanout)
			if valid {
				t.Errorf("篡改哈希后的证明验证竟然通过了")
			}

			// 恢复原始哈希
			runProof.hash = originalHash
		}
	}

	// 篡改根哈希
	tamperedRootHash := util.H256{}
	copy(tamperedRootHash[:], rootHash[:])
	tamperedRootHash[0] = ^tamperedRootHash[0] // 翻转第一个字节

	// 验证使用篡改后的根哈希应该失败
	valid, _ = store.VerifyAndCollectResult(startKey, endKey, proof, tamperedRootHash, fanout)
	if valid {
		t.Errorf("使用篡改后的根哈希验证竟然通过了")
	}
}
