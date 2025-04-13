package memtable

import (
	"VLStore/util"
	"bytes"
	"fmt"
	"math/rand"
	"runtime"
	"testing"
	"time"
)

func randStringBytes(n int) string {
	letterBytes := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func TestSequentialInsert(t *testing.T) {
	tree := NewBPlusTree(0, 4)

	var n int64 = 1000000

	// 批量插入100个顺序键
	for i := int64(0); i < n; i++ {
		tree.Insert(util.Key(i), util.Value(fmt.Sprintf("v%d", i)))
	}

	//fmt.Println(tree.PrintTree())

	// 验证所有键可查
	for i := int64(0); i < n; i++ {
		val, ok := tree.Search(util.Key(i))
		if !ok {
			t.Errorf("键%d未找到", i)
			continue
		}
		expected := fmt.Sprintf("v%d", i)
		if string(val) != expected {
			t.Errorf("键%d的值不匹配, 期望:%s, 实际:%s", i, expected, string(val))
		}
	}
}

func TestInsertThroughput(t *testing.T) {
	const (
		fanout     = 4      // 根据实际场景调整
		numInserts = 100000 // 测试数据量
		sampleSize = 1000   // 抽样验证数量
	)

	// 初始化新树实例
	tree := NewBPlusTree(0, fanout)

	// 预热运行(消除冷启动影响)
	for i := 0; i < 1000; i++ {
		tree.Insert(util.Key(i), util.Value("preheat"))
	}
	tree = NewBPlusTree(0, fanout) // 重置为干净状态

	// 生成测试数据
	keys := make([]util.Key, numInserts)
	values := make([]util.Value, numInserts)
	for i := range keys {
		keys[i] = util.Key(i)
		// 插入长度为1kb的随机字符串
		values[i] = util.Value(randStringBytes(1024))

		//values[i] = util.Value(fmt.Sprintf("val%d", i))
	}

	// 性能测试
	start := time.Now()

	for i := 0; i < numInserts; i++ {
		tree.Insert(keys[i], values[i])
	}

	elapsed := time.Since(start)

	// 计算吞吐量
	opsPerSec := float64(numInserts) / elapsed.Seconds()
	t.Logf("插入性能：")
	t.Logf("阶数       : %d", fanout)
	t.Logf("数据量     : %d records", numInserts)
	t.Logf("总耗时     : %v", elapsed.Round(time.Millisecond))
	t.Logf("吞吐量     : %.0f ops/sec", opsPerSec)
	t.Logf("每次操作   : %v/op", time.Duration(int64(elapsed)/int64(numInserts)))

	// 随机抽样验证
	for i := 0; i < sampleSize; i++ {
		r := rand.Intn(numInserts)
		foundValue, ok := tree.Search(keys[r])
		if !ok {
			t.Fatalf("键 %d 未找到", r)
		}

		// 使用字节比较（假设Value是[]byte类型）
		expected := []byte(values[r])
		actual := []byte(foundValue)
		if !bytes.Equal(actual, expected) {
			t.Fatalf("数据不一致: 键 %d\n期望值: %x\n实际值: %x",
				r, expected, actual)
		}
	}

	// 内存统计(仅供参考)
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	t.Logf("\n内存统计:")
	t.Logf("已分配内存 : %.2f MB", float64(m.Alloc)/1024/1024)
	t.Logf("总分配内存 : %.2f MB", float64(m.TotalAlloc)/1024/1024)
	t.Logf("内存分配次数: %d", m.Mallocs)
}

func TestRangeSearch(t *testing.T) {
	tree := NewBPlusTree(0, 4)

	// 插入测试数据
	numEntries := 1000
	for i := 0; i < numEntries; i++ {
		tree.Insert(util.Key(i), util.Value(fmt.Sprintf("v%d", i)))
	}

	// 查询45-55
	results := tree.RangeSearch(util.Key(45), util.Key(55))
	for _, kv := range results {
		fmt.Println(kv.Key, string(kv.Value))
	}

	// 测试用例
	testCases := []struct {
		name     string
		startKey util.Key
		endKey   util.Key
		expected int // 期望的结果数量
	}{
		{"完整范围", 0, util.Key(numEntries - 1), numEntries},
		{"部分范围", 100, 200, 101},
		{"单个元素", 500, 500, 1},
		{"空范围", 2000, 3000, 0},
		{"边界测试", util.Key(numEntries - 10), util.Key(numEntries + 10), 10},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			results := tree.RangeSearch(tc.startKey, tc.endKey)

			// 检查结果数量
			if len(results) != tc.expected {
				t.Errorf("期望返回 %d 个结果，实际返回 %d 个", tc.expected, len(results))
			}

			// 验证结果是否在范围内且有序
			for i, kv := range results {
				if kv.Key < tc.startKey || kv.Key > tc.endKey {
					t.Errorf("结果中包含范围外的键: %v", kv.Key)
				}

				// 检查值是否正确
				expectedValue := fmt.Sprintf("v%d", kv.Key)
				if string(kv.Value) != expectedValue {
					t.Errorf("键 %d 的值不匹配，期望: %s, 实际: %s", kv.Key, expectedValue, string(kv.Value))
				}

				// 检查顺序
				if i > 0 && kv.Key <= results[i-1].Key {
					t.Errorf("结果未按键排序: %v 在 %v 之后", results[i-1].Key, kv.Key)
				}
			}
		})
	}
}

func TestRangeSearchPerformance(t *testing.T) {
	const (
		fanout     = 4
		numEntries = 100000
		rangeSize  = 1000
		numQueries = 100
	)

	// 初始化树并插入数据
	tree := NewBPlusTree(0, fanout)
	for i := 0; i < numEntries; i++ {
		tree.Insert(util.Key(i), util.Value(fmt.Sprintf("v%d", i)))
	}

	// 强制提交临时节点
	if tree.tmp != nil && len(tree.tmp.KeyValues) > 0 {
		tree.commitTmpNode()
	}

	// 性能测试
	start := time.Now()

	// 执行多次范围查询
	totalResults := 0
	for i := 0; i < numQueries; i++ {
		startKey := util.Key(rand.Intn(numEntries - rangeSize))
		endKey := startKey + util.Key(rangeSize)

		results := tree.RangeSearch(startKey, endKey)
		totalResults += len(results)
	}

	elapsed := time.Since(start)

	// 计算性能指标
	queriesPerSec := float64(numQueries) / elapsed.Seconds()
	resultsPerSec := float64(totalResults) / elapsed.Seconds()

	t.Logf("范围查询性能：")
	t.Logf("数据量     : %d 条记录", numEntries)
	t.Logf("查询次数   : %d", numQueries)
	t.Logf("平均范围大小: %d", rangeSize)
	t.Logf("总耗时     : %v", elapsed.Round(time.Millisecond))
	t.Logf("查询吞吐量 : %.0f queries/sec", queriesPerSec)
	t.Logf("结果吞吐量 : %.0f results/sec", resultsPerSec)
	t.Logf("平均每次查询: %v/query", time.Duration(int64(elapsed)/int64(numQueries)))
}

// 测试空树的范围证明
func TestEmptyTree(t *testing.T) {
	fanout := 4
	tree := NewBPlusTree(0, fanout)

	startKey := util.Key(3)
	endKey := util.Key(4)

	results, proof := tree.GenerateRangeProof(startKey, endKey)
	hash := ReconstructRangeProof(startKey, endKey, results, proof)

	// 空树的根哈希应该是nil
	var expectedHash util.H256

	if hash != expectedHash {
		t.Errorf("哈希不匹配: 期望 %v, 得到 %v", expectedHash, hash)
	}
}

// 测试带有数据的树的范围证明
func TestInMemoryMBTree(t *testing.T) {
	fanout := 4
	n := 1234 // 使用较小的值以加快测试速度

	tree := NewBPlusTree(0, fanout)

	// 插入数据
	for i := 1; i <= n; i++ {
		key := util.Key(i * 2)
		value := []byte(fmt.Sprintf("value-%d", i * 2))
		tree.Insert(key, value)
	}

	fmt.Println("complete insertion")

	// 测试所有可能的范围
	for i := 1; i <= n; i++ { // 限制范围以加快测试
		for j := i; j <= n; j++ {
			startKey := util.Key(i)
			endKey := util.Key(j)

			results, proof := tree.GenerateRangeProof(startKey, endKey)
			hash := ReconstructRangeProof(startKey, endKey, results, proof)

			// 计算整棵树的哈希
			expectedHash := tree.root.GetHash()

			if hash != expectedHash {
				t.Errorf("范围 [%d, %d] 的哈希不匹配: 期望 %v, 得到 %v",
					startKey, endKey, expectedHash, hash)
			}
		}
	}
}


func TestInMemoryMBTree2(t *testing.T) {
	fanout := 4
	n := 1234 // 使用较小的值以加快测试速度

	tree := NewBPlusTree(0, fanout)

	// 插入数据
	for i := 1; i <= n; i++ {
		if i == 26 || i == 130 || i == 27 || i == 129 || i == 126{
			continue
		}
		key := util.Key(i)
		value := []byte(fmt.Sprintf("value-%d", i))
		tree.Insert(key, value)
	}

	fmt.Println("完成插入")
	// 测试所有可能的范围
	startKey := util.Key(26)
	endKey := util.Key(130)

	results, proof := tree.GenerateRangeProof(startKey, endKey)
	// 打印results 中的key
	for _, kv := range results {
		fmt.Print(kv.Key, " ")
	}
	fmt.Println()
	//fmt.Println("proof", proof)
	hash := ReconstructRangeProof(startKey, endKey, results, proof)

	// 计算整棵树的哈希
	expectedHash := tree.root.GetHash()

	if hash != expectedHash {
		t.Errorf("范围 [%d, %d] 的哈希不匹配: 期望 %v, 得到 %v",
			startKey, endKey, expectedHash, hash)
	}
}

func TestInMemoryMBTree3(t *testing.T) {
	fanout := 4

	tree := NewBPlusTree(0, fanout)

	// 插入数据
	for i := 36; i <= 54; i++ {
		if i == 44 || i == 45 || i == 46 {
			continue
		}
		key := util.Key(i)
		value := []byte(fmt.Sprintf("value-%d", i))
		tree.Insert(key, value)
	}

	fmt.Println("完成插入")
	// 测试所有可能的范围
	startKey := util.Key(44)
	endKey := util.Key(46)

	results, proof := tree.GenerateRangeProof(startKey, endKey)
	// 打印results 中的key
	for _, kv := range results {
		fmt.Print(kv.Key, " ")
	}
	fmt.Println()
	//fmt.Println("proof", proof)
	hash := ReconstructRangeProof(startKey, endKey, results, proof)

	// 计算整棵树的哈希
	expectedHash := tree.root.GetHash()

	if hash != expectedHash {
		t.Errorf("范围 [%d, %d] 的哈希不匹配: 期望 %v, 得到 %v",
			startKey, endKey, expectedHash, hash)
	}
}