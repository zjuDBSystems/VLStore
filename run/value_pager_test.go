package run

import (
	"VLStore/util"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"
)

func TestValuePager(t *testing.T) {
	// 设置测试参数
	fileName := "values.dat"
	n := 10000

	// 清理可能存在的测试文件
	os.Remove(fileName)

	// 创建写入器
	writer, err := NewValuePageWriter(fileName)
	if err != nil {
		t.Fatalf("创建写入器失败: %v", err)
	}

	// 使用固定种子初始化随机数生成器，以便测试结果可重现
	rng := rand.New(rand.NewSource(1))

	// 生成测试数据
	valueVec := make([]util.Value, 0, n)
	for i := 0; i < n; i++ {
		// 创建随机值
		value := make([]byte, 32) // 使用固定大小简化测试
		rng.Read(value)

		valueVec = append(valueVec, value)
		writer.Append(util.Key(i), value)
	}

	// 确保所有数据都写入文件
	writer.Flush()

	// 创建读取器
	reader := writer.ToValueReader()

	// 多次读取测试，测量性能
	for j := 0; j < 5; j++ {
		start := time.Now()

		// 读取所有值并验证
		currentPos := 0
		currentPage := 0

		for i := 0; i < n; i++ {
			// 找到包含当前值的页面
			keyValues := reader.ReadPageAt(currentPage)
			for len(keyValues) <= currentPos {
				currentPos -= len(keyValues)
				currentPage++
				keyValues = reader.ReadPageAt(currentPage)
			}

			// 验证键和值
			if keyValues[currentPos].Key != util.Key(i) {
				t.Errorf("键不匹配: 位置 %d, 期望 %d, 得到 %d", i, i, keyValues[currentPos].Key)
			}
			if !bytesEqual(keyValues[currentPos].Value, valueVec[i]) {
				t.Errorf("值不匹配: 位置 %d", i)
			}

			currentPos++
		}

		elapsed := time.Since(start).Nanoseconds() / int64(n)
		fmt.Printf("轮次 %d, 读取键值对时间: %d ns/键值对\n", j, elapsed)
	}

	// 关闭读取器
	reader.File.Close()

	// 测试迭代器
	reader, err = LoadValuePageReader(fileName)
	if err != nil {
		t.Fatalf("加载读取器失败: %v", err)
	}

	iterator := reader.ToKeyValueIterator()
	count := 0

	for iterator.HasNext() {
		keyValue := iterator.Next()
		if keyValue.Key != util.Key(count) {
			t.Errorf("迭代器键不匹配: 位置 %d, 期望 %d, 得到 %d", count, count, keyValue.Key)
		}
		if !bytesEqual(keyValue.Value, valueVec[count]) {
			t.Errorf("迭代器值不匹配: 位置 %d", count)
		}
		count++
	}

	if count != n {
		t.Errorf("迭代器计数不匹配: 期望 %d, 得到 %d", n, count)
	}

	// 测试范围读取
	keyValuesRange := reader.ReadKeyValuesRange(100, 200)
	if len(keyValuesRange) != 101 {
		t.Errorf("范围读取长度不匹配: 期望 %d, 得到 %d", 101, len(keyValuesRange))
	}
	for i := 0; i < len(keyValuesRange); i++ {
		expectedKey := util.Key(i + 100)
		if keyValuesRange[i].Key != expectedKey {
			t.Errorf("范围读取键不匹配: 位置 %d, 期望 %d, 得到 %d", i, expectedKey, keyValuesRange[i].Key)
		}
		if !bytesEqual(keyValuesRange[i].Value, valueVec[i+100]) {
			t.Errorf("范围读取值不匹配: 位置 %d", i)
		}
	}

	// 清理测试文件
	reader.File.Close()
	os.Remove(fileName)
}

// bytesEqual 比较两个字节切片是否相等
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
