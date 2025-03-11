package util

import (
	"math/rand"
	"sort"
	"testing"
	"time"
)

func TestModelGenerator(t *testing.T) {
	// 创建一个新的模型生成器，epsilon设为2
	generator := NewModelGenerator(2)

	// 添加一些测试数据
	keys := []Key{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}
	positions := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

	// 将数据添加到模型中
	for i := 0; i < len(keys); i++ {
		success := generator.Append(keys[i], positions[i])
		if !success {
			t.Errorf("添加点 (%d, %d) 失败", keys[i], positions[i])
		}
	}

	// 检查凸包是否为空
	if generator.IsHullEmpty() {
		t.Error("凸包不应该为空")
	}

	// 完成模型
	model := generator.FinalizeModel()

	// 验证模型的起始键
	if model.GetStart() != 10 {
		t.Errorf("模型起始键应为10，实际为%d", model.GetStart())
	}

	// 验证模型的最后索引
	if model.GetLastIndex() != 9 {
		t.Errorf("模型最后索引应为9，实际为%d", model.GetLastIndex())
	}

	// 测试预测功能
	testCases := []struct {
		key      Key
		expected int
	}{
		{10, 0},
		{20, 1},
		{30, 2},
		{40, 3},
		{50, 4},
		{60, 5},
		{70, 6},
		{80, 7},
		{90, 8},
		{100, 9},
		{15, 0},  // 在10和20之间，应该预测为0或1
		{55, 4},  // 在50和60之间，应该预测为4或5
		{110, 9}, // 超出范围，应该预测为最后一个位置
	}

	for _, tc := range testCases {
		result := model.Predict(tc.key)
		// 由于线性插值的特性，预测结果可能会有一定的误差
		// 这里我们检查预测结果是否在合理范围内
		if tc.key <= 100 && tc.key >= 10 {
			expected := int((float64(tc.key) - 10.0) / 10.0)
			if result < expected-1 || result > expected+1 {
				t.Errorf("预测键%d的结果为%d，期望在%d附近", tc.key, result, expected)
			}
		} else if tc.key > 100 && result != 9 {
			t.Errorf("预测键%d的结果为%d，期望为9", tc.key, result)
		}
	}

	// 测试序列化和反序列化
	bytes := model.ToBytes()
	if len(bytes) != MODEL_SIZE {
		t.Errorf("序列化后的字节长度应为%d，实际为%d", MODEL_SIZE, len(bytes))
	}

	deserializedModel := KeyModelFromBytes(bytes)
	if deserializedModel.GetStart() != model.GetStart() {
		t.Errorf("反序列化后的起始键应为%d，实际为%d", model.GetStart(), deserializedModel.GetStart())
	}

	if deserializedModel.GetLastIndex() != model.GetLastIndex() {
		t.Errorf("反序列化后的最后索引应为%d，实际为%d", model.GetLastIndex(), deserializedModel.GetLastIndex())
	}

	slope1, intercept1 := model.GetSlopeIntercept()
	slope2, intercept2 := deserializedModel.GetSlopeIntercept()
	if slope1 != slope2 || intercept1 != intercept2 {
		t.Errorf("反序列化后的斜率和截距不匹配")
	}
}

func TestFetchModelAndPredict(t *testing.T) {
	// 创建一些测试模型
	models := []*KeyModel{
		{Start: 10, Slope: 0.1, Intercept: 0, LastIndex: 9},
		{Start: 100, Slope: 0.1, Intercept: 10, LastIndex: 19},
		{Start: 200, Slope: 0.1, Intercept: 20, LastIndex: 29},
	}

	// 测试预测功能
	testCases := []struct {
		key      Key
		expected int
	}{
		{10, 0},
		{50, 4},
		{100, 10},
		{150, 15},
		{200, 20},
		{250, 25},
		{300, 29}, // 超出最后一个模型的范围，应该预测为最后一个位置
	}

	for _, tc := range testCases {
		result := FetchModelAndPredict(models, tc.key)
		if result != tc.expected {
			t.Errorf("预测键%d的结果为%d，期望为%d", tc.key, result, tc.expected)
		}
	}
}

func TestLargeDataset(t *testing.T) {
	// 跳过大数据集测试，除非明确要求运行
	if testing.Short() {
		t.Skip("跳过大数据集测试")
	}

	// 创建一个新的模型生成器，epsilon设为5
	generator := NewModelGenerator(5)

	// 生成大量随机数据
	const dataSize = 10000
	keys := make([]Key, dataSize)
	positions := make([]int, dataSize)

	// 生成有序的键
	for i := 0; i < dataSize; i++ {
		keys[i] = Key(i * 10)
		positions[i] = i
	}

	// 将数据添加到模型中
	for i := 0; i < dataSize; i++ {
		generator.Append(keys[i], positions[i])
	}

	// 完成模型
	model := generator.FinalizeModel()

	// 验证模型的起始键和最后索引
	if model.GetStart() != 0 {
		t.Errorf("模型起始键应为0，实际为%d", model.GetStart())
	}

	if model.GetLastIndex() != dataSize-1 {
		t.Errorf("模型最后索引应为%d，实际为%d", dataSize-1, model.GetLastIndex())
	}

	// 测试预测功能
	errorCount := 0
	maxError := 0

	for i := 0; i < dataSize; i++ {
		key := Key(i * 10)
		expected := i
		result := model.Predict(key)

		error := abs(result - expected)
		if error > maxError {
			maxError = error
		}

		if error > 10 { // 允许一定的误差
			errorCount++
		}
	}

	// 报告误差统计
	t.Logf("最大误差: %d", maxError)
	t.Logf("误差大于10的预测数量: %d (%.2f%%)", errorCount, float64(errorCount)/float64(dataSize)*100)

	// 确保大多数预测都在可接受的误差范围内
	if float64(errorCount)/float64(dataSize) > 0.05 {
		t.Errorf("预测误差过大，超过5%%的预测误差大于10")
	}
}

func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

func BenchmarkModelPredict(b *testing.B) {
	// 创建一个测试模型
	model := &KeyModel{
		Start:     0,
		Slope:     0.1,
		Intercept: 0,
		LastIndex: 1000,
	}

	// 生成一些随机键
	keys := make([]Key, 1000)
	for i := 0; i < 1000; i++ {
		keys[i] = Key(rand.Intn(10000))
	}

	b.ResetTimer()

	// 基准测试预测功能
	for i := 0; i < b.N; i++ {
		key := keys[i%1000]
		model.Predict(key)
	}
}

func BenchmarkFetchModelAndPredict(b *testing.B) {
	// 创建一些测试模型
	const modelCount = 100
	models := make([]*KeyModel, modelCount)

	for i := 0; i < modelCount; i++ {
		models[i] = &KeyModel{
			Start:     Key(i * 100),
			Slope:     0.1,
			Intercept: float64(i * 10),
			LastIndex: uint32(i*10 + 9),
		}
	}

	// 生成一些随机键
	keys := make([]Key, 1000)
	for i := 0; i < 1000; i++ {
		keys[i] = Key(rand.Intn(modelCount * 100))
	}

	b.ResetTimer()

	// 基准测试预测功能
	for i := 0; i < b.N; i++ {
		key := keys[i%1000]
		FetchModelAndPredict(models, key)
	}
}

// 模拟CompoundKey的测试
func TestCompoundKey(t *testing.T) {
	epsilon := int64(23)
	n := 100000

	// 使用固定种子初始化随机数生成器
	r := rand.New(rand.NewSource(1))

	// 生成测试数据
	var keys []Key
	for i := 0; i < n; i++ {
		// 生成随机的Key值
		key := Key(r.Int63n(1000000) + int64(i*2))
		keys = append(keys, key)
	}

	// 排序keys
	sortKeys(keys)

	t.Logf("完成排序，共 %d 个键", len(keys))

	// 创建模型生成器
	modelGenerator := NewModelGenerator(epsilon)
	var modelVec []*KeyModel

	// 添加键到模型中
	for i, key := range keys {
		success := modelGenerator.Append(key, i)
		if !success {
			model := modelGenerator.FinalizeModel()
			modelVec = append(modelVec, model)
			modelGenerator.Append(key, i)
		}
	}

	// 如果还有未完成的模型，添加到模型向量中
	if !modelGenerator.IsHullEmpty() {
		model := modelGenerator.FinalizeModel()
		modelVec = append(modelVec, model)
	}

	t.Logf("模型向量长度: %d", len(modelVec))

	// 测试预测功能
	var maxDiff float64
	var errorCount int

	for i, key := range keys {
		predictPos := FetchModelAndPredict(modelVec, key)
		diff := float64(abs(i - predictPos))

		if diff > float64(epsilon+1) {
			t.Logf("i: %d, pred_pos: %d, diff: %.0f", i, predictPos, diff)
			errorCount++
		}

		if diff > maxDiff {
			maxDiff = diff
		}
	}

	t.Logf("最大误差: %.0f", maxDiff)
	t.Logf("误差大于 %d 的预测数量: %d (%.2f%%)", epsilon+1, errorCount, float64(errorCount)/float64(len(keys))*100)

	// 测试边界情况
	if len(keys) > 0 {
		low := keys[0] - 10
		t.Logf("低于范围的键: %d", low)
		pred := FetchModelAndPredict(modelVec, low)
		t.Logf("预测位置: %d", pred)
	}
}

// 大规模PGM测试
func TestPGM(t *testing.T) {
	epsilon := int64(23)
	n := 100000 // 减少测试规模，原始是100000

	// 使用固定种子初始化随机数生成器
	r := rand.New(rand.NewSource(1))

	// 生成测试数据
	var keys []Key
	for i := 0; i < n; i++ {
		// 生成随机的Key值，但保持递增趋势
		key := Key(r.Int63n(100) + int64(i))
		keys = append(keys, key)
	}

	// 排序keys
	sortKeys(keys)

	t.Logf("完成排序，共 %d 个键", len(keys))

	// 创建模型生成器
	modelGenerator := NewModelGenerator(epsilon)
	var modelVec []*KeyModel

	// 添加键到模型中
	startTime := time.Now()
	for i, key := range keys {
		success := modelGenerator.Append(key, i)
		if !success {
			model := modelGenerator.FinalizeModel()
			modelVec = append(modelVec, model)
			modelGenerator.Append(key, i)
		}
	}

	// 如果还有未完成的模型，添加到模型向量中
	if !modelGenerator.IsHullEmpty() {
		model := modelGenerator.FinalizeModel()
		modelVec = append(modelVec, model)
	}

	buildTime := time.Since(startTime)
	t.Logf("构建模型耗时: %v", buildTime)
	t.Logf("模型向量长度: %d", len(modelVec))
	t.Logf("平均每个模型覆盖的键数: %.2f", float64(len(keys))/float64(len(modelVec)))

	// 测试预测功能
	var maxDiff float64 
	var errorCount int

	startTime = time.Now()
	for i, key := range keys {
		predictPos := FetchModelAndPredict(modelVec, key)
		diff := float64(abs(i - predictPos))

		if diff > float64(epsilon+1) {
			t.Logf("i: %d, pred_pos: %d, diff: %.0f", i, predictPos, diff)
			errorCount++
		}

		if diff > maxDiff {
			maxDiff = diff
		}
	}

	queryTime := time.Since(startTime)
	t.Logf("查询耗时: %v", queryTime)
	t.Logf("平均每次查询耗时: %v", queryTime/time.Duration(len(keys)))
	t.Logf("最大误差: %.0f", maxDiff)
	t.Logf("误差大于 %d 的预测数量: %d (%.2f%%)", epsilon+1, errorCount, float64(errorCount)/float64(len(keys))*100)

	// 确保大多数预测都在可接受的误差范围内
	if float64(errorCount)/float64(len(keys)) > 0.05 {
		t.Errorf("预测误差过大，超过5%%的预测误差大于%d", epsilon+1)
	}
}

// 辅助函数：排序Key切片
func sortKeys(keys []Key) {
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})
}

// 测试模型的序列化和反序列化
func TestModelSerialization(t *testing.T) {
	// 创建一个测试模型
	model := &KeyModel{
		Start:     100,
		Slope:     0.1,
		Intercept: 5.0,
		LastIndex: 1000,
	}

	// 序列化模型
	bytes := model.ToBytes()

	// 反序列化模型
	deserializedModel := KeyModelFromBytes(bytes)

	// 验证反序列化后的模型与原始模型相同
	if deserializedModel.Start != model.Start {
		t.Errorf("反序列化后的起始键不匹配，期望 %d，实际 %d", model.Start, deserializedModel.Start)
	}

	if deserializedModel.LastIndex != model.LastIndex {
		t.Errorf("反序列化后的最后索引不匹配，期望 %d，实际 %d", model.LastIndex, deserializedModel.LastIndex)
	}

	if deserializedModel.Slope != model.Slope {
		t.Errorf("反序列化后的斜率不匹配，期望 %f，实际 %f", model.Slope, deserializedModel.Slope)
	}

	if deserializedModel.Intercept != model.Intercept {
		t.Errorf("反序列化后的截距不匹配，期望 %f，实际 %f", model.Intercept, deserializedModel.Intercept)
	}

	// 测试预测功能是否一致
	testKeys := []Key{50, 100, 150, 200, 250}
	for _, key := range testKeys {
		pred1 := model.Predict(key)
		pred2 := deserializedModel.Predict(key)
		if pred1 != pred2 {
			t.Errorf("键 %d 的预测结果不一致，原始模型: %d，反序列化模型: %d", key, pred1, pred2)
		}
	}
}
