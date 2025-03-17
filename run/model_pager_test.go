package run

import (
	"VLStore/util"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"testing"
	"time"
)

func TestModelPager(t *testing.T) {
	n := 10000
	fileName := "model.dat"
	writer, err := CreateModelPageWriter(fileName, 0)
	if err != nil {
		t.Fatalf("Failed to create model page writer: %v", err)
	}
	// 测试结束后删除文件
	defer os.Remove(fileName)

	modelVec := make([]*util.KeyModel, 0, n)
	for i := 0; i < n; i++ {
		// 创建随机键
		key := util.Key(i)
		model := &util.KeyModel{
			Start:     key,
			Slope:     1.0,
			Intercept: 2.0,
			LastIndex: i,
		}
		modelVec = append(modelVec, model)
		err := writer.Append(model)
		if err != nil {
			t.Fatalf("Failed to append model: %v", err)
		}
	}

	err = writer.Flush()
	if err != nil {
		t.Fatalf("Failed to flush writer: %v", err)
	}

	reader := writer.ToModelReader()

	for j := 0; j < 5; j++ {
		// 迭代读取页面
		start := time.Now()
		for i := 0; i < n; i++ {
			pageID := i / MAX_NUM_MODEL_IN_PAGE
			innerPagePos := i % MAX_NUM_MODEL_IN_PAGE
			collections, err := reader.ReadDeserPageAt(pageID)
			if err != nil {
				t.Fatalf("Failed to read page: %v", err)
			}
			state := collections.V[innerPagePos]

			// 验证模型是否相同
			if state.Start != modelVec[i].Start ||
				state.Slope != modelVec[i].Slope ||
				state.Intercept != modelVec[i].Intercept ||
				state.LastIndex != modelVec[i].LastIndex {
				t.Errorf("Model mismatch at index %d", i)
			}
		}
		elapse := time.Since(start).Nanoseconds() / int64(n)
		fmt.Printf("round %d, read state time: %d ns\n", j, elapse)
	}
}

// 测试流式模型构造器写入性能
func TestStreamingModel(t *testing.T) {
	//r := rand.New(rand.NewSource(1))
	epsilon := 46
	n := 1000000 // 减少数量以加快测试速度
	fileName := "model.dat"
	// 测试结束后删除文件
	defer os.Remove(fileName)

	keys := make([]util.Key, 0, n)
	for i := 0; i < n; i++ {
		// 创建随机键
		//key := util.Key(r.Int63())
		key := util.Key(rand.Intn(100000000))
		keys = append(keys, key)
	}

	// 排序键
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})

	start := time.Now()
	streamModelConstructor, err := NewStreamModelConstructor(fileName, epsilon)
	if err != nil {
		t.Fatalf("Failed to create stream model constructor: %v", err)
	}

	pointVec := make([]util.Point, 0, n)

	for i, key := range keys {
		err := streamModelConstructor.AppendKey(key)
		if err != nil {
			t.Fatalf("Failed to append state key: %v", err)
		}
		pointVec = append(pointVec, util.Point{X: int(key), Y: i})
	}

	err = streamModelConstructor.FinalizeAppend()
	if err != nil {
		t.Fatalf("Failed to finalize append: %v", err)
	}

	elapse := time.Since(start)
	// 计算吞吐量 (ops/s)
	throughput := float64(n) / elapse.Seconds()
	fmt.Printf("throughput: %.2f ops/s\n", throughput)

	// writer := streamModelConstructor.OutputModelWriter
	// reader := writer.ToModelReader()

	// numOfPages := reader.NumStoredPages
	// for i := 0; i < numOfPages; i++ {
	// 	collection, err := reader.ReadDeserPageAt(i)
	// 	if err != nil {
	// 		t.Fatalf("Failed to read page: %v", err)
	// 	}
	// 	fmt.Printf("collection size: %d, model_level: %d\n", len(collection.V), collection.ModelLevel)
	// }

	// start = time.Now()
	// for _, point := range pointVec {
	// 	key := util.Key(point.X)
	// 	truePos := int(point.Y)
	// 	predPos, err := reader.GetPredStatePos(key, epsilon)
	// 	if err != nil {
	// 		t.Fatalf("Failed to get predicted position: %v", err)
	// 	}

	// 	diff := float64(truePos - predPos)
	// 	if diff < 0 {
	// 		diff = -diff
	// 	}

	// 	// 验证预测误差在允许范围内
	// 	if diff > float64(epsilon+1) {
	// 		t.Errorf("Prediction error too large: key=%d, true_pos=%d, pred_pos=%d, diff=%f", key, truePos, predPos, diff)
	// 	}
	// }

	// elapse = time.Since(start)
	// fmt.Printf("avg pred time: %d ns\n", elapse.Nanoseconds()/int64(n))
}
