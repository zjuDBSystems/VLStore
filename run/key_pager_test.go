package run

import (
	"VLStore/util"
	"math/rand"	
	"testing"
	"os"
	"fmt"
)

func TestKeyPager(t *testing.T) {
	fileName := "key_pager_test.dat"
	n := 10000
	os.Remove(fileName)

	writer, err := NewKeyPageWriter(fileName)
	if err != nil {
		t.Fatalf("创建写入器失败: %v", err)
	}

	rng := rand.New(rand.NewSource(1))

	keyoffsets := make([]KeyPos, 0, n)
	for i := 0; i < n; i++ {
		// 生成随机偏移量,大小为unit32
		pos := ChunkPosition{BlockNumber: uint32(rng.Uint32()), ChunkOffset: uint64(rng.Uint32()), ChunkSize: 1024}
		key := util.Key(i)
		keyoffsets = append(keyoffsets, KeyPos{Key: key, Pos: &pos})
		writer.Append(keyoffsets[i])
	}
	writer.Flush()

	reader := writer.ToKeyPageReader()
	if err != nil {
		t.Fatalf("创建读取器失败: %v", err)
	}

	// 多次读取测试
	for j := 0; j < 5; j++ {
		posL := rng.Intn(n)
		posR := rng.Intn(n)
		if posL > posR {
			posL, posR = posR, posL
		}
		
		fmt.Printf("测试第 %d 次, 读取位置范围: %d - %d\n", j, posL, posR)
		keyoffsets, err := reader.ReadDeserPageRange(posL, posR)
		if err != nil {
			t.Fatalf("读取失败: %v", err)
		}

		for i := 0; i < len(keyoffsets); i++ {
			if keyoffsets[i].Key != util.Key(posL + i) {
				t.Fatalf("键不匹配: 位置 %d, 期望 %d, 得到 %d", posL + i, util.Key(posL + i), keyoffsets[i].Key)
			}
		}
	}

	// 清理测试文件
	reader.File.Close()
	os.Remove(fileName)
}