package run

import (
	"VLStore/util"
	"fmt"
	"os"
	"testing"
	"time"
)

func TestHashPager(t *testing.T) {
	n := 1000000

	// 创建一个临时文件用于测试
	fileName := "hash_test.dat"
	defer os.Remove(fileName) // 测试结束后删除文件

	// 创建HashPageWriter
	writer, err := NewHashPageWriter(fileName)
	if err != nil {
		t.Fatalf("Failed to create HashPageWriter: %v", err)
	}

	// 生成随机哈希并存储
	hashVec := make([]util.H256, 0, n)
	for i := 0; i < n; i++ {
		// 使用确定性的方式生成哈希，类似于原测试中的随机数生成器
		hash := hashFromLowU64BE(uint64(i))
		hashVec = append(hashVec, hash)
		writer.Append(hash)
	}

	// 刷新写入器
	writer.Flush()

	// 创建读取器
	reader := writer.ToHashReader()

	// 多次读取测试性能
	for j := 0; j < 3; j++ {
		start := time.Now()

		for i := 0; i < n; i++ {
			pageId := i / MAX_NUM_HASH_IN_PAGE
			innerPagePos := i % MAX_NUM_HASH_IN_PAGE

			// 读取页面
			v, err := reader.ReadPageAt(pageId)
			if err != nil {
				t.Fatalf("Failed to read page at %d: %v", pageId, err)
			}

			// 确保页面中有足够的哈希
			if innerPagePos >= len(v) {
				t.Fatalf("Page %d does not have enough hashes, expected at least %d, got %d",
					pageId, innerPagePos+1, len(v))
			}

			// 验证哈希值
			hash := v[innerPagePos]
			if hash != hashVec[i] {
				t.Fatalf("Hash mismatch at position %d: expected %v, got %v", i, hashVec[i], hash)
			}
		}

		elapsed := time.Since(start).Nanoseconds() / int64(n)
		fmt.Printf("round %d, read hash time: %d ns/op\n", j, elapsed)
	}
}


func TestConstructMHT(t *testing.T) {
	n := 1000
	fanout := 16

	start := time.Now()

	// 创建构造器
	constructor, err := NewStreamMHTConstructor("hash.dat", fanout, util.NewHasher(util.SHA256))
	if err != nil {
		t.Fatalf("Failed to create constructor: %v", err)
	}

	// 最后删除hash.dat
	defer os.Remove("hash.dat")

	// 添加哈希
	for i := 0; i < n; i++ {
		hash := hashFromLowU64BE(uint64(i))
		constructor.Append(hash)
	}

	// 构建MHT
	constructor.BuildMHT()

	elapse := time.Since(start).Nanoseconds()
	fmt.Printf("elapse: %d\n", elapse)

	// 创建读取器
	reader := constructor.OutputMHTWriter.ToHashReader()
	root := reader.Root
	fmt.Printf("root: %v\n", root)
	

	// 创建叶子哈希集合
	leafHashCollection := make([]util.H256, n)
	for i := 0; i < n; i++ {
		leafHashCollection[i] = hashFromLowU64BE(uint64(i))
	}
	//fmt.Println("leafHashCollection: ", leafHashCollection)
	// 验证
	cnt := 0
	verifyStart := time.Now()

	for i := 0; i < n; i++ {
		for j := i; j < n; j++ {
			
			// 获取非叶子证明
			proof := reader.proveNonLeaf(i, j, n, fanout)
			
			// 获取叶子证明
			proveLeaf(i, j, n, fanout, proof, leafHashCollection)

			// 获取对象哈希
			objHashes := leafHashCollection[i : j+1]

			// 重构范围证明
			hRe := ReconstructRangeProof(proof, fanout, objHashes, constructor.Hasher)

			// 验证
			if hRe != root {
				t.Errorf("i: %d, j: %d, h_re: %v, root: %v", i, j, hRe, root)
			}

			cnt++
		}
	}

	averageVerify := time.Since(verifyStart).Nanoseconds() / int64(cnt)
	fmt.Printf("average verify: %d\n", averageVerify)
}

// 辅助函数：从低位u64创建H256
func hashFromLowU64BE(value uint64) util.H256 {
	var hash util.H256

	// 将uint64值以大端序放入H256的低8字节
	for i := 0; i < 8; i++ {
		hash[24+i] = byte(value >> (8 * (7 - i)))
	}

	return hash
}

// 辅助函数
func proveLeaf(l int, r int, numOfData int, fanout int, proof *RangeProof, leafHashCollection []util.H256){
	levelL := l
	levelR := r

	proofPosL := levelL - levelL % fanout
	var proofPosR int
	if levelR - levelR % fanout + fanout > numOfData {
		proofPosR = numOfData - 1
	} else {
		proofPosR = levelR - levelR % fanout + fanout - 1
	}

	leafHashes := make([]util.H256, proofPosR - proofPosL + 1)
	for i := proofPosL; i <= proofPosR; i++ {
		leafHashes[i - proofPosL] = leafHashCollection[i]
	}

	for i := 0; i < levelR - levelL + 1; i++ {
		// leafHashes.remove(levelL - proofPosL)
		leafHashes = append(leafHashes[:levelL - proofPosL], leafHashes[levelL - proofPosL + 1:]...)
	}

	// proof.p.insert(0, leafHashes)
	proof.p = append([][]util.H256{leafHashes}, proof.p...)
}