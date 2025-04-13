package run

import (
	"VLStore/util"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"sort"

	"github.com/bits-and-blooms/bloom/v3"
)

const (
	FILTER_FP_RATE  = 0.1         // 布隆过滤器的误报率
	MAX_FILTER_SIZE = 1024 * 1024 // 最大过滤器大小：1MB
)

// LevelRun 定义了一个层级中的运行
type LevelRun struct {
	ComponentID int                // 组件ID
	ValueReader *ValuePageReader   // 值读取器
	ModelReader *ModelPageReader   // 模型读取器
	MHTReader   *HashPageReader    // MHT读取器
	Digest      util.H256          // MHT根摘要
}

// Load 根据运行ID、层级ID和配置加载一个运行
func Load(componentID int, levelID int, configs *util.Configs) (*LevelRun, error) {
	// 定义值、模型、MHT和过滤器的文件名
	valueFileName := FileName(componentID, levelID, configs.DirName, "v")
	modelFileName := FileName(componentID, levelID, configs.DirName, "m")
	mhtFileName := FileName(componentID, levelID, configs.DirName, "h")
	filterFileName := FileName(componentID, levelID, configs.DirName, "f")

	// 加载三个读取器
	valueReader, err := LoadValuePageReader(valueFileName)
	if err != nil {
		return nil, err
	}

	modelReader, err := LoadModelPageReader(modelFileName)
	if err != nil {
		return nil, err
	}

	mhtReader, err := LoadHashPageReader(mhtFileName)
	if err != nil {
		return nil, err
	}

	// 初始化过滤器为nil
	var filter *bloom.BloomFilter = nil

	// 如果过滤器文件存在，从文件中读取过滤器
	if file, err := os.Open(filterFileName); err == nil {
		defer file.Close()

		// 读取长度
		var lenBytes [4]byte
		file.Read(lenBytes[:])
		length := binary.BigEndian.Uint32(lenBytes[:])

		// 读取过滤器数据
		data := make([]byte, length)
		file.Read(data)

		// 反序列化过滤器
		filter = &bloom.BloomFilter{}
		if err := filter.UnmarshalBinary(data); err != nil {
			return nil, err
		}
	}

	// 获取MHT根哈希
	mhtRoot := mhtReader.Root

	// 计算摘要
	digest := mhtRoot
	return &LevelRun{
		ComponentID: componentID,
		ValueReader: valueReader,
		ModelReader: modelReader,
		MHTReader:   mhtReader,
		Digest:      digest,
	}, nil
}

// ConstructRunByInMemoryTree 使用内存迭代器构建运行
func ConstructRunByInMemoryTree(inputs *InMemKeyValueIterator, componentID int, levelID int, dirName string, epsilon int, fanout int, maxNumOfValue int, levelNumOfRun int, sizeRatio int) (*LevelRun, error) {
	valueFileName := FileName(componentID, levelID, dirName, "v")
	modelFileName := FileName(componentID, levelID, dirName, "m")
	mhtFileName := FileName(componentID, levelID, dirName, "h")

	// 根据输入值构建新的值文件、模型文件、MHT文件，并将值键插入过滤器
	valueWriter, err := NewValuePageWriter(valueFileName)
	if err != nil {
		return nil, err
	}

	modelConstructor, err := NewStreamModelConstructor(modelFileName, epsilon)
	if err != nil {
		return nil, err
	}

	mhtConstructor, err := NewStreamMHTConstructor(mhtFileName, fanout, util.NewHasher(util.BLAKE3))
	if err != nil {
		return nil, err
	}

	// 遍历迭代器
	for inputs.HasNext() {
		keyValue := inputs.Next()
		// add the KV's key to the model constructor
		modelConstructor.AppendKey(keyValue.Key)
		// add the KV's hash to the mht constructor
		mhtConstructor.Append(keyValue.ComputeHash(util.NewHasher(util.BLAKE3)))
		// add the KV's value to the value writer
		valueWriter.Append(keyValue.Key, keyValue.Value)
	}
	// flush the value writer
	valueWriter.Flush()
	// finalize the model constructor
	modelConstructor.FinalizeAppend()
	// build the mht
	mhtConstructor.BuildMHT()

	// 创建读取器
	valueReader := valueWriter.ToValueReader()
	modelReader := modelConstructor.OutputModelWriter.ToModelReader()
	mhtReader := mhtConstructor.OutputMHTWriter.ToHashReader()

	// 获取MHT根哈希
	mhtRoot := mhtReader.Root

	// 计算摘要
	digest := mhtRoot

	return &LevelRun{
		ComponentID: componentID,
		ValueReader: valueReader,
		ModelReader: modelReader,
		MHTReader:   mhtReader,
		Digest:      digest,
	}, nil
}

// SearchRun 在运行中搜索键
func (lr *LevelRun) SearchRun(key util.Key, configs *util.Configs) *util.KeyValue {
	// 使用模型文件预测值文件中的位置
	// 计算边界键
	epsilon := configs.Epsilon

	// 使用模型文件预测位置
	predPos, err := lr.ModelReader.GetPredStatePos(key, epsilon)
	if err != nil {
		panic(err)
	}

	numOfValues := lr.ValueReader.NumKeyValues

	// 根据预测位置和epsilon计算下界和上界位置
	posL := int(math.Min(math.Max(float64(predPos-epsilon-1), 0), float64(numOfValues-1)))
	posR := int(math.Min(float64(predPos+epsilon+2), float64(numOfValues-1)))

	// 从给定范围[posL, posR]加载键值对
	keyValues := lr.ValueReader.ReadKeyValuesRange(posL, posR)

	// 在键值对中查找目标键
	for _, kv := range keyValues {
		if kv.Key == key {
			return &kv
		}
	}
	return nil
}

// FileName 生成不同文件类型的文件名
func FileName(runID int, levelID int, dirName string, fileType string) string {
	return fmt.Sprintf("%s/%s_%d_%d.dat", dirName, fileType, levelID, runID)
}

// LoadKeyValues 加载LevelRun中的所有键值对
func (lr *LevelRun) LoadKeyValues() []util.KeyValue {
	fileInfo, err := lr.ValueReader.File.Stat()
	if err != nil {
		panic(err)
	}
	valuePageNum := int(fileInfo.Size() / PAGE_SIZE)
	result := make([]util.KeyValue, 0)
	for pageID := 0; pageID < valuePageNum; pageID++ {
		pageKeyValues := lr.ValueReader.ReadPageAt(pageID)
		result = append(result, pageKeyValues...)
	}
	return result
}

type RunProof struct {
	rangeProof *RangeProof
}

func NewRunProof() *RunProof {
	return &RunProof{
		rangeProof: nil,
	}
}

func (lr *LevelRun) ProveLeaf(l, r, numOfData, fanout int, proof *RangeProof) {
	levelL := l
	levelR := r
	proofPosL := levelL - levelL%fanout
	proofPosR := levelR - levelR%fanout + fanout
	if proofPosR > numOfData {
		proofPosR = numOfData
	}
	proofPosR--

	// Read key-values from value reader
	keyValues := lr.ValueReader.ReadKeyValuesRange(proofPosL, proofPosR)

	// Create leaf hashes
	leafHashes := make([]util.H256, 0, len(keyValues))
	for _, kv := range keyValues {
		leafHashes = append(leafHashes, kv.ComputeHash(util.NewHasher(util.BLAKE3)))
	}
	
	// Remove the hashes that are in the proven range
	for i := 0; i < (levelR - levelL + 1); i++ {
		leafHashes = append(leafHashes[:levelL-proofPosL], leafHashes[levelL-proofPosL+1:]...)
	}

	// Insert at the beginning of the proof path
	proof.p = append([][]util.H256{leafHashes}, proof.p...)
}

/*
Generate the result and the RunProof
If the filter does show that the addr_key does not exist, use the filter + MHT root as the proof.
If the filter cannot show, use the MHT to prove the result, add the filter's hash to the proof.
*/

func (lr *LevelRun) ProveRange(startKey, endKey util.Key, configs *util.Configs) ([]util.KeyValue, *RunProof) {
	// init the proof
	proof := NewRunProof()

	epsilon := configs.Epsilon

	// Use model file to predict positions
	predPosLow, err := lr.ModelReader.GetPredStatePos(startKey, epsilon)
	if err != nil {
		panic(err)
	}
	predPosUpper, err := lr.ModelReader.GetPredStatePos(endKey, epsilon)
	if err != nil {
		panic(err)
	}

	numOfValues := lr.ValueReader.NumKeyValues

	// Compute position boundaries based on predictions and epsilon
	posL := int(math.Min(math.Max(float64(predPosLow-epsilon-1), 0), float64(numOfValues-1)))
	posR := int(math.Min(float64(predPosUpper+epsilon+3), float64(numOfValues-1)))

	// 打印posL 和 posR
	//fmt.Println("posL: ", posL)
	//fmt.Println("posR: ", posR)

	// Load key-values from the range
	keyValues := lr.ValueReader.ReadKeyValuesRange(posL, posR)
	if len(keyValues) == 0 {
		return nil, proof
	}
	// Binary search to find exact positions in the retrieved data
	lowerIndex := sort.Search(len(keyValues), func(i int) bool {
		return keyValues[i].Key >= startKey
	})
	upperIndex := sort.Search(len(keyValues), func(i int) bool {
		return keyValues[i].Key >= endKey
	})
	// 打印lowerIndex 和 upperIndex
	//fmt.Println("lowerIndex: ", lowerIndex)
	//fmt.Println("upperIndex: ", upperIndex)

	// Derive the actual position by adding offset pos_l
	leftProofPos := lowerIndex + posL
	rightProofPos := upperIndex + posL
	// 打印leftProofPos 和 rightProofPos
	//fmt.Println("leftProofPos: ", leftProofPos)
	//fmt.Println("rightProofPos: ", rightProofPos)

	// Adjust boundary positions
	if leftProofPos > 0 && rightProofPos < numOfValues-1{
		if keyValues[lowerIndex].Key == startKey {
			leftProofPos--
		}
		if keyValues[upperIndex].Key == endKey {
			rightProofPos++
		}
	}
	if rightProofPos == numOfValues {
		rightProofPos = numOfValues - 1
	}
	
	// 打印leftProofPos 和 rightProofPos
	//fmt.Println("leftProofPos: ", leftProofPos)
	//fmt.Println("rightProofPos: ", rightProofPos)
	// Get the result key-value pairs
	resultData := keyValues[leftProofPos-posL : rightProofPos-posL+1]
	// 打印resultData 中的key
	//fmt.Println("resultData :")
	//for _, kv := range resultData {
	//	fmt.Print(kv.Key, " ")
	//}
	//fmt.Println()

	// Generate non-leaf range proof
	fanout := configs.Fanout
	rangeProof := lr.MHTReader.proveNonLeaf(leftProofPos, rightProofPos, numOfValues, fanout)

	// Generate leaf range proof
	lr.ProveLeaf(leftProofPos, rightProofPos, numOfValues, fanout, rangeProof)

	proof.rangeProof = rangeProof

	return resultData, proof
}

// VerifyRunProof verifies a run proof against a known root hash
func VerifyRunProof(startKey, endKey util.Key, results []util.KeyValue, proof *RunProof, fanout int, rootHash util.H256) bool {
	for i, result := range results {
		if i == 0 {
			if result.Key >= startKey {
				return false
			}
		}else if i == len(results)-1 {
			if result.Key <= endKey {
				return false
			}
		}else{
			if result.Key < startKey || result.Key > endKey {
				return false
			}
		}
	}
	rangeProof := proof.rangeProof
	resultsHashes := make([]util.H256, len(results))
	for i, result := range results {
		resultsHashes[i] = result.ComputeHash(util.NewHasher(util.BLAKE3))
	}

	
	reconstructMerkleRoot := ReconstructRangeProof(rangeProof, fanout, resultsHashes, util.NewHasher(util.BLAKE3))
	return reconstructMerkleRoot == rootHash
}

func ReconstructRunProof(startKey, endKey util.Key, results []util.KeyValue, proof *RunProof, fanout int) (bool, util.H256) {
	// 打印 results 中的第一个和最后一个key
	//fmt.Println("results[0].Key: ", results[0].Key)
	//fmt.Println("results[len(results)-1].Key: ", results[len(results)-1].Key)
	// for i, result := range results {
	// 	if i == 0 {
	// 		if result.Key > startKey {
	// 			return false, util.H256{}
	// 		}
	// 	}else if i == len(results)-1 {
	// 		if result.Key < endKey {
	// 			return false, util.H256{}
	// 		}
	// 	}else{
	// 		if result.Key < startKey || result.Key > endKey {
	// 			return false, util.H256{}
	// 		}
	// 	}
	// }
	rangeProof := proof.rangeProof
	resultsHashes := make([]util.H256, len(results))
	for i, result := range results {
		resultsHashes[i] = result.ComputeHash(util.NewHasher(util.BLAKE3))
	}
	
	reconstructMerkleRoot := ReconstructRangeProof(rangeProof, fanout, resultsHashes, util.NewHasher(util.BLAKE3))
	return true, reconstructMerkleRoot
}

