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
	RunID       int                // 运行ID
	ValueReader *ValuePageReader   // 值读取器
	ModelReader *ModelPageReader   // 模型读取器
	MHTReader   *HashPageReader    // MHT读取器
	Filter      *bloom.BloomFilter // 布隆过滤器
	FilterHash  *util.H256         // 过滤器的哈希值
	Digest      util.H256          // MHT根和过滤器哈希的摘要
}

// Load 根据运行ID、层级ID和配置加载一个运行
func Load(runID int, levelID int, configs *util.Configs) (*LevelRun, error) {
	// 定义值、模型、MHT和过滤器的文件名
	valueFileName := FileName(runID, levelID, configs.DirName, "v")
	modelFileName := FileName(runID, levelID, configs.DirName, "m")
	mhtFileName := FileName(runID, levelID, configs.DirName, "h")
	filterFileName := FileName(runID, levelID, configs.DirName, "f")

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

	// 计算过滤器哈希（如果存在）
	var filterHash *util.H256 = nil
	if filter != nil {
		filterData, err := filter.MarshalBinary()
		if err != nil {
			return nil, err
		}
		hash := util.NewHasher(util.BLAKE3).HashBytes(filterData)
		filterHash = &hash
	}

	// 计算摘要
	//digest := LoadDigest(mhtRoot, filterHash)
	digest := mhtRoot
	return &LevelRun{
		RunID:       runID,
		ValueReader: valueReader,
		ModelReader: modelReader,
		MHTReader:   mhtReader,
		Filter:      filter,
		FilterHash:  filterHash,
		Digest:      digest,
	}, nil
}

// ConstructRunByInMemoryTree 使用内存迭代器构建运行
func ConstructRunByInMemoryTree(inputs *InMemKeyValueIterator, runID int, levelID int, dirName string, epsilon int, fanout int, maxNumOfValue int, levelNumOfRun int, sizeRatio int) (*LevelRun, error) {
	valueFileName := FileName(runID, levelID, dirName, "v")
	modelFileName := FileName(runID, levelID, dirName, "m")
	mhtFileName := FileName(runID, levelID, dirName, "h")

	// 估算过滤器大小，决定是否创建过滤器
	estFilterSize := EstimateAllFilterSize(levelID, maxNumOfValue, levelNumOfRun, sizeRatio)
	var filter *bloom.BloomFilter = nil
	if estFilterSize <= MAX_FILTER_SIZE {
		// 创建布隆过滤器，使用预估的元素数量和指定的误报率
		filter = bloom.NewWithEstimates(uint(maxNumOfValue), FILTER_FP_RATE)
	}

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
		// insert the KV's key to the filter
		if filter != nil {
			keyBytes := make([]byte, 8)
			binary.LittleEndian.PutUint64(keyBytes, uint64(keyValue.Key))
			filter.Add(keyBytes)
		}
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

	//计算过滤器哈希（如果存在）
	var filterHash *util.H256 = nil
	if filter != nil {
		filterData, err := filter.MarshalBinary()
		if err != nil {
			return nil, err
		}
		hash := util.NewHasher(util.BLAKE3).HashBytes(filterData)
		filterHash = &hash
	}

	// 计算摘要(暂时不计算过滤器哈希)
	digest := mhtRoot

	return &LevelRun{
		RunID:       runID,
		ValueReader: valueReader,
		ModelReader: modelReader,
		MHTReader:   mhtReader,
		Filter:      filter,
		FilterHash:  filterHash,
		Digest:      digest,
	}, nil
}

// SearchRun 在运行中搜索键
func (lr *LevelRun) SearchRun(key util.Key, configs *util.Configs) *util.KeyValue {
	// 尝试使用过滤器测试键是否存在
	if lr.Filter != nil {
		// 将键转换为字节数组
		keyBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(keyBytes, uint64(key))

		// 如果过滤器不包含键，则返回nil
		if !lr.Filter.Test(keyBytes) {
			return nil
		}
	}

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

// PersistFilter 如果过滤器存在，则持久化它
func (lr *LevelRun) PersistFilter(levelID int, configs *util.Configs) error {
	if lr.Filter != nil {
		// 初始化过滤器文件名
		filterFileName := FileName(lr.RunID, levelID, configs.DirName, "f")

		// 序列化过滤器
		filterData, err := lr.Filter.MarshalBinary()
		if err != nil {
			return err
		}

		// 获取序列化字节的长度
		bytesLen := uint32(len(filterData))

		// 创建要持久化到过滤器文件的向量
		data := make([]byte, 4+len(filterData))
		binary.BigEndian.PutUint32(data[:4], bytesLen)
		copy(data[4:], filterData)

		// 写入文件
		file, err := os.OpenFile(filterFileName, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0666)
		if err != nil {
			return err
		}
		defer file.Close()

		_, err = file.Write(data)
		if err != nil {
			return err
		}
	}
	return nil
}

// EstimateAllFilterSize 估算所有过滤器的大小
func EstimateAllFilterSize(levelID int, maxNumOfValue int, levelNumOfRun int, sizeRatio int) int {
	totalSize := 0
	curLevel := levelID
	curNumOfValue := maxNumOfValue

	// 计算当前级别的过滤器大小
	curLevelFilterSize := util.ComputeBitmapSizeInBytes(curNumOfValue, FILTER_FP_RATE) * levelNumOfRun
	totalSize += curLevelFilterSize

	// 计算所有低级别的过滤器大小
	for curLevel >= 0 {
		curLevel--
		curNumOfValue /= sizeRatio
		curLevelFilterSize = util.ComputeBitmapSizeInBytes(curNumOfValue, FILTER_FP_RATE) * sizeRatio
		totalSize += curLevelFilterSize
	}

	return totalSize
}

// ComputeDigest 计算LevelRun的摘要
// func (lr *LevelRun) ComputeDigest() util.H256 {
// 	mhtRoot := lr.MHTReader.Root
// 	var bytes []byte
// 	bytes = append(bytes, mhtRoot[:]...)

// 	if lr.FilterHash != nil {
// 		bytes = append(bytes, (*lr.FilterHash)[:]...)
// 	}

// 	return util.NewHasher(util.BLAKE3).HashBytes(bytes)
// }

// FileName 生成不同文件类型的文件名
func FileName(runID int, levelID int, dirName string, fileType string) string {
	return fmt.Sprintf("%s/%s_%d_%d.dat", dirName, fileType, levelID, runID)
}

// LoadDigest 根据MHT根和过滤器（如果存在）计算运行的摘要
// func LoadDigest(mhtRoot util.H256, filterHash *util.H256) util.H256 {
// 	var bytes []byte
// 	bytes = append(bytes, mhtRoot[:]...)

// 	if filterHash != nil {
// 		bytes = append(bytes, (*filterHash)[:]...)
// 	}

// 	return util.NewHasher(util.BLAKE3).HashBytes(bytes)
// }

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

// FilterCost 返回过滤器的大小信息
func (lr *LevelRun) FilterCost() int {
	var filterSize int = 0
	if lr.Filter != nil {
		// 获取过滤器的大小
		// 注意：bloom库可能没有直接提供内存大小的方法，这里使用近似计算
		filterData, _ := lr.Filter.MarshalBinary()
		filterSize = len(filterData)
	}

	return filterSize
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

func (lr *LevelRun) proveRange(startKey, endKey util.Key, configs *util.Configs) ([]util.KeyValue, *RunProof) {
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

	// Load key-values from the range
	keyValues := lr.ValueReader.ReadKeyValuesRange(posL, posR)

	// Binary search to find exact positions in the retrieved data
	lowerIndex := sort.Search(len(keyValues), func(i int) bool {
		return keyValues[i].Key >= startKey
	})
	upperIndex := sort.Search(len(keyValues), func(i int) bool {
		return keyValues[i].Key >= endKey
	})

	var leftProofPos int = -1
	var rightProofPos int = -1

    if lowerIndex == len(keyValues) || upperIndex == len(keyValues) {
		if lowerIndex == len(keyValues) {
			leftProofPos = numOfValues-1
		}
		if upperIndex == len(keyValues) {
			rightProofPos = numOfValues-1
		}
	}else{
		// Derive the actual position by adding offset pos_l
		leftProofPos = lowerIndex + posL
		rightProofPos = upperIndex + posL

		// Adjust boundary positions
		if leftProofPos != 0 && keyValues[lowerIndex].Key == startKey {
			leftProofPos--
		}
		if rightProofPos != numOfValues-1 {
			rightProofPos++
		}
	}


	// Get the result key-value pairs
	resultData := keyValues[leftProofPos-posL : rightProofPos-posL+1]

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
