package run

import (
	"VLStore/util"
	"fmt"
	"math"
	"sort"
	//"time"
)

/* Iterator of key-value vector in memory
 */
 type InMemKeyValueIterator struct {
	KeyValues   []util.KeyValue // the key-values in memory
	CurValuePos int             // the position of the current value
}

func NewInMemKeyValueIterator(keyValues []util.KeyValue) *InMemKeyValueIterator {
	return &InMemKeyValueIterator{
		KeyValues:   keyValues,
		CurValuePos: 0,
	}
}

func (it *InMemKeyValueIterator) HasNext() bool {
	return it.CurValuePos < len(it.KeyValues)
}

func (it *InMemKeyValueIterator) Next() util.KeyValue {
	keyValue := it.KeyValues[it.CurValuePos]
	it.CurValuePos++
	return keyValue
}

// LevelRun 定义了一个层级中的运行
type LevelRun struct {
	ComponentID int                // 组件ID
	KeyReader *KeyPageReader       // 键读取器
	ValueLog *ValueLog   		   // 值读取器
	ModelReader *ModelPageReader   // 模型读取器
	MHTReader   *HashPageReader    // MHT读取器
	Digest      util.H256          // MHT根摘要
}

// Load 根据组件ID、层级ID和配置加载一个运行
func Load(componentID int, levelID int, configs *util.Configs) (*LevelRun, error) {
	// 定义值、模型、MHT的文件名
	keyFileName := FileName(componentID, levelID, configs.DirName, "k")
	valueFileName := FileName(componentID, levelID, configs.DirName, "v")
	modelFileName := FileName(componentID, levelID, configs.DirName, "m")
	mhtFileName := FileName(componentID, levelID, configs.DirName, "h")

	// 加载三个读取器
	keyReader, err := LoadKeyPageReader(keyFileName)
	if err != nil {
		return nil, err
	}

	valueLog, err := OpenValueFile(valueFileName)
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

	// 获取MHT根哈希
	mhtRoot := mhtReader.Root

	// 计算摘要
	digest := mhtRoot
	return &LevelRun{
		ComponentID: componentID,
		KeyReader: keyReader,
		ValueLog: valueLog,
		ModelReader: modelReader,
		MHTReader:   mhtReader,
		Digest:      digest,
	}, nil
}

// ConstructRunByInMemoryTree 使用内存迭代器构建运行
func ConstructRunByInMemoryTree(inputs *InMemKeyValueIterator, componentID int, levelID int, dirName string, epsilon int, fanout int, maxNumOfValue int, levelNumOfRun int, sizeRatio int) (*LevelRun, error) {
	keyFileName := FileName(componentID, levelID, dirName, "k")
	valueFileName := FileName(componentID, levelID, dirName, "v")
	modelFileName := FileName(componentID, levelID, dirName, "m")
	mhtFileName := FileName(componentID, levelID, dirName, "h")

	// 根据输入值构建新的值文件、模型文件、MHT文件
	keyWriter, err := NewKeyPageWriter(keyFileName)
	if err != nil {
		return nil, err
	}

	valueLog, err := OpenValueFile(valueFileName)
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
		// write the KV's value to the value log
		pos, err := valueLog.Write(keyValue.Value)
		if err != nil {
			panic(err)
		}

		//fmt.Println("key: ", keyValue.Key, "pos: blockNumber: ", pos.BlockNumber, "chunkOffset: ", pos.ChunkOffset)

		// add the KV's key and value position to the key writer
		keyWriter.Append(KeyPos{
			Key: keyValue.Key,
			Pos: pos,
		})
	}
	// flush the value writer
	valueLog.Sync()
	// flush the key writer
	keyWriter.Flush()
	// finalize the model constructor
	modelConstructor.FinalizeAppend()
	// build the mht
	mhtConstructor.BuildMHT()

	// 创建读取器
	keyReader := keyWriter.ToKeyPageReader()
	modelReader := modelConstructor.OutputModelWriter.ToModelReader()
	mhtReader := mhtConstructor.OutputMHTWriter.ToHashReader()

	// 获取MHT根哈希
	mhtRoot := mhtReader.Root

	// 计算摘要
	digest := mhtRoot

	return &LevelRun{
		ComponentID: componentID,
		KeyReader: keyReader,
		ValueLog: valueLog,
		ModelReader: modelReader,
		MHTReader:   mhtReader,
		Digest:      digest,
	}, nil
}

// SearchRun 在运行中搜索键
func (lr *LevelRun) SearchRun(searchKey util.Key, configs *util.Configs, cacheManager *CacheManager) *util.KeyValue {
	// 使用模型文件预测值文件中的位置
	// 计算边界键
	epsilon := configs.Epsilon

	// 使用模型文件预测位置
	predPos, err := lr.ModelReader.GetPredKeyPos(lr.ComponentID, searchKey, epsilon, cacheManager)
	if err != nil {
		panic(err)
	}

	numOfKeys := lr.KeyReader.NumKeyPos

	// 根据预测位置和epsilon计算下界和上界位置
	posL := int(math.Min(math.Max(float64(predPos-epsilon-1), 0), float64(numOfKeys-1)))
	posR := int(math.Min(float64(predPos+epsilon+2), float64(numOfKeys-1)))

	// 从给定范围[posL, posR]加载键
	keyPoss, err := lr.KeyReader.ReadPageRange(lr.ComponentID, posL, posR, cacheManager)
	if err != nil {
		panic(err)
	}

	// 在键中查找目标键
	targetKeyPos := KeyPos{}
	for _, keyPos := range keyPoss {
		if keyPos.Key == searchKey {
			targetKeyPos = keyPos
			break
		}
	}

	// 从值文件中读取目标键的值
	value, err := lr.ValueLog.Read(targetKeyPos.Pos.BlockNumber, targetKeyPos.Pos.ChunkOffset)
	if err != nil {
		panic(err)
	}

	return &util.KeyValue{Key: targetKeyPos.Key, Value: value}
}

// FileName 生成不同文件类型的文件名
func FileName(runID int, levelID int, dirName string, fileType string) string {
	return fmt.Sprintf("%s/%s_%d_%d.dat", dirName, fileType, levelID, runID)
}

type RunProof struct {
	rangeProof *RangeProof
}

func NewRunProof() *RunProof {
	return &RunProof{
		rangeProof: nil,
	}
}

func (lr *LevelRun) ProveLeaf(l, r, numOfData, fanout int, proof *RangeProof, cacheManager *CacheManager) {
	levelL := l
	levelR := r
	proofPosL := levelL - levelL%fanout
	proofPosR := levelR - levelR%fanout + fanout
	if proofPosR > numOfData {
		proofPosR = numOfData
	}
	proofPosR--

	// Read keys from key reader
	keyPoss, err := lr.KeyReader.ReadPageRange(lr.ComponentID, proofPosL, proofPosR, cacheManager)
	if err != nil {
		panic(err)
	}
	// // Read values from value reader
	// keyValues := make([]util.KeyValue, 0, len(keyPoss))
	// for _, keyPos := range keyPoss {
	// 	value, err := lr.ValueLog.Read(keyPos.Pos.BlockNumber, keyPos.Pos.ChunkOffset)
	// 	if err != nil {
	// 		fmt.Println("keyPos: ", keyPos)
	// 		panic(err)
	// 	}
	// 	keyValues = append(keyValues, util.KeyValue{Key: keyPos.Key, Value: value})
	// }

	// // Create leaf hashes
	// //createLeafHashesStart := time.Now()
	// leafHashes := make([]util.H256, 0, len(keyValues))
	// for _, kv := range keyValues {
	// 	leafHashes = append(leafHashes, kv.ComputeHash(util.NewHasher(util.BLAKE3)))
	// }
	
	// // Remove the hashes that are in the proven range
	// for i := 0; i < (levelR - levelL + 1); i++ {
	// 	leafHashes = append(leafHashes[:levelL-proofPosL], leafHashes[levelL-proofPosL+1:]...)
	// }

	// 优化1: 只为需要的哈希预分配空间
	rangeStart := levelL - proofPosL
	rangeEnd := levelR - proofPosL
	totalHashes := len(keyPoss)
	excludedCount := rangeEnd - rangeStart + 1
	leafHashes := make([]util.H256, 0, totalHashes-excludedCount)
	
	hasher := util.NewHasher(util.BLAKE3)
	
	// 优化2: 边读边计算哈希，跳过不需要的范围
	for i, keyPos := range keyPoss {
		// 跳过证明范围内的元素
		if i >= rangeStart && i <= rangeEnd {
			continue
		}
		
		value, err := lr.ValueLog.Read(keyPos.Pos.BlockNumber, keyPos.Pos.ChunkOffset)
		if err != nil {
			fmt.Println("keyPos: ", keyPos)
			panic(err)
		}
		
		kv := util.KeyValue{Key: keyPos.Key, Value: value}
		leafHashes = append(leafHashes, kv.ComputeHash(hasher))
	}

	//fmt.Println("createLeafHashesTime: ", time.Since(createLeafHashesStart))
	// Insert at the beginning of the proof path
	proof.p = append([][]util.H256{leafHashes}, proof.p...)
}

/*
Generate the result and the RunProof
If the filter does show that the addr_key does not exist, use the filter + MHT root as the proof.
If the filter cannot show, use the MHT to prove the result, add the filter's hash to the proof.
*/

func (lr *LevelRun) ProveRange(startKey, endKey util.Key, configs *util.Configs, cacheManager *CacheManager) ([]util.KeyValue, *RunProof) {
	// init the proof
	proof := NewRunProof()

	epsilon := configs.Epsilon

	// Use model file to predict positions
	//learnIndexStart := time.Now()
	predPosLow, err := lr.ModelReader.GetPredKeyPos(lr.ComponentID, startKey, epsilon, cacheManager)
	if err != nil {
		panic(err)
	}
	predPosUpper, err := lr.ModelReader.GetPredKeyPos(lr.ComponentID, endKey, epsilon, cacheManager)
	if err != nil {
		panic(err)
	}
	
	//fmt.Println("learnIndexTime: ", time.Since(learnIndexStart))
	
	numOfKeys := lr.KeyReader.NumKeyPos
	// Compute position boundaries based on predictions and epsilon
	posL := int(math.Min(math.Max(float64(predPosLow-epsilon-1), 0), float64(numOfKeys-1)))
	posR := int(math.Min(float64(predPosUpper+epsilon+3), float64(numOfKeys-1)))

	// 打印posL 和 posR
	//fmt.Println("posL: ", posL)
	//fmt.Println("posR: ", posR)

	// Load keys from the range
	//loadKeyStart := time.Now()
	keyPoss, err := lr.KeyReader.ReadPageRange(lr.ComponentID, posL, posR, cacheManager)
	if err != nil {
		panic(err)
	}
	if len(keyPoss) == 0 {
		return nil, proof
	}
	// Binary search to find exact positions in the retrieved data
	lowerIndex := sort.Search(len(keyPoss), func(i int) bool {
		return keyPoss[i].Key >= startKey
	})
	upperIndex := sort.Search(len(keyPoss), func(i int) bool {
		return keyPoss[i].Key >= endKey
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
	// if leftProofPos > 0 && leftProofPos < numOfKeys {
	// 	if keyPoss[lowerIndex].Key == startKey {
	// 		leftProofPos--
	// 	}
	// }

	// if rightProofPos < numOfKeys - 1 {
	// 	if keyPoss[upperIndex].Key == endKey {
	// 		rightProofPos++
	// 	}
	// }

	// if rightProofPos == numOfKeys {
	// 	rightProofPos = numOfKeys - 1
	// }
	// if leftProofPos == numOfKeys{
	// 	leftProofPos = numOfKeys - 1
	// }
	if leftProofPos > 0 && rightProofPos < numOfKeys-1{
		if keyPoss[lowerIndex].Key == startKey {
			leftProofPos--
		}
		if keyPoss[upperIndex].Key == endKey {
			rightProofPos++
		}
	}
	if rightProofPos == numOfKeys {
		rightProofPos = numOfKeys - 1
	}
	
	// 打印leftProofPos 和 rightProofPos
	//fmt.Println("leftProofPos: ", leftProofPos)
	//fmt.Println("rightProofPos: ", rightProofPos)
	// Get the result key-value pairs
	resultKeyPoss := keyPoss[leftProofPos-posL : rightProofPos-posL+1]
	//fmt.Println("loadKeyTime: ", time.Since(loadKeyStart))

	//readValueStart := time.Now()
	resultData := make([]util.KeyValue, 0, len(resultKeyPoss))
	for _, keyPos := range resultKeyPoss {
		value, err := lr.ValueLog.Read(keyPos.Pos.BlockNumber, keyPos.Pos.ChunkOffset)
		if err != nil {
			fmt.Println("key: ", keyPos.Key, "pos: blockNumber: ", keyPos.Pos.BlockNumber, "chunkOffset: ", keyPos.Pos.ChunkOffset)
			panic(err)
		}
		resultData = append(resultData, util.KeyValue{Key: keyPos.Key, Value: value})
	}
	//fmt.Println("readValueTime: ", time.Since(readValueStart))
	// 打印resultKeyPoss 中的key
	//fmt.Println("resultKeyPoss :")
	//for _, kv := range resultKeyPoss {
	//	fmt.Print(kv.Key, " ")
	//}
	//fmt.Println()

	// Generate non-leaf range proof
	//proveNonLeafStart := time.Now()
	fanout := configs.Fanout
	rangeProof := lr.MHTReader.proveNonLeaf(lr.ComponentID, leftProofPos, rightProofPos, numOfKeys, fanout, cacheManager)
	//fmt.Println("proveNonLeafTime: ", time.Since(proveNonLeafStart))	
	// Generate leaf range proof
	//proveLeafStart := time.Now()
	lr.ProveLeaf(leftProofPos, rightProofPos, numOfKeys, fanout, rangeProof, cacheManager)
	//fmt.Println("proveLeafTime: ", time.Since(proveLeafStart))
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

