package vlstore

import (
	"VLStore/memtable"
	"VLStore/run"
	"VLStore/util"
	"encoding/binary"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	//"time"
)
type ComponentMeta struct {
    MinKey util.Key
    MaxKey util.Key
    Digest util.H256
}

func newComponentMeta(minKey util.Key, maxKey util.Key, digest util.H256) *ComponentMeta {
	return &ComponentMeta{
		MinKey: minKey,
		MaxKey: maxKey,
		Digest: digest,
	}
}

/*
VLStore consists of:

	(i) a reference of configs that include params
	(ii) an in-memory MB-Tree as the authenticated index
	(iii) a vector of levels that stores each level's LevelRuns
*/
type VLStore struct {
	configs                    *util.Configs
	memTable                   *memtable.MBTree
	mu                         sync.RWMutex
	immutableMemTableVec       []*memtable.MBTree    	// immutable memtables, waiting to be flushed to disk
	immutableMemTableVecLock   sync.RWMutex
	flushImmutableMemTableChan chan *memtable.MBTree 	// channel to notify the flush goroutine to flush immutable memtable to disk
	levelVec                   []*run.Level
	componentIDCnt             atomic.Int64 			// this helps to generate a component_id
	componentMetaMap           map[int]*ComponentMeta
	cacheManager               *run.CacheManager
}

// create a new index with given configs
func NewVLStore(configs *util.Configs) *VLStore {
	vl := &VLStore{
		configs:                    configs,
		immutableMemTableVec:          []*memtable.MBTree{},
		flushImmutableMemTableChan: make(chan *memtable.MBTree),
		levelVec:                   []*run.Level{},
		componentIDCnt:             atomic.Int64{},
		componentMetaMap:           make(map[int]*ComponentMeta),
		cacheManager:               run.NewCacheManager(),
	}
	vl.memTable = memtable.NewBPlusTree(vl.newComponentID(), configs.Fanout)
	vl.componentMetaMap[vl.memTable.GetComponentID()] = newComponentMeta(-1, -1, util.H256{})

	go vl.flushMemTableWorker()

	return vl
}

func (vl *VLStore) getMeta() int {
	path := fmt.Sprintf("%s/mht", vl.configs.DirName)
	file, err := os.ReadFile(path)
	if err != nil {
		panic(err)
	}

	// read level len from the file
	levelLenBytes := make([]byte, 8)
	copy(levelLenBytes, file[:8])
	levelLen := int(binary.BigEndian.Uint64(levelLenBytes))

	// read component_id_cnt（atomic.Int64{},） from the file
	componentIDCntBytes := make([]byte, 8)
	copy(componentIDCntBytes, file[8:16])
	componentIDCnt := int64(binary.BigEndian.Uint64(componentIDCntBytes))
	vl.componentIDCnt.Store(componentIDCnt)

	// read mem mht from the file
	// read component_meta_map from the file
	// TODO: implement this
	return levelLen
}

// load a new index using configs
func Load(configs *util.Configs) *VLStore {
	vl := NewVLStore(configs)
	levelLen := vl.getMeta()
	// load levels
	for i := 0; i < levelLen; i++ {
		level, err := run.LoadLevel(i, vl.configs)
		if err != nil {
			panic(err)
		}
		vl.levelVec = append(vl.levelVec, level)
	}

	return vl
}

func (vl *VLStore) newComponentID() int {
	// increment the component_id and return it
	componentID := vl.componentIDCnt.Add(1)
	return int(componentID)
}

func (vl *VLStore) Insert(key util.Key, value util.Value) {
	vl.mu.Lock()
	defer vl.mu.Unlock()

	// directly insert the state into the mem_mht
	vl.memTable.Insert(key, value)


	// update the component_meta_map
	componentMeta := vl.componentMetaMap[vl.memTable.GetComponentID()]

	if componentMeta.MaxKey == -1 || componentMeta.MaxKey < key {
		componentMeta.MaxKey = key
	}
	if componentMeta.MinKey == -1 || componentMeta.MinKey > key {
		componentMeta.MinKey = key
	}

	componentMeta.Digest = vl.memTable.GetHash()
	vl.componentMetaMap[vl.memTable.GetComponentID()] = componentMeta
	

	// check if the memtable is full and need to be flushed to disk
	inMemThreshold := vl.configs.BaseStateNum
	if vl.memTable.KeyNum() >= inMemThreshold {
		// the in-memory mb-tree is full
		// swap the memtable to the immutable memtable and create a new memtable
		immutableMemTable := vl.memTable

		vl.immutableMemTableVecLock.Lock()
		vl.immutableMemTableVec = append(vl.immutableMemTableVec, immutableMemTable)
		vl.immutableMemTableVecLock.Unlock()

		vl.memTable = memtable.NewBPlusTree(vl.newComponentID(), vl.configs.Fanout)
		vl.componentMetaMap[vl.memTable.GetComponentID()] = newComponentMeta(-1, -1, util.H256{})
		// flush the immutable memtable to disk
		vl.flushImmutableMemTableChan <- immutableMemTable
	}
}

// search a key in the VLStore
func (vl *VLStore) Search(key util.Key) util.Value {
	vl.mu.RLock()
	defer vl.mu.RUnlock()

	// search in the memtable
	memTableID := vl.memTable.GetComponentID()
	memTableMeta := vl.componentMetaMap[memTableID]
	if key >= memTableMeta.MinKey && key <= memTableMeta.MaxKey {
		value, isExist := vl.memTable.Search(key)
		if isExist {
			return value
		}
	}

	// search in the immutable memtable vector
	for _, mt := range vl.immutableMemTableVec {
		immutableMemTableID := mt.GetComponentID()
		immutableMemTableMeta := vl.componentMetaMap[immutableMemTableID]
		if key >= immutableMemTableMeta.MinKey && key <= immutableMemTableMeta.MaxKey {
			value, isExist := mt.Search(key)
			if isExist {
				return value
			}
		}
	}

	// search other levels on the disk
	for _, level := range vl.levelVec {
		for _, run := range level.RunVec {
			runID := run.ComponentID
			runMeta := vl.componentMetaMap[runID]
			if key >= runMeta.MinKey && key <= runMeta.MaxKey {
				keyValue := run.SearchRun(key, vl.configs, vl.cacheManager)
				if keyValue != nil {
					return keyValue.Value
				}
			}
		}
	}

	return nil
}



// 返回和查询范围有重叠的组件的component_id
func (vl *VLStore) SearchRangeFromComponentMeta(startKey util.Key, endKey util.Key) map[int]bool{
	componentIDSet := make(map[int]bool)

	// 遍历所有组件的元数据
	for componentID, meta := range vl.componentMetaMap {
		//fmt.Println("componentID :", componentID, " meta :", meta)
		// 检查组件的键范围是否与查询范围有重叠
		if !(meta.MaxKey < startKey || meta.MinKey > endKey) {
			componentIDSet[componentID] = true
		}else{
			componentIDSet[componentID] = false
		}
	}

	// // 打印componentIDSet
	// fmt.Println("componentIDSet :")
	// for componentID := range componentIDSet {
	// 	fmt.Print(componentID, " : ", componentIDSet[componentID], ", ")
	// }
	// fmt.Println()
	
	return componentIDSet
}


type MemTableProofOrHash struct {
	results []util.KeyValue
	proof memtable.RangeProof
	hash util.H256
	isHash bool
}

type LevelRunProofOrHash struct {
	results []util.KeyValue
	proof run.RunProof
	hash util.H256
	isHash bool
}

type LevelProof struct {
	runProofOrHashVec []*LevelRunProofOrHash
}

type VLStoreProof struct {
	memTableProofVec []*MemTableProofOrHash	
	levelProofVec []*LevelProof
}

func (vl *VLStore) SearchWithProof(startKey util.Key, endKey util.Key) *VLStoreProof {
	vl.mu.RLock()
	defer vl.mu.RUnlock()

	// get the component_id vector that has overlap with the query range
	componentIDSet := vl.SearchRangeFromComponentMeta(startKey, endKey)

	vlStoreProof := &VLStoreProof{
		memTableProofVec: []*MemTableProofOrHash{},
		levelProofVec: []*LevelProof{},
	}

	// search in the memtable
	//memTableStart := time.Now()
	memTableID := vl.memTable.GetComponentID()
	var memTableResults []util.KeyValue
	var memTableProofOrHash MemTableProofOrHash
	if componentIDSet[memTableID] {
		var memTableProof memtable.RangeProof
		memTableResults, memTableProof = vl.memTable.GenerateRangeProof(startKey, endKey)
		memTableProofOrHash = MemTableProofOrHash{
			results: memTableResults,
			proof: memTableProof,
			hash: util.H256{},
			isHash: false,
		}
	}else{
		memTableProofOrHash = MemTableProofOrHash{
			results: memTableResults,
			proof: memtable.RangeProof{},
			hash: vl.memTable.GetHash(),
			isHash: true,
		}
	}
	vlStoreProof.memTableProofVec = append(vlStoreProof.memTableProofVec, &memTableProofOrHash)
	//fmt.Printf("MemTable search time: %v, found %d results\n", 
    //    time.Since(memTableStart), len(memTableResults))

	// search in the immutable memtable vector
	for _, mt := range vl.immutableMemTableVec {
		//immutableMemTableStart := time.Now()
		immutableMemTableID := mt.GetComponentID()
		var immutableMemTableResults []util.KeyValue
		var immutableMemTableProofOrHash MemTableProofOrHash
		if componentIDSet[immutableMemTableID] {
			immutableMemTableResults, immutableMemTableProof := mt.GenerateRangeProof(startKey, endKey)
			immutableMemTableProofOrHash = MemTableProofOrHash{
				results: immutableMemTableResults,
				proof: immutableMemTableProof,
				hash: util.H256{},
				isHash: false,
			}
		}else{
			immutableMemTableProofOrHash = MemTableProofOrHash{
				results: immutableMemTableResults,
				proof: memtable.RangeProof{},
				hash: mt.GetHash(),
				isHash: true,
			}
		}
		vlStoreProof.memTableProofVec = append(vlStoreProof.memTableProofVec, &immutableMemTableProofOrHash)
		//fmt.Printf("ImmutableMemTable search time: %v, found %d results\n", 
        //time.Since(immutableMemTableStart), len(immutableMemTableResults))
	}

	// search in the disk-level
	for _, level := range vl.levelVec {
		levelProof := &LevelProof{
			runProofOrHashVec: []*LevelRunProofOrHash{},
		}
		for _, levelRun := range level.RunVec {
			//levelRunStart := time.Now()
			levelRunID := levelRun.ComponentID
			//fmt.Println("levelRunID :", levelRunID)
			var levelRunResults []util.KeyValue
			var levelRunProofOrHash LevelRunProofOrHash
			if componentIDSet[levelRunID] {
				var levelRunProof *run.RunProof
				levelRunResults, levelRunProof = levelRun.ProveRange(startKey, endKey, vl.configs, vl.cacheManager)
				levelRunProofOrHash = LevelRunProofOrHash{
					results: levelRunResults,
					proof: *levelRunProof,
					hash: util.H256{},
					isHash: false,
				}
			}else{
				levelRunProofOrHash = LevelRunProofOrHash{
					results: levelRunResults,
					proof: run.RunProof{},
					hash: levelRun.Digest,
					isHash: true,
				}
			}	
			levelProof.runProofOrHashVec = append(levelProof.runProofOrHashVec, &levelRunProofOrHash)
			//fmt.Printf("LevelRun search time: %v, found %d results\n", 
        	//	time.Since(levelRunStart), len(levelRunResults))
		}
		vlStoreProof.levelProofVec = append(vlStoreProof.levelProofVec, levelProof)
	}
	return vlStoreProof
}

func (vl *VLStore) ComputeDigest() util.H256 {
	HashVec := make([]util.H256, 0)

	// compute the digest of the memtable
	memTableHash := vl.memTable.GetHash()
	HashVec = append(HashVec, memTableHash)
	// compute the digest of the immutable memtable vector
	for _, mt := range vl.immutableMemTableVec {
		immutableMemTableHash := mt.GetHash()
		HashVec = append(HashVec, immutableMemTableHash)
	}

	// collect each disk-level's digest
	for _, level := range vl.levelVec {
		levelHash := level.ComputeDigest()
		HashVec = append(HashVec, levelHash)
	}
	
	bytes := make([]byte, 0)
	for _, hash := range HashVec {
		bytes = append(bytes, hash[:]...)
	}
	return util.NewBlake3Hasher().HashBytes(bytes)
}

func (vl *VLStore) VerifyAndCollectResult(startKey util.Key, endKey util.Key, proof *VLStoreProof, rootHash util.H256, fanout int) (bool, []util.KeyValue) {
	componentDigestVec := make([]util.H256, 0)

	// first reconstruct the memtable_proof
	memTableProofOrHash := proof.memTableProofVec[0]
	memTableResult := memTableProofOrHash.results

	// // 打印memTableResult 中的key
	// fmt.Println("memTable :")
	// for _, kv := range memTableResult {
	// 	fmt.Print(kv.Key, " ")
	// }
	// fmt.Println()

	var memTableDigest util.H256
	if memTableProofOrHash.isHash {
		memTableDigest = memTableProofOrHash.hash
	}else{
		memTableDigest = memtable.ReconstructRangeProof(startKey, endKey, memTableResult, memTableProofOrHash.proof)
	}
	componentDigestVec = append(componentDigestVec, memTableDigest)


	mergeResult := make([]util.KeyValue, 0)
	mergeResult = append(mergeResult, memTableResult...)

	// reconstruct the immutable memtable_proof
	for i := 1; i < len(proof.memTableProofVec); i++ {
		immutableMemTableProofOrHash := proof.memTableProofVec[i]
		immutableMemTableResult := immutableMemTableProofOrHash.results

		// // 打印memTableResult 中的key
		// fmt.Println("immutable memTable :", i)
		// for _, kv := range immutableMemTableResult {
		// 	fmt.Print(kv.Key, " ")
		// }
		// fmt.Println()

		var immutableMemTableDigest util.H256
		if immutableMemTableProofOrHash.isHash {
			immutableMemTableDigest = immutableMemTableProofOrHash.hash
		}else{
			immutableMemTableDigest = memtable.ReconstructRangeProof(startKey, endKey, immutableMemTableResult, immutableMemTableProofOrHash.proof)
		}
		componentDigestVec = append(componentDigestVec, immutableMemTableDigest)
		mergeResult = append(mergeResult, immutableMemTableResult...)
	}

	// reconstruct the level_proof
	for _, levelProof := range proof.levelProofVec {
		runDigestVec := make([]util.H256, 0)
		for _, levelRunProofOrHash := range levelProof.runProofOrHashVec {
			levelRunResults := levelRunProofOrHash.results
			// // 打印levelRunResults 中的key
			// fmt.Println("levelRun :")
			// fmt.Println(levelRunProofOrHash.isHash)
			// for _, kv := range levelRunResults {
			// 	fmt.Print(kv.Key, " ")
			// }
			// fmt.Println()

			var levelRunDigest util.H256
			if levelRunProofOrHash.isHash {
				levelRunDigest = levelRunProofOrHash.hash
			}else{
				_, levelRunDigest = run.ReconstructRunProof(startKey, endKey, levelRunResults, &levelRunProofOrHash.proof, fanout)
				//fmt.Println("levelRunDigest :", levelRunDigest)
			}
			runDigestVec = append(runDigestVec, levelRunDigest)
			mergeResult = append(mergeResult, levelRunResults...)
		}
		levelDigest := util.NewBlake3Hasher().ComputeConcatHash(runDigestVec)
		componentDigestVec = append(componentDigestVec, levelDigest)
	}

	reconstructRootHash := util.NewBlake3Hasher().ComputeConcatHash(componentDigestVec)
	if reconstructRootHash != rootHash {
		fmt.Println("reconstructRootHash :", reconstructRootHash)
		fmt.Println("rootHash :", rootHash)
		return false, nil
	}
	// sort the mergeResult by the key
	sort.Slice(mergeResult, func(i, j int) bool {
		return mergeResult[i].Key < mergeResult[j].Key
	})

	// delete the elements in mergeResult whose key is not in [startKey, endKey]
	for i := 0; i < len(mergeResult); i++ {
		if mergeResult[i].Key < startKey || mergeResult[i].Key > endKey {
			mergeResult = append(mergeResult[:i], mergeResult[i+1:]...)
			i--
		}
	}
	

	return true, mergeResult
}





// flush the specified memtable to disk
// Following steps are included:
// 1. Load all the key-values in the memtable
// 2. Construct a new run by the key-values
// 3. Insert the run into the disk-level
// 4. If the level reaches the capacity, merge all the runs in the level to the next level
func (vl *VLStore) flushMemTable(table *memtable.MBTree) {
	// Load all the key-values in the memtable
	keyValues := table.LoadAllKeyValues()
	iter := run.NewInMemKeyValueIterator(keyValues)

	// Construct a new run by the key-values
	componentID := table.GetComponentID()
	levelID := 0 // the first on-disk level id is 0
	levelNumOfRuns := 0
	if levelID < len(vl.levelVec) && vl.levelVec[levelID] != nil {
		levelNumOfRuns = len(vl.levelVec[levelID].RunVec)
	}
	levelRun, err := run.ConstructRunByInMemoryTree(iter, componentID, levelID, vl.configs.DirName, vl.configs.Epsilon, vl.configs.Fanout, vl.configs.MaxNumOfStatesInARun(levelID), levelNumOfRuns, vl.configs.SizeRatio)
	if err != nil {
		panic(err)
	}

	// update the component_meta_map
	componentMeta := vl.componentMetaMap[componentID]
	componentMeta.Digest = levelRun.Digest
	vl.componentMetaMap[componentID] = componentMeta
	

	// Insert the run into the disk-level
	var level *run.Level = nil
	if levelID < len(vl.levelVec) && vl.levelVec[levelID] != nil {
		level = vl.levelVec[levelID]
	}
	if level != nil {
		// always insert the new run to the front, so that the latest states are at the front of the level
		level.RunVec = append([]*run.LevelRun{levelRun}, level.RunVec...)
	} else {
		// the level with level_id does not exist, so create a new one
		newLevel := run.NewLevel(levelID)
		newLevel.RunVec = append([]*run.LevelRun{levelRun}, newLevel.RunVec...)
		vl.levelVec = append(vl.levelVec, newLevel)
	}

	// remove the flushed memtable from the immutable memtable vector
	vl.immutableMemTableVecLock.Lock()
	vl.immutableMemTableVec = vl.immutableMemTableVec[1:]
	vl.immutableMemTableVecLock.Unlock()

	// iteratively merge the levels if the level reaches the capacity
	//vl.check_and_merge()
}

func (vl *VLStore) flushMemTableWorker() {
	for mt := range vl.flushImmutableMemTableChan {
		vl.flushMemTable(mt)
	}
}

// from the first disk level to the last disk level, check whether a level reaches the capacity, if so, merge all the runs in the level to the next level
//func (vl *VLStore) check_and_merge() {}