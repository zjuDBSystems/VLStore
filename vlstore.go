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
)

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
	immutableMemTableVec       []*memtable.MBTree    // immutable memtables, waiting to be flushed to disk
	immutableMemTableVecLock   sync.RWMutex
	flushImmutableMemTableChan chan *memtable.MBTree // channel to notify the flush goroutine to flush immutable memtable to disk
	levelVec                   []*run.Level
	runIDCnt                   int // this helps to generate a new run_id
}

// create a new index with given configs
func NewVLStore(configs *util.Configs) *VLStore {
	vl := &VLStore{
		configs:                    configs,
		memTable:                   memtable.NewBPlusTree(configs.Fanout),
		immutableMemTableVec:          []*memtable.MBTree{},
		flushImmutableMemTableChan: make(chan *memtable.MBTree),
		levelVec:                   []*run.Level{},
		runIDCnt:                   0,
	}

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

	// read run_id_cnt from the file
	runIDCntBytes := make([]byte, 8)
	copy(runIDCntBytes, file[8:16])
	runIDCnt := int(binary.BigEndian.Uint64(runIDCntBytes))
	vl.runIDCnt = runIDCnt

	// read mem mht from the file
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

func (vl *VLStore) newRunID() int {
	// increment the run_id and return it
	vl.runIDCnt++
	return vl.runIDCnt
}

func (vl *VLStore) Insert(key util.Key, value util.Value) {
	vl.mu.Lock()
	defer vl.mu.Unlock()

	// directly insert the state into the mem_mht
	vl.memTable.Insert(key, value)

	// check if the memtable is full and need to be flushed to disk
	inMemThreshold := vl.configs.BaseStateNum
	if vl.memTable.KeyNum() >= inMemThreshold {
		// the in-memory mb-tree is full
		// swap the memtable to the immutable memtable and create a new memtable
		immutableMemTable := vl.memTable

		vl.immutableMemTableVecLock.Lock()
		vl.immutableMemTableVec = append(vl.immutableMemTableVec, immutableMemTable)
		vl.immutableMemTableVecLock.Unlock()

		vl.memTable = memtable.NewBPlusTree(vl.configs.Fanout)
		// flush the immutable memtable to disk
		vl.flushImmutableMemTableChan <- immutableMemTable
	}
}

// search a key in the VLStore
func (vl *VLStore) Search(key util.Key) util.Value {
	vl.mu.RLock()
	defer vl.mu.RUnlock()

	// search in the memtable
	value, isExist := vl.memTable.Search(key)
	if isExist {
		return value
	}

	// search in the immutable memtable vector
	for _, mt := range vl.immutableMemTableVec {
		value, isExist := mt.Search(key)
		if isExist {
			return value
		}
	}

	// search other levels on the disk
	for _, level := range vl.levelVec {
		for _, run := range level.RunVec {
			keyValue := run.SearchRun(key, vl.configs)
			if keyValue != nil {
				return keyValue.Value
			}
		}
	}

	return nil
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
	runProofVec []*LevelRunProofOrHash
}

type VLStoreProof struct {
	memTableProofVec []*MemTableProofOrHash	
	levelProofVec []*LevelProof
}

func (vl *VLStore) SearchWithProof(startKey util.Key, endKey util.Key) *VLStoreProof {
	vl.mu.RLock()
	defer vl.mu.RUnlock()

	vlStoreProof := &VLStoreProof{
		memTableProofVec: []*MemTableProofOrHash{},
		levelProofVec: []*LevelProof{},
	}
    restIsHash := false
	// search in the memtable
	memTableResults, memTableProof := vl.memTable.GenerateRangeProof(startKey, endKey)
	if len(memTableResults) > 0 {
		leftMostResult := memTableResults[0]
		if leftMostResult.Key <= startKey {
			restIsHash = true
		}
	}
	vlStoreProof.memTableProofVec = append(vlStoreProof.memTableProofVec, &MemTableProofOrHash{
		results: memTableResults,
		proof: memTableProof,
		hash: util.H256{},
		isHash: false,
	})


	// search in the immutable memtable vector
	for _, mt := range vl.immutableMemTableVec {
		if restIsHash {
			hash := mt.GetHash()
			vlStoreProof.memTableProofVec = append(vlStoreProof.memTableProofVec, &MemTableProofOrHash{
				results: nil,
				proof: memtable.RangeProof{},
				hash: hash,
				isHash: true,
			})
		}else{
			memTableResults, memTableProof := mt.GenerateRangeProof(startKey, endKey)
			if len(memTableResults) > 0 {
				leftMostResult := memTableResults[0]
				if leftMostResult.Key <= startKey {
					restIsHash = true
				}
			}
			vlStoreProof.memTableProofVec = append(vlStoreProof.memTableProofVec, &MemTableProofOrHash{
				results: memTableResults,
				proof: memTableProof,
				hash: util.H256{},
				isHash: false,
			})
		}	
	}

	// search in the disk-level
	for _, level := range vl.levelVec {
		levelProof := &LevelProof{
			runProofVec: []*LevelRunProofOrHash{},
		}
		for _, levelRun := range level.RunVec {
			if restIsHash {
				hash := levelRun.Digest
				levelProof.runProofVec = append(levelProof.runProofVec, &LevelRunProofOrHash{
					results: nil,
					proof: run.RunProof{},
					hash: hash,
					isHash: true,
				})
			}else{
				runResults, runProof := levelRun.ProveRange(startKey, endKey, vl.configs)
				
				// 打印runResults 中的key
				fmt.Println("runResults :")
				for _, kv := range runResults {
					fmt.Print(kv.Key, " ")
				}
				fmt.Println()

				if len(runResults) > 0 {
					leftMostResult := runResults[0]
					if leftMostResult.Key <= startKey {
						restIsHash = true
					}
				}
				levelProof.runProofVec = append(levelProof.runProofVec, &LevelRunProofOrHash{
					results: runResults,
					proof: *runProof,
					hash: util.H256{},
					isHash: false,
				})
			}
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
	levelRoots := make([]util.H256, 0)

	// first reconstruct the memtable_proof
	memTableResult := proof.memTableProofVec[0].results

	// 打印memTableResult 中的key
	fmt.Println("memTable :")
	for _, kv := range memTableResult {
		fmt.Print(kv.Key, " ")
	}
	fmt.Println()

	h := memtable.ReconstructRangeProof(startKey, endKey, memTableResult, proof.memTableProofVec[0].proof)
	levelRoots = append(levelRoots, h)

	



	mergeResult := make([]util.KeyValue, 0)
	restIsHash := false

	if len(memTableResult) > 0 {
		leftMostResult := memTableResult[0]
		if leftMostResult.Key <= startKey{
			restIsHash = true
		}
		mergeResult = append(mergeResult, memTableResult...)
	}

	// reconstruct the immutable memtable_proof
	for i := 1; i < len(proof.memTableProofVec); i++ {
		memTableProof := proof.memTableProofVec[i]
		memTableResult := memTableProof.results

		// 打印memTableResult 中的key
		fmt.Println("immutable memTable :", i)
		for _, kv := range memTableResult {
			fmt.Print(kv.Key, " ")
		}
		fmt.Println()

		if memTableProof.isHash {
			if !restIsHash {
				return false, nil
			}
			levelRoots = append(levelRoots, memTableProof.hash)
		}else{
			if restIsHash {
				return false, nil
			}
			h := memtable.ReconstructRangeProof(startKey, endKey, memTableResult, memTableProof.proof)
			levelRoots = append(levelRoots, h)		
		}

		if len(memTableResult) > 0 {
			leftMostResult := memTableResult[0]
			if leftMostResult.Key <= endKey {
				restIsHash = true
			}
			mergeResult = append(mergeResult, memTableResult...)
		}

	}

	// reconstruct the level_proof
	for _, levelProof := range proof.levelProofVec {
		levelHashVec := make([]util.H256, 0)
		for i, levelRunProof := range levelProof.runProofVec {
			runResults, runProof := levelRunProof.results, levelRunProof.proof
			// 打印runResults 中的key
			fmt.Println("run :", i)
			for _, kv := range runResults {
				fmt.Print(kv.Key, " ")
			}
			fmt.Println()
			if levelRunProof.isHash {
				if !restIsHash {
					return false, nil
				}
				levelHashVec = append(levelHashVec, levelRunProof.hash)
			}else{
				if restIsHash {
					return false, nil
				}
				_, h := run.ReconstructRunProof(startKey, endKey, runResults, &runProof, fanout)
				levelHashVec = append(levelHashVec, h)
			}
			if len(runResults) > 0 {
				leftMostResult := runResults[0]
				if leftMostResult.Key <= endKey {
					restIsHash = true
				}
				mergeResult = append(mergeResult, runResults...)
			}
		}
		levelH := util.NewBlake3Hasher().ComputeConcatHash(levelHashVec)
		levelRoots = append(levelRoots, levelH)
	}
	reconstructRootHash := util.NewBlake3Hasher().ComputeConcatHash(levelRoots)
	if reconstructRootHash != rootHash {
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
	runID := vl.newRunID()
	levelID := 0 // the first on-disk level id is 0
	levelNumOfRuns := 0
	if levelID < len(vl.levelVec) && vl.levelVec[levelID] != nil {
		levelNumOfRuns = len(vl.levelVec[levelID].RunVec)
	}
	levelRun, err := run.ConstructRunByInMemoryTree(iter, runID, levelID, vl.configs.DirName, vl.configs.Epsilon, vl.configs.Fanout, vl.configs.MaxNumOfStatesInARun(levelID), levelNumOfRuns, vl.configs.SizeRatio)
	if err != nil {
		panic(err)
	}

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