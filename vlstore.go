package vlstore

import (
	"VLStore/memtable"
	"VLStore/disktable"
	"VLStore/util"
	"encoding/binary"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
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
	configs  *util.Configs
	memTable *memtable.MBTree
	//mu                         sync.RWMutex
	immutableMemTableVec       []*memtable.MBTree // immutable memtables, waiting to be flushed to disk
	immutableMemTableVecLock   sync.RWMutex
	flushImmutableMemTableChan chan *memtable.MBTree // channel to notify the flush goroutine to flush immutable memtable to disk
	diskTableVec               []*disktable.MerkleLearnedTable
	componentIDCnt             atomic.Int64 // this helps to generate a component_id
	componentMetaMap           map[int]*ComponentMeta
	componentMetaMapLock       sync.RWMutex
	cacheManager               *disktable.CacheManager
}

// create a new index with given configs
func NewVLStore(configs *util.Configs) *VLStore {
	vl := &VLStore{
		configs:                    configs,
		immutableMemTableVec:       []*memtable.MBTree{},
		flushImmutableMemTableChan: make(chan *memtable.MBTree, 16),
		diskTableVec:               []*disktable.MerkleLearnedTable{},
		componentIDCnt:             atomic.Int64{},
		componentMetaMap:           make(map[int]*ComponentMeta),
		componentMetaMapLock:       sync.RWMutex{},
		cacheManager:               disktable.NewCacheManager(),
	}
	vl.memTable = memtable.NewBPlusTree(vl.newComponentID(), configs.TreeFanout)
	vl.componentMetaMapLock.Lock()
	vl.componentMetaMap[vl.memTable.GetComponentID()] = newComponentMeta(-1, -1, util.H256{})
	vl.componentMetaMapLock.Unlock()

	go vl.flushMemTableWorker()

	return vl
}

func (vl *VLStore) getMeta() {
	path := fmt.Sprintf("%s/mht", vl.configs.DirName)
	file, err := os.ReadFile(path)
	if err != nil {
		panic(err)
	}


	// read component_id_cnt（atomic.Int64{},） from the file
	componentIDCntBytes := make([]byte, 8)
	copy(componentIDCntBytes, file[8:16])
	componentIDCnt := int64(binary.BigEndian.Uint64(componentIDCntBytes))
	vl.componentIDCnt.Store(componentIDCnt)

	// read mem mht from the file
	// read component_meta_map from the file
	// TODO: implement this
}

// load a new index using configs
func Load(configs *util.Configs) *VLStore {
	vl := NewVLStore(configs)
	vl.getMeta()
	// load disk tables
	for i := 0; i < len(vl.diskTableVec); i++ {
		diskTable, err := disktable.Load(i, vl.configs)
		if err != nil {
			panic(err)
		}
		vl.diskTableVec = append(vl.diskTableVec, diskTable)
	}

	return vl
}

func (vl *VLStore) newComponentID() int {
	// increment the component_id and return it
	componentID := vl.componentIDCnt.Add(1)
	return int(componentID)
}

func (vl *VLStore) Insert(key util.Key, value util.Value) {
	// directly insert the state into the mem_mht
	vl.memTable.Insert(key, value)

	// update the component_meta_map
	vl.componentMetaMapLock.Lock()
	componentMeta := vl.componentMetaMap[vl.memTable.GetComponentID()]
	if componentMeta.MaxKey == -1 || componentMeta.MaxKey < key {
		componentMeta.MaxKey = key
	}
	if componentMeta.MinKey == -1 || componentMeta.MinKey > key {
		componentMeta.MinKey = key
	}
	componentMeta.Digest = vl.memTable.GetHash()
	vl.componentMetaMap[vl.memTable.GetComponentID()] = componentMeta
	vl.componentMetaMapLock.Unlock()

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
		vl.componentMetaMapLock.Lock()
		vl.componentMetaMap[vl.memTable.GetComponentID()] = newComponentMeta(-1, -1, util.H256{})
		vl.componentMetaMapLock.Unlock()
		// flush the immutable memtable to disk
		vl.flushImmutableMemTableChan <- immutableMemTable
	}
}

// search a key in the VLStore
func (vl *VLStore) Search(key util.Key) util.Value {
	//vl.mu.RLock()
	//defer vl.mu.RUnlock()

	// search in the memtable
	memTableID := vl.memTable.GetComponentID()
	vl.componentMetaMapLock.RLock()
	memTableMeta := vl.componentMetaMap[memTableID]
	minKey := memTableMeta.MinKey
	maxKey := memTableMeta.MaxKey
	vl.componentMetaMapLock.RUnlock()
	if key >= minKey && key <= maxKey {
		value, isExist := vl.memTable.Search(key)
		if isExist {
			return value
		}
	}

	// search in the immutable memtable vector
	vl.immutableMemTableVecLock.RLock()
	for _, mt := range vl.immutableMemTableVec {
		immutableMemTableID := mt.GetComponentID()
		vl.componentMetaMapLock.RLock()
		immutableMemTableMeta := vl.componentMetaMap[immutableMemTableID]
		minK := immutableMemTableMeta.MinKey
		maxK := immutableMemTableMeta.MaxKey
		vl.componentMetaMapLock.RUnlock()
		if key >= minK && key <= maxK {
			value, isExist := mt.Search(key)
			if isExist {
				vl.immutableMemTableVecLock.RUnlock()
				return value
			}
		}
	}
	vl.immutableMemTableVecLock.RUnlock()

	// search disk tables on the disk
	for _, diskTable := range vl.diskTableVec {
		diskTableID := diskTable.ComponentID
			vl.componentMetaMapLock.RLock()
			diskTableMeta := vl.componentMetaMap[diskTableID]
			minK := diskTableMeta.MinKey
			maxK := diskTableMeta.MaxKey
			vl.componentMetaMapLock.RUnlock()
			if key >= minK && key <= maxK {
				keyValue := diskTable.SearchTable(key, vl.configs, vl.cacheManager)
				if keyValue != nil {
					return keyValue.Value
			}
		}
	}

	return nil
}

// 返回和查询范围有重叠的组件的component_id
func (vl *VLStore) SearchRangeFromComponentMeta(startKey util.Key, endKey util.Key) map[int]bool {
	componentIDSet := make(map[int]bool)

	// 遍历所有组件的元数据
	vl.componentMetaMapLock.RLock()
	for componentID, meta := range vl.componentMetaMap {
		//fmt.Println("componentID :", componentID, " meta :", meta)
		// 检查组件的键范围是否与查询范围有重叠
		if !(meta.MaxKey < startKey || meta.MinKey > endKey) {
			componentIDSet[componentID] = true
		} else {
			componentIDSet[componentID] = false
		}
	}
	vl.componentMetaMapLock.RUnlock()

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
	proof   memtable.RangeProof
	hash    util.H256
	isHash  bool
}

type DiskTableProofOrHash struct {
	results []util.KeyValue
	proof   disktable.TableProof
	hash    util.H256
	isHash  bool
}


type VLStoreProof struct {
	memTableProofVec []*MemTableProofOrHash
	diskTableProofVec    []*DiskTableProofOrHash
}

func (vl *VLStore) SearchWithProof(startKey util.Key, endKey util.Key) *VLStoreProof {
	//vl.mu.RLock()
	//defer vl.mu.RUnlock()

	// get the component_id vector that has overlap with the query range
	componentIDSet := vl.SearchRangeFromComponentMeta(startKey, endKey)

	vlStoreProof := &VLStoreProof{
		memTableProofVec: []*MemTableProofOrHash{},
		diskTableProofVec:    []*DiskTableProofOrHash{},
	}

	// search in the memtable
	memTableStart := time.Now()
	memTableID := vl.memTable.GetComponentID()
	var memTableResults []util.KeyValue
	var memTableProofOrHash MemTableProofOrHash
	if componentIDSet[memTableID] {
		var memTableProof memtable.RangeProof
		memTableResults, memTableProof = vl.memTable.GenerateRangeProof(startKey, endKey)
		memTableProofOrHash = MemTableProofOrHash{
			results: memTableResults,
			proof:   memTableProof,
			hash:    util.H256{},
			isHash:  false,
		}
	} else {
		memTableProofOrHash = MemTableProofOrHash{
			results: memTableResults,
			proof:   memtable.RangeProof{},
			hash:    vl.memTable.GetHash(),
			isHash:  true,
		}
	}
	vlStoreProof.memTableProofVec = append(vlStoreProof.memTableProofVec, &memTableProofOrHash)
	fmt.Printf("MemTable search time: %v, found %d results\n", time.Since(memTableStart), len(memTableResults))

	// search in the immutable memtable vector
	vl.immutableMemTableVecLock.RLock()
	for _, mt := range vl.immutableMemTableVec {
		immutableMemTableStart := time.Now()
		immutableMemTableID := mt.GetComponentID()
		var immutableMemTableResults []util.KeyValue
		var immutableMemTableProofOrHash MemTableProofOrHash
		if componentIDSet[immutableMemTableID] {
			immutableMemTableResults, immutableMemTableProof := mt.GenerateRangeProof(startKey, endKey)
			immutableMemTableProofOrHash = MemTableProofOrHash{
				results: immutableMemTableResults,
				proof:   immutableMemTableProof,
				hash:    util.H256{},
				isHash:  false,
			}
		} else {
			immutableMemTableProofOrHash = MemTableProofOrHash{
				results: immutableMemTableResults,
				proof:   memtable.RangeProof{},
				hash:    mt.GetHash(),
				isHash:  true,
			}
		}
		vlStoreProof.memTableProofVec = append(vlStoreProof.memTableProofVec, &immutableMemTableProofOrHash)
		fmt.Printf("ImmutableMemTable search time: %v, found %d results\n", time.Since(immutableMemTableStart), len(immutableMemTableResults))
	}
	vl.immutableMemTableVecLock.RUnlock()

	// search in the disk tables
	for _, diskTable := range vl.diskTableVec {
			diskTableStart := time.Now()
			diskTableID := diskTable.ComponentID
			fmt.Println("diskTableID :", diskTableID)
			var diskTableResults []util.KeyValue
			var diskTableProofOrHash DiskTableProofOrHash
			if componentIDSet[diskTableID] {
				var diskTableProof *disktable.TableProof
				diskTableResults, diskTableProof = diskTable.ProveRange(startKey, endKey, vl.configs, vl.cacheManager)
				diskTableProofOrHash = DiskTableProofOrHash{
					results: diskTableResults,
					proof:   *diskTableProof,
					hash:    util.H256{},
					isHash:  false,
				}
			} else {
				diskTableProofOrHash = DiskTableProofOrHash{
					results: diskTableResults,
					proof:   disktable.TableProof{},
					hash:    diskTable.Digest,
					isHash:  true,
				}
			}
			vlStoreProof.diskTableProofVec = append(vlStoreProof.diskTableProofVec, &diskTableProofOrHash)
			fmt.Printf("DiskTable search time: %v, found %d results\n", time.Since(diskTableStart), len(diskTableResults))
	}
	return vlStoreProof
}

func (vl *VLStore) ComputeDigest() util.H256 {
	HashVec := make([]util.H256, 0)

	// compute the digest of the memtable
	memTableHash := vl.memTable.GetHash()
	HashVec = append(HashVec, memTableHash)
	// compute the digest of the immutable memtable vector
	vl.immutableMemTableVecLock.RLock()
	for _, mt := range vl.immutableMemTableVec {
		immutableMemTableHash := mt.GetHash()
		HashVec = append(HashVec, immutableMemTableHash)
	}
	vl.immutableMemTableVecLock.RUnlock()

	// collect each disk-table's digest
	for _, diskTable := range vl.diskTableVec {
		diskTableHash := diskTable.Digest
		HashVec = append(HashVec, diskTableHash)
	}

	bytes := make([]byte, 0)
	for _, hash := range HashVec {
		bytes = append(bytes, hash[:]...)
	}
	return util.NewBlake3Hasher().HashBytes(bytes)
}

func (vl *VLStore) VerifyAndCollectResult(startKey util.Key, endKey util.Key, proof *VLStoreProof, rootHash util.H256, fanout int) (bool, []util.KeyValue) {
	componentDigestVec := make([]util.H256, 0)

	// Prepare containers for all components
	memVecLen := len(proof.memTableProofVec)
	memDigests := make([]util.H256, memVecLen)
	memResults := make([][]util.KeyValue, memVecLen)

	diskTableCount := len(proof.diskTableProofVec)
	diskTableDigests := make([]util.H256, diskTableCount)
	diskTableResults := make([][]util.KeyValue, diskTableCount)

	// Launch a global parallel batch for all memtables and all runs
	var wg sync.WaitGroup

	// memtable + immutable memtables
	for i := 0; i < memVecLen; i++ {
		idx := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			proofOrHash := proof.memTableProofVec[idx]
			memResults[idx] = proofOrHash.results
			if proofOrHash.isHash {
				memDigests[idx] = proofOrHash.hash
			} else {
				memDigests[idx] = memtable.ReconstructRangeProof(startKey, endKey, memResults[idx], proofOrHash.proof)
			}
		}()
	}

	// all disk tables
	for i := 0; i < diskTableCount; i++ {
		idx := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			diskTableProofOrHash := proof.diskTableProofVec[idx]
			diskTableResults[idx] = diskTableProofOrHash.results
			if diskTableProofOrHash.isHash {
				diskTableDigests[idx] = diskTableProofOrHash.hash
			} else {
				_, digest := disktable.ReconstructRunProof(startKey, endKey, diskTableResults[idx], &diskTableProofOrHash.proof, fanout)
				diskTableDigests[idx] = digest
			}
		}()
	}
	

	wg.Wait()

	// Assemble component digests and merged results deterministically
	mergeResult := make([]util.KeyValue, 0)
	for i := 0; i < memVecLen; i++ {
		componentDigestVec = append(componentDigestVec, memDigests[i])
		if len(memResults[i]) > 0 {
			mergeResult = append(mergeResult, memResults[i]...)
		}
	}

	for di := 0; di < diskTableCount; di++ {
		componentDigestVec = append(componentDigestVec, diskTableDigests[di])
		if len(diskTableResults[di]) > 0 {
			mergeResult = append(mergeResult, diskTableResults[di]...)
		}
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

	// filter elements not in [startKey, endKey] in-place (linear time)
	dst := mergeResult[:0]
	for _, kv := range mergeResult {
		if kv.Key >= startKey && kv.Key <= endKey {
			dst = append(dst, kv)
		}
	}
	mergeResult = dst

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
	iter := disktable.NewInMemKeyValueIterator(keyValues)

	// Construct a new disk table by the key-values
	componentID := table.GetComponentID()
	diskTable, err := disktable.ConstructRunByInMemoryTree(keyValues, iter, componentID, vl.configs.DirName, vl.configs.Epsilon, vl.configs.Fanout)
	if err != nil {
		panic(err)
	}

	// update the component_meta_map
	vl.componentMetaMapLock.Lock()
	componentMeta := vl.componentMetaMap[componentID]
	componentMeta.Digest = diskTable.Digest
	vl.componentMetaMap[componentID] = componentMeta
	vl.componentMetaMapLock.Unlock()

	// remove the flushed memtable from the immutable memtable vector
	vl.immutableMemTableVecLock.Lock()
	vl.immutableMemTableVec = vl.immutableMemTableVec[1:]
	vl.immutableMemTableVecLock.Unlock()

	// insert the disk table into the disk table vector
	vl.diskTableVec = append([]*disktable.MerkleLearnedTable{diskTable}, vl.diskTableVec...)
}

func (vl *VLStore) flushMemTableWorker() {
	for mt := range vl.flushImmutableMemTableChan {
		vl.flushMemTable(mt)
	}
}

// from the first disk level to the last disk level, check whether a level reaches the capacity, if so, merge all the runs in the level to the next level
//func (vl *VLStore) check_and_merge() {}
