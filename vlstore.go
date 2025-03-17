package vlstore

import (
	"VLStore/memdb"
	"VLStore/run"
	"VLStore/util"
	"encoding/binary"
	"fmt"
	"os"
)

/*
VLStore consists of:

	(i) a reference of configs that include params
	(ii) an in-memory MB-Tree as the authenticated index
	(iii) a vector of levels that stores each level's LevelRuns
*/
type VLStore struct {
	Configs  *util.Configs
	MemDB    *memdb.MBTree
	//ImmutableMemDB *memdb.MBTree
	LevelVec []*run.Level
	runIDCnt int // this helps to generate a new run_id
}

// create a new index with given configs
func NewVLStore(configs *util.Configs) *VLStore {
	return &VLStore{
		Configs:  configs,
		MemDB:    memdb.NewBPlusTree(configs.Fanout),
		LevelVec: []*run.Level{},
		runIDCnt: 0,
	}
}

func (vl *VLStore) getMeta() int {
	path := fmt.Sprintf("%s/mht", vl.Configs.DirName)
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
		level, err := run.LoadLevel(i, vl.Configs)
		if err != nil {
			panic(err)
		}
		vl.LevelVec = append(vl.LevelVec, level)
	}

	return vl
}

func (vl *VLStore) newRunID() int {
	// increment the run_id and return it
	vl.runIDCnt++
	return vl.runIDCnt
}

func (vl *VLStore) Insert(key util.Key, value util.Value) {
	// directly insert the state into the mem_mht
	vl.MemDB.Insert(key, value)

	// compute the in-memory threshold
	inMemThreshold := vl.Configs.BaseStateNum
	if vl.MemDB.KeyNum() == inMemThreshold {
		// the in-memory mb-tree is full, the data should be merged to the run in the disk-level
		keyValues := vl.MemDB.LoadAllKeyValues()
		// clear the in-memory mb-tree
		vl.MemDB = memdb.NewBPlusTree(vl.Configs.Fanout)

		iter := run.NewInMemKeyValueIterator(keyValues)
		runID := vl.newRunID()
		levelID := 0 // the first on-disk level id is 0
		levelNumOfRuns := 0
		if levelID < len(vl.LevelVec) && vl.LevelVec[levelID] != nil {
			levelNumOfRuns = len(vl.LevelVec[levelID].RunVec)
		}
		levelRun, err := run.ConstructRunByInMemoryTree(iter, runID, levelID, vl.Configs.DirName, vl.Configs.Epsilon, vl.Configs.Fanout, vl.Configs.MaxNumOfStatesInARun(levelID), levelNumOfRuns, vl.Configs.SizeRatio)
		if err != nil {
			panic(err)
		}
		var level *run.Level = nil
		if levelID < len(vl.LevelVec) && vl.LevelVec[levelID] != nil {
			level = vl.LevelVec[levelID]
		}
		if level != nil {
			// always insert the new run to the front, so that the latest states are at the front of the level
			level.RunVec = append([]*run.LevelRun{levelRun}, level.RunVec...)
		} else {
			// the level with level_id does not exist, so create a new one
			newLevel := run.NewLevel(levelID)
			newLevel.RunVec = append([]*run.LevelRun{levelRun}, newLevel.RunVec...)
			vl.LevelVec = append(vl.LevelVec, newLevel)
		}
		// iteratively merge the levels if the level reaches the capacity
		vl.check_and_merge()
	}
}

// from the first disk level to the last disk level, check whether a level reaches the capacity, if so, merge all the runs in the level to the next level
func (vl *VLStore) check_and_merge() {
	// TODO:

}

func (vl *VLStore) Search(key util.Key) util.Value {
	// search in the in-memory mb-tree
	value, isExist := vl.MemDB.Search(key)
	if isExist {
		return value
	}

	// search other levels on the disk
	for _, level := range vl.LevelVec {
		for _, run := range level.RunVec {
			keyValue := run.SearchRun(key, vl.Configs)
			if keyValue != nil {
				return keyValue.Value
			}
		}
	}

	return nil
}
