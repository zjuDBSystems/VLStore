package run

import (
	"VLStore/util"
	"encoding/binary"
	"fmt"
	"os"
)

// a level is a collection of runs
type Level struct {
	LevelID int // id to identify the level
	RunVec  []*LevelRun // a vector of runs in this level
}

func NewLevel(levelID int) *Level {
	return &Level{
		LevelID: levelID,
		RunVec:  []*LevelRun{},
	}
}


// load the level from the file given level_id and the reference of configs
func LoadLevel(levelID int, configs *util.Configs) (*Level, error) {
	level := NewLevel(levelID)
	levelMetaFileName := level.LevelMetaFileName(configs)
	
	file, err := os.Open(levelMetaFileName)
	if err != nil {
		return nil, fmt.Errorf("打开级别元数据文件失败: %w", err)
	}
	defer file.Close()
	
	// read num_of_run from the file
	var numRunsBytes [8]byte
	if _, err := file.Read(numRunsBytes[:]); err != nil {
		return nil, fmt.Errorf("读取运行数量失败: %w", err)
	}
	numRuns := int(binary.BigEndian.Uint64(numRunsBytes[:]))
	
	// read run_id from the file and load the run to the vector
	for i := 0; i < numRuns; i++ {
		var runIDBytes [8]byte
		if _, err := file.Read(runIDBytes[:]); err != nil {
			return nil, fmt.Errorf("读取运行ID失败: %w", err)
		}
		
		// deserialize the run_id
		runID := int(binary.BigEndian.Uint64(runIDBytes[:]))
		
		// load the run 
		run, err := Load(runID, levelID, configs)
		if err != nil {
			return nil, fmt.Errorf("加载运行 %d 失败: %w", runID, err)
		}
		
		level.RunVec = append(level.RunVec, run)
	}
	
	return level, nil
}

// persist the level, including the run_id and run's filter of each run in run_vec
func (l *Level) Persist(configs *util.Configs) {
	numOfRuns := len(l.RunVec)
	b := make([]byte, 0)
	// store the binary bytes of num_of_run
	numOfRunsBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(numOfRunsBytes, uint64(numOfRuns))
	b = append(b, numOfRunsBytes...)
	// store the binary bytes of run_id and run's filter to the output vector
	for _, run := range l.RunVec {
		// get the run_id of the current run
		componentID := run.ComponentID
		// store the binary of run_id 
		componentIDBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(componentIDBytes, uint64(componentID))
		b = append(b, componentIDBytes...)	
	}

	levelMetaFileName := l.LevelMetaFileName(configs)
	file, err := os.Create(levelMetaFileName)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	
	// write the binary bytes to the file
	if _, err := file.Write(b); err != nil {
		panic(err)
	}
}

func (l *Level) LevelMetaFileName(config *util.Configs) string {
	//format!("{}/{}.lv", &configs.dir_name, self.level_id)
	return fmt.Sprintf("%s/%d.lv", config.DirName, l.LevelID)
}
// if the number of runs in the level is the same as the size ratio, the level is full
func (l *Level) levelReachCapacity(config *util.Configs) bool {
	return len(l.RunVec) >= config.SizeRatio
}

 // compute the digest of the level
func (l *Level) ComputeDigest() util.H256 {
	bytes := make([]byte, 0)
	for _, run := range l.RunVec {
		//runHash := run.ComputeDigest()
		runHash := run.Digest
		bytes = append(bytes, runHash[:]...)
	}
	
	return util.NewBlake3Hasher().HashBytes(bytes)
}
