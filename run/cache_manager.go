package run

import (
	"VLStore/util"

	lru "github.com/hashicorp/golang-lru/v2"
)

const (
	CACHE_SIZE = 1024 * 1024 * 10 // 10MB
)

// PageIndex 用于标识缓存中的页面
type PageIndex struct {
	RunID  int
	PageID int
}

// CacheManager 管理三种类型的页面缓存
type CacheManager struct {
	mhtCache   *lru.Cache[PageIndex, []util.H256]       // 存储MHT页面
	keyCache   *lru.Cache[PageIndex, []KeyPos]          // 存储键页面
	modelCache *lru.Cache[PageIndex, *ModelCollections] // 存储模型页面
}

// NewCacheManager 创建一个新的缓存管理器
func NewCacheManager() *CacheManager {
	// 计算每种缓存的容量
	mhtCapacity := (CACHE_SIZE / PAGE_SIZE) * 0.3
	keyCapacity := (CACHE_SIZE / PAGE_SIZE) * 0.3
	modelCapacity := (CACHE_SIZE / PAGE_SIZE) * 0.4

	mhtCache, err := lru.New[PageIndex, []util.H256](int(mhtCapacity))
	if err != nil {
		panic(err)
	}

	keyCache, err := lru.New[PageIndex, []KeyPos](int(keyCapacity))
	if err != nil {
		panic(err)
	}

	modelCache, err := lru.New[PageIndex, *ModelCollections](int(modelCapacity))
	if err != nil {
		panic(err)
	}

	return &CacheManager{
		mhtCache:   mhtCache,
		keyCache:   keyCache,
		modelCache: modelCache,
	}
}

// GetMHTPage 从缓存中获取MHT页面
func (cm *CacheManager) GetMHTPage(runID int, pageID int) ([]util.H256, bool) {
	index := PageIndex{RunID: runID, PageID: pageID}
	return cm.mhtCache.Get(index)
}

// SetMHTPage 将MHT页面存入缓存
func (cm *CacheManager) SetMHTPage(runID int, pageID int, hashes []util.H256) {
	index := PageIndex{RunID: runID, PageID: pageID}
	cm.mhtCache.Add(index, hashes)
}

// GetKeyPage 从缓存中获取键页面
func (cm *CacheManager) GetKeyPage(runID int, pageID int) ([]KeyPos, bool) {
	index := PageIndex{RunID: runID, PageID: pageID}
	return cm.keyCache.Get(index)
}

// SetKeyPage 将键页面存入缓存
func (cm *CacheManager) SetKeyPage(runID int, pageID int, keyPos []KeyPos) {
	index := PageIndex{RunID: runID, PageID: pageID}
	cm.keyCache.Add(index, keyPos)
}

// GetModelPage 从缓存中获取模型页面
func (cm *CacheManager) GetModelPage(runID int, pageID int) (*ModelCollections, bool) {
	index := PageIndex{RunID: runID, PageID: pageID}
	return cm.modelCache.Get(index)
}

// SetModelPage 将模型页面存入缓存
func (cm *CacheManager) SetModelPage(runID int, pageID int, models *ModelCollections) {
	index := PageIndex{RunID: runID, PageID: pageID}
	cm.modelCache.Add(index, models)
}

// ClearCache 清空所有缓存
func (cm *CacheManager) ClearCache() {
	cm.mhtCache.Purge()
	cm.keyCache.Purge()
	cm.modelCache.Purge()
}

// GetCacheSize 获取当前缓存大小
func (cm *CacheManager) GetCacheSize() (int, int, int) {
	var mhtSize, keySize, modelSize int

	// 遍历MHT缓存
	for _, hashes := range cm.mhtCache.Values() {
		mhtSize += len(hashes) * util.H256_SIZE
	}

	// 遍历键缓存
	for _, keyPos := range cm.keyCache.Values() {
		keySize += len(keyPos) * util.KEY_POS_SIZE
	}

	// 遍历模型缓存
	for _, models := range cm.modelCache.Values() {
		modelSize += len(models.V) * util.Model_SIZE
	}

	return mhtSize, keySize, modelSize
}
