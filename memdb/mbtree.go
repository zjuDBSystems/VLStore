package memdb

import (
	"VLStore/util"
	"bytes"
	"fmt"
)

// 节点接口定义
type node interface {
	IsLeaf() bool
	GetHash() util.H256
	ComputeHash(hasher util.Hasher)
}

// 内部节点实现
type internalNode struct {
	Parent      *internalNode
	Keys        []util.Key
	Children    []node
	ChildHashes []util.H256
	Hash        util.H256
}

// 叶子节点实现
type leafNode struct {
	Parent    *internalNode
	KeyValues []util.KeyValue
	Next      *leafNode
	Hash      util.H256
}

func (n *internalNode) IsLeaf() bool {
	return false
}

func (n *internalNode) GetHash() util.H256 {
	return n.Hash
}

func (n *internalNode) ComputeHash(hasher util.Hasher) {
	var b []byte
	for _, key := range n.Keys {
		keyHash := hasher.HashKey(key)
		b = append(b, keyHash[:]...)
	}

	for _, childHash := range n.ChildHashes {
		b = append(b, childHash[:]...)
	}

	n.Hash = hasher.HashBytes(b)
}

func (n *leafNode) IsLeaf() bool {
	return true
}

func (n *leafNode) GetHash() util.H256 {
	return n.Hash
}

func (n *leafNode) ComputeHash(hasher util.Hasher) {
	var b []byte
	for _, kv := range n.KeyValues {
		keyHash := hasher.HashKey(kv.Key)
		valueHash := hasher.HashValue(kv.Value)
		b = append(b, keyHash[:]...)
		b = append(b, valueHash[:]...)
	}
	n.Hash = hasher.HashBytes(b)
}

type MBTree struct {
	root   node
	keyNum int32
	fanout int
	tmp    *leafNode
	last   *leafNode
	hasher util.Hasher
}

func NewBPlusTree(fanout int) *MBTree {
	return &MBTree{
		root:   nil,
		keyNum: 0,
		fanout: fanout,
		tmp:    nil,
		last:   nil,
		hasher: util.NewHasher(util.BLAKE3),
	}
}

func (t *MBTree) Search(key util.Key) (util.Value, bool) {
	// 先检查临时节点
	if t.tmp != nil {
		for _, kv := range t.tmp.KeyValues {
			if kv.Key == key {
				return kv.Value, true
			}
		}
	}

	// 如果树为空，直接返回
	if t.root == nil {
		return nil, false
	}

	// 从根开始导航
	current := t.root
	for {
		if current.IsLeaf() {
			// 叶子节点 - 搜索键值
			leaf := current.(*leafNode)

			// 线性搜索以避免任何排序问题
			for _, kv := range leaf.KeyValues {
				if kv.Key == key {
					return kv.Value, true
				}
			}

			return nil, false
		} else {
			// 内部节点导航
			internal := current.(*internalNode)

			// 空检查
			if len(internal.Keys) == 0 && len(internal.Children) == 0 {
				return nil, false
			}

			// 遍历所有键，找到正确的子节点
			childIndex := 0
			for i := 0; i < len(internal.Keys); i++ {
				if key >= internal.Keys[i] {
					childIndex = i + 1
					// 确保不会超出索引范围
					if childIndex >= len(internal.Children) {
						// 报错，并输出idx的值
						fmt.Printf("childIndex %d\n", childIndex)
						// 打印树
						fmt.Println(t.PrintTree())
						panic("childIndex out of range")
					}
				} else {
					break
				}
			}

			current = internal.Children[childIndex]
		}
	}
}

func (t *MBTree) Insert(key util.Key, value util.Value) {
	t.keyNum++

	// 1. 优先插入到tmpNode
	if t.tmp == nil {
		t.tmp = t.createTmpNode()
	}

	t.tmp.KeyValues = append(t.tmp.KeyValues, util.KeyValue{Key: key, Value: value})
	t.tmp.ComputeHash(t.hasher)

	// 2. 如果tmpNode满了，执行提交操作
	if len(t.tmp.KeyValues) == t.fanout {
		t.commitTmpNode()
	}
}

func (t *MBTree) commitTmpNode() {
	// 确保临时节点存在
	if t.tmp == nil || len(t.tmp.KeyValues) == 0 {
		panic("no data to commit") // 没有数据需要提交
	}

	// 创建新叶子节点
	newLeaf := t.createLeafNode()

	// 复制临时节点数据
	newLeaf.KeyValues = append(newLeaf.KeyValues, t.tmp.KeyValues...)
	newLeaf.ComputeHash(t.hasher)

	// 插入到主树
	t.insertLeafNode(newLeaf)

	// 清空临时节点
	t.tmp.KeyValues = t.tmp.KeyValues[:0]
	t.tmp.ComputeHash(t.hasher)
}

func (t *MBTree) insertLeafNode(leaf *leafNode) {
	// 场景1：空树初始化
	if t.root == nil {
		t.root = leaf
		t.last = leaf
		return
	}

	// 场景2：找到插入位置（通过last指针）
	var leftParent *internalNode = nil
	if t.last != nil {
		leftParent = t.last.Parent
	}

	// 场景3：插入到最右端

	// 1. 更新叶子节点链表
	if t.last != nil && t.last != leaf {
		t.last.Next = leaf
		t.last.ComputeHash(t.hasher) // 更新哈希
	}
	t.last = leaf

	// 2. 插入分割键到上层节点
	t.insertLeafKeyToParent(leftParent, leaf.KeyValues[0].Key, leaf)
}

func (t *MBTree) insertLeafKeyToParent(leftParent *internalNode, key util.Key, leaf *leafNode) {
	// 1. 判断左兄弟节点是否是根节点（没有父节点）
	if leftParent == nil {
		// 如果根是叶子节点
		if t.root != nil && t.root.IsLeaf() {
			oldRoot := t.root.(*leafNode)

			// 创建新根节点
			newRoot := t.createInternalNode()
			newRoot.Keys = []util.Key{key} // 仅使用新叶子节点的第一个Key作为分隔Key

			newRoot.Children = []node{oldRoot, leaf}
			newRoot.ChildHashes = []util.H256{oldRoot.Hash, leaf.Hash}
			newRoot.ComputeHash(t.hasher)

			// 更新关系
			oldRoot.Parent = newRoot
			leaf.Parent = newRoot

			t.root = newRoot
			return
		}
	}

	// 2. 判断左兄弟节点的父节点是否已满
	if leftParent != nil && len(leftParent.Children) < t.fanout {
		// 父节点未满，直接追加
		leftParent.Keys = append(leftParent.Keys, key)
		leftParent.Children = append(leftParent.Children, leaf)
		leftParent.ChildHashes = append(leftParent.ChildHashes, leaf.Hash)
		leaf.Parent = leftParent
		leftParent.ComputeHash(t.hasher)
	} else {
		// 父节点已满或不存在，需要创建新的父节点
		newParent := t.createInternalNode()
		newParent.Keys = []util.Key{}
		newParent.Children = []node{leaf}
		newParent.ChildHashes = []util.H256{leaf.Hash}
		newParent.ComputeHash(t.hasher)

		// 更新关系
		leaf.Parent = newParent

		// 递归处理上层
		var leftGrandParent *internalNode = nil
		if leftParent != nil {
			leftGrandParent = leftParent.Parent
		}
		t.insertInternalKeyToParent(leftGrandParent, key, newParent)
	}
}

func (t *MBTree) insertInternalKeyToParent(leftParent *internalNode, key util.Key, internal *internalNode) {
	// 1. 判断左兄弟节点是否是根节点（没有父节点）
	if leftParent == nil {
		// 如果根是内部节点
		if t.root != nil && !t.root.IsLeaf() {
			oldRoot := t.root.(*internalNode)

			// 创建新根节点
			newRoot := t.createInternalNode()
			newRoot.Keys = []util.Key{key} // 仅使用新内部节点的第一个Key作为分隔Key

			newRoot.Children = []node{oldRoot, internal}
			newRoot.ChildHashes = []util.H256{oldRoot.Hash, internal.Hash}
			newRoot.ComputeHash(t.hasher)

			// 更新关系
			oldRoot.Parent = newRoot
			internal.Parent = newRoot

			t.root = newRoot
			return
		}
	}

	// 2. 判断左兄弟节点的父节点是否已满
	if leftParent != nil && len(leftParent.Children) < t.fanout {
		// 父节点未满，直接追加
		leftParent.Keys = append(leftParent.Keys, key)
		leftParent.Children = append(leftParent.Children, internal)
		leftParent.ChildHashes = append(leftParent.ChildHashes, internal.Hash)
		internal.Parent = leftParent
		leftParent.ComputeHash(t.hasher)
	} else {
		// 父节点已满或不存在，需要创建新的父节点
		newParent := t.createInternalNode()
		newParent.Keys = []util.Key{}
		newParent.Children = []node{internal}
		newParent.ChildHashes = []util.H256{internal.Hash}
		newParent.ComputeHash(t.hasher)

		// 更新关系
		internal.Parent = newParent

		// 递归处理上层
		var leftGrandParent *internalNode = nil
		if leftParent != nil {
			leftGrandParent = leftParent.Parent
		}
		t.insertInternalKeyToParent(leftGrandParent, key, newParent)
	}
}

func (t *MBTree) createLeafNode() *leafNode {
	leaf := &leafNode{
		Parent:    nil,
		KeyValues: make([]util.KeyValue, 0),
		Next:      nil,
	}
	leaf.ComputeHash(t.hasher)
	return leaf
}

func (t *MBTree) createInternalNode() *internalNode {
	internal := &internalNode{
		Parent:      nil,
		Keys:        make([]util.Key, 0),
		Children:    make([]node, 0),
		ChildHashes: make([]util.H256, 0),
	}
	internal.ComputeHash(t.hasher)
	return internal
}

func (t *MBTree) createTmpNode() *leafNode {
	tmp := &leafNode{
		Parent:    nil,
		KeyValues: make([]util.KeyValue, 0),
		Next:      nil,
	}
	tmp.ComputeHash(t.hasher)
	return tmp
}

// PrintTree 打印整个树的结构, 包括节点个数和树高
func (t *MBTree) PrintTree() string {
	if t.root == nil {
		return "空树"
	}

	var buffer bytes.Buffer

	// 计算树高和节点总数
	height := t.calculateHeight(t.root)
	nodeCount := t.countNodes(t.root)

	buffer.WriteString(fmt.Sprintf("B+树 (阶数=%d, 键数=%d, 树高=%d, 节点总数=%d)\n",
		t.fanout, t.keyNum, height, nodeCount))
	buffer.WriteString("----------------------------------------\n")

	return buffer.String()
}

// calculateHeight 计算树的高度
func (t *MBTree) calculateHeight(n node) int {
	if n == nil {
		return 0
	}

	if n.IsLeaf() {
		return 1
	}

	internal := n.(*internalNode)
	if len(internal.Children) == 0 {
		return 1
	}

	// 只需检查第一个子节点，因为B+树所有叶子节点在同一层
	return 1 + t.calculateHeight(internal.Children[0])
}

// countNodes 递归计算节点总数
func (t *MBTree) countNodes(n node) int {
	if n == nil {
		return 0
	}

	if n.IsLeaf() {
		return 1
	}

	internal := n.(*internalNode)
	count := 1 // 当前节点

	// 递归计算所有子节点
	for _, child := range internal.Children {
		count += t.countNodes(child)
	}

	return count
}

// RangeQuery 执行范围查询，返回指定范围内的所有键值对
func (t *MBTree) RangeSearch(startKey, endKey util.Key) []util.KeyValue {
	result := make([]util.KeyValue, 0)

	// 检查临时节点中的键值对
	if t.tmp != nil {
		for _, kv := range t.tmp.KeyValues {
			if kv.Key >= startKey && kv.Key <= endKey {
				result = append(result, kv)
			}
		}
	}

	// 如果树为空，直接返回
	if t.root == nil {
		return result
	}

	// 找到第一个包含startKey的叶子节点
	current := t.root
	for !current.IsLeaf() {
		internal := current.(*internalNode)

		// 空检查
		if len(internal.Keys) == 0 && len(internal.Children) == 0 {
			return result
		}

		// 找到合适的子节点
		childIndex := 0
		for i := 0; i < len(internal.Keys); i++ {
			if startKey >= internal.Keys[i] {
				childIndex = i + 1
				if childIndex >= len(internal.Children) {
					childIndex = len(internal.Children) - 1
				}
			} else {
				break
			}
		}

		current = internal.Children[childIndex]
	}

	// 从找到的叶子节点开始遍历
	leaf := current.(*leafNode)
	for leaf != nil {
		// 检查当前叶子节点中的键值对
		for _, kv := range leaf.KeyValues {
			if kv.Key >= startKey && kv.Key <= endKey {
				result = append(result, kv)
			}

			// 如果已经超过了endKey，可以提前结束
			if kv.Key > endKey {
				return result
			}
		}

		// 移动到下一个叶子节点
		leaf = leaf.Next
	}

	return result
}

// Proof of a range query, each level consist of a vector of MB-Tree nodes
type level struct {
	StartIdx int
	EndIdx   int
	Node     node
}

type RangeProof struct {
	Levels [][]level
	// 添加额外字段来存储tmp节点或root节点的哈希
	ExtraHash util.H256
	// 标识proof是来自主树还是tmp节点
	FromMainTree bool
}

func (t *MBTree) GenerateRangeProof(startKey, endKey util.Key) ([]util.KeyValue, RangeProof) {
	// 初始化range proof
	proof := RangeProof{
		Levels: make([][]level, 0),
	}

	// 初始化结果数组
	results := make([]util.KeyValue, 0)

	// 首先检查tmp节点
	tmpResults := make([]util.KeyValue, 0)
	if t.tmp != nil {
		for _, kv := range t.tmp.KeyValues {
			if kv.Key >= startKey && kv.Key <= endKey {
				tmpResults = append(tmpResults, kv)
			}
		}
	}

	// 如果在tmp节点中找到了所有结果
	if len(tmpResults) > 0 {
		// 创建tmp节点的证明
		tmpProof := make([]level, 0)

		// 找到tmp节点中符合条件的索引范围
		startIdx, endIdx := 0, 0
		if t.tmp != nil {
			startIdx, endIdx = t.searchProveIdxRange(t.tmp, startKey, endKey)
			tmpProof = append(tmpProof, level{
				StartIdx: startIdx,
				EndIdx:   endIdx,
				Node:     t.tmp,
			})
		}

		proof.Levels = append(proof.Levels, tmpProof)
		proof.FromMainTree = false

		// 如果主树存在，添加主树根节点的哈希
		if t.root != nil {
			proof.ExtraHash = t.root.GetHash()
		}

		return tmpResults, proof
	}

	// 如果树为空，直接返回
	if t.root == nil {
		return []util.KeyValue{}, proof
	}

	// 创建队列来遍历树
	queue := make([]node, 0)
	queue = append(queue, t.root)

	// 计数器帮助确定每层节点数量
	prevCnt := 1
	curCnt := 0

	// 当前层的临时证明
	curLevelProof := make([]level, 0)

	// 遍历树直到队列为空
	for len(queue) > 0 {
		curNode := queue[0]
		queue = queue[1:]
		prevCnt--

		if !curNode.IsLeaf() {
			// 内部节点
			internal := curNode.(*internalNode)

			// 获取子节点的位置范围
			startIdx, endIdx := t.searchProveIdxRange(curNode, startKey, endKey)

			// 更新当前层节点计数
			curCnt += endIdx - startIdx + 1

			// 将当前节点添加到证明中
			curLevelProof = append(curLevelProof, level{
				StartIdx: startIdx,
				EndIdx:   endIdx,
				Node:     curNode,
			})

			// 将对应的子节点添加到队列
			for idx := startIdx; idx <= endIdx; idx++ {
				childNode := internal.Children[idx]
				queue = append(queue, childNode)
			}
		} else {
			// 叶子节点
			leaf := curNode.(*leafNode)

			// 获取叶子节点的位置范围
			startIdx, endIdx := t.searchProveIdxRange(curNode, startKey, endKey)

			// 更新当前层节点计数
			curCnt += endIdx - startIdx + 1

			// 将当前节点添加到证明中
			curLevelProof = append(curLevelProof, level{
				StartIdx: startIdx,
				EndIdx:   endIdx,
				Node:     curNode,
			})

			// 将对应的搜索条目添加到结果中
			for id := startIdx; id <= endIdx; id++ {
				keyValue := leaf.KeyValues[id]
				results = append(results, keyValue)
			}
		}

		if prevCnt == 0 {
			// 如果prevCnt=0，开始新的一层
			prevCnt = curCnt
			curCnt = 0

			// 将当前层的临时证明添加到最终证明中
			proof.Levels = append(proof.Levels, curLevelProof)
			curLevelProof = make([]level, 0)
		}
	}

	// 设置proof来源为主树
	proof.FromMainTree = true

	// 添加tmp节点的哈希
	if t.tmp != nil {
		proof.ExtraHash = t.tmp.GetHash()
	}

	return results, proof
}

// searchProveIdxRange 根据范围查询的边界确定节点中需要遍历的条目范围
func (t *MBTree) searchProveIdxRange(n node, startKey, endKey util.Key) (int, int) {
	if n.IsLeaf() {
		leaf := n.(*leafNode)
		startIdx := 0
		endIdx := len(leaf.KeyValues) - 1

		// 找到第一个大于等于startKey的位置
		for i, kv := range leaf.KeyValues {
			if kv.Key >= startKey {
				startIdx = i
				break
			}
		}

		// 找到最后一个小于等于endKey的位置
		for i := len(leaf.KeyValues) - 1; i >= 0; i-- {
			if leaf.KeyValues[i].Key <= endKey {
				endIdx = i
				break
			}
		}

		return startIdx, endIdx
	} else {
		internal := n.(*internalNode)
		startIdx := 0
		endIdx := len(internal.Children) - 1

		// 找到第一个可能包含startKey的子节点
		for i := 0; i < len(internal.Keys); i++ {
			if startKey <= internal.Keys[i] {
				break
			}
			startIdx = i + 1
		}

		// 找到最后一个可能包含endKey的子节点
		for i := len(internal.Keys) - 1; i >= 0; i-- {
			if endKey >= internal.Keys[i] {
				endIdx = i + 1
				break
			}
		}

		return startIdx, endIdx
	}
}

func (t *MBTree) ReconstructRangeProof(startKey, endKey util.Key, results []util.KeyValue, proof RangeProof) util.H256 {
	// 如果结果为空且证明为空，返回默认哈希
	if len(results) == 0 && len(proof.Levels) == 0 {
		return util.H256{}
	}

	// 验证标志
	validate := true

	// 根据proof来源确定要验证的哈希
	var computeRootHash util.H256

	if proof.FromMainTree {
		// 如果proof来自主树，根哈希是证明第一层的单个节点的摘要
		computeRootHash = proof.Levels[0][0].Node.GetHash()
	} else {
		// 如果proof来自tmp节点，根哈希是tmp节点的摘要
		if len(proof.Levels) > 0 && len(proof.Levels[0]) > 0 {
			computeRootHash = proof.Levels[0][0].Node.GetHash()
		}
	}

	// 临时向量存储下一层的哈希值
	nextLevelHashes := make([]util.H256, 0)

	// 遍历证明中的每一层
	for i, levelProof := range proof.Levels {
		// 检查nextLevelHashes中的哈希值是否与当前层重新计算的哈希值匹配
		if i != 0 {
			computedHashes := make([]util.H256, 0)
			for _, curLevelNode := range levelProof {
				computedHashes = append(computedHashes, curLevelNode.Node.GetHash())
			}

			// 比较哈希值
			if !compareHashes(computedHashes, nextLevelHashes) {
				validate = false
				break
			}

			// 开始新的一层，清除哈希值
			nextLevelHashes = make([]util.H256, 0)
		}

		// 结果向量中的ID
		leafID := 0

		for _, innerProof := range levelProof {
			// 从层证明中检索节点引用
			node := innerProof.Node

			// 从层证明中检索起始和结束位置
			startIdx := innerProof.StartIdx
			endIdx := innerProof.EndIdx

			if !node.IsLeaf() {
				// 节点是内部节点
				internal := node.(*internalNode)

				// 将遍历的子节点的哈希值添加到nextLevelHashes
				for id := startIdx; id <= endIdx; id++ {
					h := internal.ChildHashes[id]
					nextLevelHashes = append(nextLevelHashes, h)
				}
			} else {
				// 节点是叶子节点
				leaf := node.(*leafNode)

				// 检查结果向量中的值与证明中的值
				for i, id := range makeRange(startIdx, endIdx) {
					if id >= len(leaf.KeyValues) {
						validate = false
						break
					}

					keyValue := leaf.KeyValues[id]
					if leafID >= len(results) {
						validate = false
						break
					}

					if i == 0 {
						if results[leafID].Key != keyValue.Key || !bytes.Equal(results[leafID].Value, keyValue.Value) {
							validate = false
							break
						}
					} else {
						k := keyValue.Key
						if k < startKey || k > endKey ||
							results[leafID].Key != keyValue.Key ||
							!bytes.Equal(results[leafID].Value, keyValue.Value) {
							validate = false
							break
						}
					}
					leafID++
				}
			}
		}
	}

	if !validate {
		return util.H256{}
	}

	// 计算整棵树的哈希：hash(rootHash || extraHash)
	var finalHash util.H256
	if proof.FromMainTree {
		// 如果proof来自主树，计算hash(rootHash || tmpHash)
		var b []byte
		b = append(b, computeRootHash[:]...)
		b = append(b, proof.ExtraHash[:]...)
		finalHash = t.hasher.HashBytes(b)
	} else {
		// 如果proof来自tmp节点，计算hash(rootHash || tmpHash)
		var b []byte
		b = append(b, proof.ExtraHash[:]...) // 主树的根哈希
		b = append(b, computeRootHash[:]...) // tmp节点的哈希
		finalHash = t.hasher.HashBytes(b)
	}

	return finalHash
}

// 辅助函数，比较两个哈希数组是否相等
func compareHashes(a, b []util.H256) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// 辅助函数，生成范围内的整数序列
func makeRange(min, max int) []int {
	result := make([]int, max-min+1)
	for i := range result {
		result[i] = min + i
	}
	return result
}
