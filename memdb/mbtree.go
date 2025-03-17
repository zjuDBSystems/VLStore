package memdb

import (
	"VLStore/util"
	"bytes"
	"fmt"
)

type MBTree struct {
	root   node
	keyNum int
	fanout int
	tmp    *leafNode
	last   *leafNode
	hasher util.Hasher
}

func (t *MBTree) KeyNum() int {
	return t.keyNum
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

// 获取树中叶子节点所有键值对
func (t *MBTree) LoadAllKeyValues() []util.KeyValue {
	result := make([]util.KeyValue, 0)

	// 如果根节点为空，只返回临时节点中的键值对
	if t.root == nil {
		if t.tmp != nil {
			result = append(result, t.tmp.KeyValues...)
		}
		return result
	}

	// 找到最左边的叶子节点
	current := t.root
	for !current.IsLeaf() {
		current = current.(*internalNode).Children[0]
	}

	// 遍历叶子节点
	for current != nil {
		currentLeaf := current.(*leafNode)
		if currentLeaf == nil {
			break
		}
		result = append(result, currentLeaf.KeyValues...)
		current = currentLeaf.Next
	}

	// 获取临时节点中的键值对
	if t.tmp != nil {
		result = append(result, t.tmp.KeyValues...)
	}

	return result
}

// 单点查询，返回指定键的值
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

// 范围查询，返回指定范围内的所有键值对
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

// 插入键值对
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
		//t.last.ComputeHash(t.hasher) // 更新哈希
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
	if len(leftParent.Children) < t.fanout {
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
		leftGrandParent = leftParent.Parent
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
			oldRoot.ParentId = 0
			internal.Parent = newRoot
			internal.ParentId = 1

			t.root = newRoot
			return
		}
	}

	// 2. 判断左兄弟节点的父节点是否已满
	if len(leftParent.Children) < t.fanout {
		// 父节点未满，直接追加
		leftParent.Keys = append(leftParent.Keys, key)
		leftParent.Children = append(leftParent.Children, internal)
		leftParent.ChildHashes = append(leftParent.ChildHashes, internal.Hash)
		internal.Parent = leftParent
		internal.ParentId = len(leftParent.Children) - 1
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
		internal.ParentId = 0
		// 递归处理上层
		var leftGrandParent *internalNode = nil
		leftGrandParent = leftParent.Parent
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

// 节点接口定义
type node interface {
	IsLeaf() bool
	GetHash() util.H256
	ComputeHash(hasher util.Hasher)
	SearchProveIdxRange(startKey, endKey util.Key) (int, int)
}

// 内部节点实现
type internalNode struct {
	Parent      *internalNode
	ParentId    int
	Keys        []util.Key
	Children    []node
	ChildHashes []util.H256
	Hash        util.H256
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

	// recursively update the hash of its parent node
	if n.Parent != nil {
		n.Parent.ChildHashes[n.ParentId] = n.Hash
		n.Parent.ComputeHash(hasher)
	}

}

// search the index range of the searched key range in the internal node
func (n *internalNode) SearchProveIdxRange(startKey, endKey util.Key) (int, int) {
	i := 0
	num := len(n.Keys)

	for i < num {
		if startKey < n.Keys[i] {
			break
		}
		i++
	}

	j := 0
	for j < num {
		if endKey < n.Keys[j] {
			break
		}
		j++
	}

	return i, j
}

// 叶子节点实现
type leafNode struct {
	Parent    *internalNode
	KeyValues []util.KeyValue
	Next      *leafNode
	Hash      util.H256
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

func (n *leafNode) SearchProveIdxRange(startKey, endKey util.Key) (int, int) {
	i := 0
	num := len(n.KeyValues)

	for i < num-1 {
		// if key is smaller or equal to the first key in the leaf node, index is 0
		if startKey <= n.KeyValues[0].Key {
			break
		} else if startKey >= n.KeyValues[i].Key && startKey < n.KeyValues[i+1].Key {
			// if key[i] <= key < key[i+1], index is i
			break
		} else {
			// otherwise, increment i until i == n - 1, which is the last index of the leaf node
			i++
		}
	}

	j := 0
	for j < num-1 {
		if endKey <= n.KeyValues[0].Key {
			break
		} else if endKey >= n.KeyValues[j].Key && endKey < n.KeyValues[j+1].Key {
			break
		} else {
			j++
		}
	}

	return i, j
}

// Proof of a range query, each level consist of a vector of MB-Tree nodes
type RangeProof struct {
	Levels [][]level
}

// store the start_idx and end_idx of the searched entries in the node
type level struct {
	StartIdx int
	EndIdx   int
	Node     node
}

func (t *MBTree) GenerateRangeProof(startKey, endKey util.Key) ([]util.KeyValue, RangeProof) {
	// init a range proof
	proof := RangeProof{
		Levels: make([][]level, 0),
	}

	// init a result set
	results := make([]util.KeyValue, 0)

	// if there is a tmp node, commit it to the main tree
	if t.tmp != nil && len(t.tmp.KeyValues) > 0 {
		t.commitTmpNode()
	}

	// if main tree is empty
	if t.root == nil {
		return results, proof
	}

	// create a queue to help traverse the tree
	queue := make([]node, 0)
	queue = append(queue, t.root)

	// some counter to help determine the number of nodes in the level
	prevCount := 1
	curCount := 0

	// a temporary proof for the current level
	curLevelProof := make([]level, 0)

	// traverse the tree in a while loop until the queue is empty
	for len(queue) > 0 {
		curNode := queue[0]
		queue = queue[1:]
		// decrease the node counter of the previous level
		prevCount--

		if !curNode.IsLeaf() {
			// the node is an internal node, retrieve the reference of the internal node
			internal := curNode.(*internalNode)
			// given the start and end, get the position range of the child nodes
			startIdx, endIdx := internal.SearchProveIdxRange(startKey, endKey)
			// update the node counter for the level
			curCount += endIdx - startIdx + 1
			// add the cur_node to the proof as well as the starting and ending position of the traversed entries
			curLevelProof = append(curLevelProof, level{
				StartIdx: startIdx,
				EndIdx:   endIdx,
				Node:     curNode,
			})
			// add the corresponding child nodes to the queue
			for i := startIdx; i <= endIdx; i++ {
				queue = append(queue, internal.Children[i])
			}
		} else {
			// the node is a leaf node, retrieve the reference of the leaf node
			leaf := curNode.(*leafNode)
			// get the position range of the leaf node
			startIdx, endIdx := leaf.SearchProveIdxRange(startKey, endKey)
			// update the node counter for the level
			curCount += endIdx - startIdx + 1
			// add the cur_node to the proof as well as the starting and ending position of the traversed entries
			curLevelProof = append(curLevelProof, level{
				StartIdx: startIdx,
				EndIdx:   endIdx,
				Node:     curNode,
			})
			// add the corresponding searched entries to the value_vec
			for i := startIdx; i <= endIdx; i++ {
				results = append(results, leaf.KeyValues[i])
			}
		}

		// if the previous level is traversed, add the proof to the final proof
		if prevCount == 0 {
			prevCount = curCount
			curCount = 0

			// add the proof of the current level to the final proof
			proof.Levels = append(proof.Levels, curLevelProof)
			// clear the temporary proof for the current level
			curLevelProof = make([]level, 0)
		}
	}
	return results, proof
}

func (t *MBTree) ReconstructRangeProof(startKey, endKey util.Key, results []util.KeyValue, proof RangeProof) util.H256 {
	// if the result is empty and the proof is empty, return the default hash
	if len(results) == 0 && len(proof.Levels) == 0 {
		return util.H256{}
	}
	// a flag to determine whethere there is an verification error
	validate := true
	// the root hash from the proof should be the first level's single node's digest
	computeRootHash := proof.Levels[0][0].Node.GetHash()
	// a temporary vector to store the hash values of the next level
	nextLevelHashes := make([]util.H256, 0)
	// iterate each of the levels in the proof
	for i, levelProof := range proof.Levels {
		// check whether the hash valeus in the next_level_hashes vector (constructed during the prevous level) match the re-computed one for the current level or not
		if i != 0 {
			computedHashes := make([]util.H256, 0)
			for _, curLevelNode := range levelProof {
				computedHashes = append(computedHashes, curLevelNode.Node.GetHash())
			}
			//fmt.Println("computedHashes", computedHashes)
			//fmt.Println("nextLevelHashes", nextLevelHashes)
			if !compareHashes(computedHashes, nextLevelHashes) {
				// not match
				validate = false
				break
			}

			// start another level by clearing the hashes
			nextLevelHashes = nextLevelHashes[:0]
		}

		// id of the result in the vector of the proof
		leafID := 0
		for _, innerProof := range levelProof {
			// retrieve the node from the level proof
			node := innerProof.Node
			// retrieve the start and end positions from the level proof
			startIdx := innerProof.StartIdx
			endIdx := innerProof.EndIdx
			if !node.IsLeaf() {
				/// node is an internal node
				internal := node.(*internalNode)
				// add the hash values of the traversed child nodes to the next_level_hashes
				for id := startIdx; id <= endIdx; id++ {
					h := internal.ChildHashes[id]
					nextLevelHashes = append(nextLevelHashes, h)
				}
			} else {
				// node is a leaf node
				leaf := node.(*leafNode)
				// check the values in the result vector against the values in the proof
				for i := startIdx; i <= endIdx; i++ {
					keyValue := leaf.KeyValues[i]
					if i == startIdx {
						if results[leafID].Key != keyValue.Key || !bytes.Equal(results[leafID].Value, keyValue.Value) {
							validate = false
							break
						}
					} else {
						k := keyValue.Key
						if k < startKey || k > endKey || results[leafID].Key != keyValue.Key || !bytes.Equal(results[leafID].Value, keyValue.Value) {
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

	return computeRootHash
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
