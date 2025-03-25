package run

import (
	"VLStore/util"
	"math"
	"os"
	"slices"
	"fmt"
)

/*
A helper that writes the hash into a file with a sequence of pages

	According to the disk-optimization objective, the state writing should be in a streaming fashion.
*/
type HashPageWriter struct {
	File                     *os.File    // file object of the corresponding hash file
	HashesInLatestUpdatePage []util.H256 // a preparation vector to obsorb the streaming hashes which are not persisted in the file yet
	NumStoredPages           int         // records the number of pages that are stored in the file
}

/* Initialize the writer using a given file name
 */
func NewHashPageWriter(fileName string) (*HashPageWriter, error) {
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0666)
	if err != nil {
		return nil, err
	}

	return &HashPageWriter{
		File:                     file,
		HashesInLatestUpdatePage: make([]util.H256, 0),
		NumStoredPages:           0,
	}, nil
}

/*
Load the writer from a given file.

	num_stored_pages are derived from the file
*/
func LoadHashPageWriter(fileName string) (*HashPageWriter, error) {
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	numStoredPages := int(fileInfo.Size()) / PAGE_SIZE
	hashesInLatestPage := make([]util.H256, 0)
	if numStoredPages > 0 {
		lastPageOffset := int64((numStoredPages - 1) * PAGE_SIZE)
		// get last page from file
		bytes := make([]byte, PAGE_SIZE)
		_, err := file.ReadAt(bytes, lastPageOffset)
		if err != nil {
			return nil, err
		}
		page := NewPageFromArray([PAGE_SIZE]byte(bytes))
		pageVector := page.ToHashVector()
		// derive number of hashes in the latest page
		numHashesInLatestPage := len(hashesInLatestPage)

		if numHashesInLatestPage < MAX_NUM_HASH_IN_PAGE {
			// the last page is not full, should not be finalized in the file
			hashesInLatestPage = pageVector
			numStoredPages--
		}
	}

	return &HashPageWriter{
		File:                     file,
		HashesInLatestUpdatePage: hashesInLatestPage,
		NumStoredPages:           numStoredPages,
	}, nil
}

/*
Streamingly add the hash to the latest_update_page
Flush the latest_update_page to the file once it is full, and clear it.
*/
func (w *HashPageWriter) Append(hash util.H256) {
	// add the hash
	w.HashesInLatestUpdatePage = append(w.HashesInLatestUpdatePage, hash)
	if len(w.HashesInLatestUpdatePage) == MAX_NUM_HASH_IN_PAGE {
		// vector is full, should be added to a page and flushed to the file
		w.Flush()
	}
}

/*
Flush the latest_update_page to the file
*/
func (w *HashPageWriter) Flush() {
	if len(w.HashesInLatestUpdatePage) != 0 {
		// put the vector into a page
		page := NewPageFromHashVector(w.HashesInLatestUpdatePage)
		// compute the offset at which the page should be written in the file
		offset := int64(w.NumStoredPages * PAGE_SIZE)
		// write the page to the file
		_, err := w.File.WriteAt(page.Data[:], offset)
		if err != nil {
			panic(err)
		}
		// clear the vector
		w.HashesInLatestUpdatePage = make([]util.H256, 0)
		// increment the number of stored pages
		w.NumStoredPages++
	}
}

/*
Transform pager writer to reader
*/
func (w *HashPageWriter) ToHashReader() *HashPageReader {
	file, err := os.OpenFile(w.File.Name(), os.O_RDONLY, 0666)
	if err != nil {
		panic(err)
	}

	fileInfo, err := file.Stat()
	if err != nil {
		panic(err)
	}

	numStoredPages := int(fileInfo.Size()) / PAGE_SIZE
	root := util.H256{}
	if numStoredPages > 0 {
		lastPageId := numStoredPages - 1
		lastPageOffset := int64(lastPageId * PAGE_SIZE)
		// get last page from file
		bytes := make([]byte, PAGE_SIZE)
		_, err := file.ReadAt(bytes, lastPageOffset)
		if err != nil {
			panic(err)
		}
		page := NewPageFromArray([PAGE_SIZE]byte(bytes))
		pageVector := page.ToHashVector()
		root = pageVector[len(pageVector)-1]
	}

	return &HashPageReader{
		File: file,
		Root: root,
	}
}

/*
A helper to read hash from the file
A LRU cache is used to optimize the read performance.
*/
type HashPageReader struct {
	File *os.File  // file object of the corresponding hash file
	Root util.H256 // cache of the root hash of the MHT
}

/*
Load the reader from a given file.
*/
func LoadHashPageReader(fileName string) (*HashPageReader, error) {
	file, err := os.OpenFile(fileName, os.O_RDONLY, 0666)
	if err != nil {
		return nil, err
	}

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	numStoredPages := int(fileInfo.Size()) / PAGE_SIZE
	root := util.H256{}
	if numStoredPages > 0 {
		lastPageId := numStoredPages - 1
		lastPageOffset := int64(lastPageId * PAGE_SIZE)
		// get last page from file
		bytes := make([]byte, PAGE_SIZE)
		_, err := file.ReadAt(bytes, lastPageOffset)
		if err != nil {
			return nil, err
		}
		page := NewPageFromArray([PAGE_SIZE]byte(bytes))
		pageVector := page.ToHashVector()
		root = pageVector[len(pageVector)-1]
	}

	return &HashPageReader{
		File: file,
		Root: root,
	}, nil
}

/*
Load the deserialized vector of the page from the file at given page_id
*/
func (r *HashPageReader) ReadPageAt(pageId int) ([]util.H256, error) {
	//  load the page from the file
	offset := int64(pageId * PAGE_SIZE)
	bytes := make([]byte, PAGE_SIZE)
	_, err := r.File.ReadAt(bytes, offset)
	if err != nil {
		return nil, err
	}
	page := NewPageFromArray([PAGE_SIZE]byte(bytes))
	pageVector := page.ToHashVector()
	return pageVector, nil
}

/*
Generate the non-leaf range proof given the left position l, right position r, and the total number of states in the leaf of the MHT
*/
func (r *HashPageReader) proveNonLeaf(left int, right int, numOfData int, fanout int) *RangeProof {
	proof := &RangeProof{}
	proof.indexList = [2]int{left, right}
	if numOfData == 1 {
		// only one data, just return the empty proof since the data's hash equals the root hash
		return proof
	} else {
		// add non-leaf hash values to the proof
		// compute the size of the current level
		curLevelSize := int(math.Ceil(float64(numOfData) / float64(fanout)))
		// a position that is used to determine the first position of the current level
		startIdx := 0
		// compute the level's left and right position
		levelL := left / fanout
		levelR := right / fanout
		// iteratively add the hash values from the bottom to the top
		for curLevelSize != 1 {
			// compute the boundary of the two positions (i.e. used to generate the left and right hashes of the proved Merkle node to reconstruct the Merkle root)
			// 当前节点的最左/右兄弟节点
			proofPosL := levelL - levelL%fanout
			proofPosR := levelR - levelR%fanout + fanout - 1
			if proofPosR > curLevelSize-1 {
				proofPosR = curLevelSize - 1
			}
			proofPosL = proofPosL + startIdx
			proofPosR = proofPosR + startIdx
			// next, retrieve the hash values from the position (proof_pos_l + start_idx) to (proof_pos_r + start_idx)
			// compute the corresponding page id
			pageIdL := proofPosL / MAX_NUM_HASH_IN_PAGE
			pageIdR := proofPosR / MAX_NUM_HASH_IN_PAGE
			v := make([]util.H256, 0)
			for pageId := pageIdL; pageId <= pageIdR; pageId++ {
				hashes, err := r.ReadPageAt(pageId)
				if err != nil {
					return nil
				}
				v = append(v, hashes...)
			}
			// keep the hashes from proof_pos_l % MAX_NUM_HASH_IN_PAGE to
			leftSlicePos := proofPosL % MAX_NUM_HASH_IN_PAGE
			rightSlicePos := (pageIdR-pageIdL)*MAX_NUM_HASH_IN_PAGE + (proofPosR % MAX_NUM_HASH_IN_PAGE)
			v = v[leftSlicePos : rightSlicePos+1]
			//fmt.Println("v: ", v)
			// remove the proving hashes from index level_l - proof_pos_l to level_r - proof_pos_l
			v = slices.Delete(v, levelL - (proofPosL - startIdx), levelR - (proofPosL - startIdx) + 1)
			//fmt.Println("levelL - (proofPosL - startIdx): ", levelL - (proofPosL - startIdx))
			//fmt.Println("levelR - (proofPosL - startIdx): ", levelR - (proofPosL - startIdx))
			//fmt.Println("v: ", v)
			proof.p = append(proof.p, v)
			levelL /= fanout
			levelR /= fanout
			startIdx += curLevelSize
			curLevelSize = int(math.Ceil(float64(curLevelSize) / float64(fanout)))
		}
		return proof
	}
}

/*
A MHT constructor that generates and appends hashes to the file in a streaming fashion
*/
type StreamMHTConstructor struct {
	OutputMHTWriter *HashPageWriter // the writer of output MHT file
	Fanout          int            // the fanout of the MHT
	CacheVector     []util.H256     // a cache that keep a vector of at most fanout hash values of the level to compute the upper level's hash
	NumOfHash       int             // the total number of hashes in the file
	CntInLevel      int             // a counter for the focused level
	Hasher          util.Hasher     // the hasher
}

/*
Initialize the constructor with a given output file name and fanout
*/
func NewStreamMHTConstructor(fileName string, fanout int, hasher util.Hasher) (*StreamMHTConstructor, error) {
	// create the writer
	writer, err := NewHashPageWriter(fileName)
	if err != nil {
		return nil, err
	}
	// initialize the cache of the level
	cacheVector := make([]util.H256, 0)
	return &StreamMHTConstructor{
		OutputMHTWriter: writer,
		Fanout:          fanout,
		CacheVector:     cacheVector,
		NumOfHash:       0,
		CntInLevel:      0,
		Hasher:          hasher,
	}, nil
}

/*
Streamingly append the hash to the cache of the lowest level
*/
func (c *StreamMHTConstructor) Append(hash util.H256) {
	// append the hash to the cache
	c.CacheVector = append(c.CacheVector, hash)

	if len(c.CacheVector) == c.Fanout {
		// the cache is full, the hash of the concatenated hash values in the cache should be computed and added to the output_mht_writer
		h := computeConcatenateHash(c.CacheVector, c.Hasher)
		// add the hash to the output_mht_writer
		c.OutputMHTWriter.Append(h)
		c.NumOfHash++
		c.CntInLevel++
		// clear the cache
		c.CacheVector = make([]util.H256, 0)
	}
}

func (c *StreamMHTConstructor) FinalizeLevel() {
	if len(c.CacheVector) != 0 {
		// finalize the hash values in the cache
		h := computeConcatenateHash(c.CacheVector, c.Hasher)
		c.OutputMHTWriter.Append(h)
		c.NumOfHash++
		c.CntInLevel++
		c.CacheVector = make([]util.H256, 0)
	}
}

/*
Finalize the append of the state
End the insertion of the lowest cache: finalize the hash, append it to the output hash writer and flush it to the file
Recursively build the MHT upon the lowest level and append them to the file in a streaming fashion.
*/
func (c *StreamMHTConstructor) BuildMHT() {
	c.FinalizeLevel()
	// recursively build the MHT

	// n is the number of hash values of the current input MHT level
	n := c.NumOfHash
	for n != 1 {
		// reset the cnt in the level
		c.CntInLevel = 0
		// the position of the starting input hash value of the current level
		startHashPos := c.NumOfHash - n
		// the position of the ending input hash value of the current level
		endHashPos := c.NumOfHash - 1
		startPageId := startHashPos / MAX_NUM_HASH_IN_PAGE
		endPageId := endHashPos / MAX_NUM_HASH_IN_PAGE
		for pageId := startPageId; pageId <= endPageId; pageId++ {
			pageVector, err := c.ReadPage(pageId)
			if err != nil {
				panic(err)
			}
			if pageId == startPageId {
				pageVector = pageVector[startHashPos%MAX_NUM_HASH_IN_PAGE:]
			} else if pageId == endPageId {
				pageVector = pageVector[:endHashPos%MAX_NUM_HASH_IN_PAGE+1]
			}
			for _, hash := range pageVector {
				c.Append(hash)
			}
		}
		c.FinalizeLevel()
		n = c.CntInLevel
	}
	c.OutputMHTWriter.Flush()
}

/*
Read the page from the file at given page_id
*/
func (c *StreamMHTConstructor) ReadPage(pageId int) ([]util.H256, error) {
	var hashVector []util.H256
	if pageId >= c.OutputMHTWriter.NumStoredPages {
		// page should be read from the in memory cache vector
		hashVector = c.OutputMHTWriter.HashesInLatestUpdatePage
	} else {
		offset := int64(pageId * PAGE_SIZE)
		bytes := make([]byte, PAGE_SIZE)
		_, err := c.OutputMHTWriter.File.ReadAt(bytes, offset)
		if err != nil {
			return nil, err
		}
		page := NewPageFromArray([PAGE_SIZE]byte(bytes))
		hashVector = page.ToHashVector()
	}
	return hashVector, nil
}

/*
Compute the hash of the concatenated hash values in the cache
*/
func computeConcatenateHash(hashes []util.H256, hasher util.Hasher) util.H256 {
	bytes := make([]byte, 0)
	for _, hash := range hashes {
		bytes = append(bytes, hash[:]...)
	}
	return hasher.HashBytes(bytes)
}

/*
A range proof for a given hash value in the MHT
*/
type RangeProof struct {
	indexList [2]int        // include left and right position
	p         [][]util.H256 // the proof path
}

func ReconstructRangeProof(proof *RangeProof, fanout int, objHashes []util.H256, hasher util.Hasher) util.H256 {
	// 打印proof.indexList
	fmt.Println("proof: ", proof)
	fmt.Println("proof.indexList: ", proof.indexList)
	l := proof.indexList[0]
	r := proof.indexList[1]

	indexList := make([]int, r-l+1)
	for i := l; i <= r; i++ {
		indexList[i-l] = i
	}

	if len(indexList) == 1 && indexList[0] == 0 && len(proof.p) == 0 {
		return objHashes[0]
	}

	insertedHashes := make([]util.H256, len(objHashes))
	copy(insertedHashes, objHashes)

	for _, v := range proof.p {
		// copy v to elem
		elem := make([]util.H256, len(v))
		copy(elem, v)

		offset := indexList[0] % fanout
		for i := 0; i < len(indexList); i++ {
			// 在i+offset位置插入insertedHashes[i]
			elem = slices.Insert(elem, i+offset, insertedHashes[i])
		}
		insertedHashes = make([]util.H256, 0)
		// hash_seg: recomputed hash number for the current level
		hashSeg := int(math.Ceil(float64(len(elem)) / float64(fanout)))

		for j := 0; j < hashSeg; j++ {
			startIdx := j * fanout
			var endIdx int
			if startIdx+fanout > len(elem) {
				endIdx = len(elem) - 1
			} else {
				endIdx = startIdx + fanout - 1
			}
			subHashVec := elem[startIdx : endIdx+1]
			h := computeConcatenateHash(subHashVec, hasher)
			insertedHashes = append(insertedHashes, h)
		}

		for i:= 0; i < len(indexList); i++ {
			indexList[i] /= fanout
		}

		// remove duplicates
		indexList = slices.Compact(indexList)
	}
	//fmt.Println("len(insertedHashes): ", len(insertedHashes))
	//fmt.Println("insertedHashes: ", insertedHashes)
	if len(insertedHashes) != 1 {
		panic("Expected exactly one hash in the final result")
	}

	return insertedHashes[0]
}
