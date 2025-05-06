package run

import (
	"VLStore/util"
	"os"
)


type KeyPos struct {
	Key   util.Key
	Pos   *ChunkPosition
}

/*
A helper that writes the key-offset pairs into a file with a sequence of pages
According to the disk-optimization objective, the value writing should be in a streaming fashion.
*/
type KeyPageWriter struct {
	File 						 *os.File          // file object of the corresponding key-offset file
	KeyPosInLatestUpdatePage []KeyPos // a preparation vector to obsorb the streaming key-offsets which are not persisted in the file yet
	NumStoredPages 				 int              // records the number of pages that are stored in the file
	NumKeyPos				 int              // records the number of key-offsets in the file
}

/* Initialize the writer using a given file name
 */
func NewKeyPageWriter(fileName string) (*KeyPageWriter, error) {
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0666)
	if err != nil {
		return nil, err
	}

	return &KeyPageWriter{
		File: file,
		KeyPosInLatestUpdatePage: make([]KeyPos, 0),
		NumStoredPages: 0,
		NumKeyPos: 0,
	}, nil
}

/*
Load the writer from a given file
num_stored_pages are derived from the file
*/
func LoadKeyPageWriter(fileName string) (*KeyPageWriter, error) {
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	numStoredPages := int(fileInfo.Size()) / PAGE_SIZE
	keyPosInLatestPage := make([]KeyPos, 0)
	numKeyPos := 0
	if numStoredPages > 0 {
		lastPageOffset := int64((numStoredPages - 1) * PAGE_SIZE)
		// get last page from file
		bytes := make([]byte, PAGE_SIZE)
		_, err = file.ReadAt(bytes, lastPageOffset)
		if err != nil {
			return nil, err
		}
		page := NewPageFromArray([PAGE_SIZE]byte(bytes))
		pageKeyPos := page.ToKeyPosVector()
		// derive number of key-offsets in the last page
		numKeyPosInLastPage := len(pageKeyPos)
		// derive the number of key-offsets in the file
		numKeyPos = (numStoredPages - 1) * MAX_NUM_KEY_POS_IN_PAGE + numKeyPosInLastPage
		
		if numKeyPosInLastPage < MAX_NUM_KEY_POS_IN_PAGE {
			// the last page is not full, should not be finalized in the file
			keyPosInLatestPage = pageKeyPos
			numStoredPages--
		}
	}

	return &KeyPageWriter{
		File: file,
		KeyPosInLatestUpdatePage: keyPosInLatestPage,
		NumStoredPages: numStoredPages,
		NumKeyPos: numKeyPos,
	}, nil
}

/*
Streamingly add the key-offset to the latest_update_page
Flush the latest_update_page to the file once it is full, and clear it.
*/
func (w *KeyPageWriter) Append(keyPos KeyPos) {
	w.KeyPosInLatestUpdatePage = append(w.KeyPosInLatestUpdatePage, keyPos)
	if len(w.KeyPosInLatestUpdatePage) == MAX_NUM_KEY_POS_IN_PAGE {
		// vector is full, should be added to a page and flushed to the file
		w.Flush()
	}
	w.NumKeyPos++
}

/* Flush the vector in latest update page to the last page in the value file
 */
func (w *KeyPageWriter) Flush() {
	if len(w.KeyPosInLatestUpdatePage) > 0 {
		// put the vector into a page
		page := NewPageFromKeyPosVector(w.KeyPosInLatestUpdatePage)
		// compute the offset at which the page should be written in the file
		offset := int64(w.NumStoredPages * PAGE_SIZE)
		// write the page to the file
		_, err := w.File.WriteAt(page.Data[:], offset)
		if err != nil {
			panic(err)
		}
		// clear the vector
		w.KeyPosInLatestUpdatePage = make([]KeyPos, 0)
		// increment the number of stored pages
		w.NumStoredPages++
	}
}

/* Transform pager writer to pager reader
 */
func (w *KeyPageWriter) ToKeyPageReader() *KeyPageReader {
	return &KeyPageReader{
		File: w.File,
		NumKeyPos: w.NumKeyPos,
	}
}
	
/*
A helper to readthe key-offset pairs from a file
*/
type KeyPageReader struct {
	File *os.File     // file object of the corresponding key-offset file
	NumKeyPos int // the number of key-pos in the file
}

/*
Load the reader from a given file
num_key_offsets are derived from the file
*/
func LoadKeyPageReader(fileName string) (*KeyPageReader, error) {
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}
	
	numStoredPages := int(fileInfo.Size()) / PAGE_SIZE
	numKeyPos := 0
	if numStoredPages > 0 {
		lastPageOffset := (numStoredPages - 1) * PAGE_SIZE
		// get the last page from the file
		bytes := make([]byte, PAGE_SIZE)
		_, err = file.ReadAt(bytes, int64(lastPageOffset))
		if err != nil {
			return nil, err
		}
		page := NewPageFromArray([PAGE_SIZE]byte(bytes))
		pageKeyPos := page.ToKeyPosVector()
		// derive the number of key-pos in the last page
		numKeyPosInLastPage := len(pageKeyPos)
		// derive the number of key-pos in the file
		numKeyPos = (numStoredPages - 1) * MAX_NUM_KEY_POS_IN_PAGE + numKeyPosInLastPage
	}

	return &KeyPageReader{
		File: file,
		NumKeyPos: numKeyPos,
	}, nil
}

/*
Load the deserialized vector of the page from the file at given page_id
*/
func (r *KeyPageReader) ReadDeserPageAt(pageID int) ([]KeyPos, error) {
	// load page from the file
	offset := pageID * PAGE_SIZE
	bytes := make([]byte, PAGE_SIZE)
	_, err := r.File.ReadAt(bytes, int64(offset))
	if err != nil {
		return nil, err
	}
	
	page := NewPageFromArray([PAGE_SIZE]byte(bytes))
	keyPos := page.ToKeyPosVector()

	return keyPos, nil
}

/* 
* Load the deserialized vector given the key's location range
*/
func (r *KeyPageReader) ReadDeserPageRange(posL int, posR int) ([]KeyPos, error) {
	pageIDL := posL / MAX_NUM_KEY_POS_IN_PAGE
	pageIDR := posR / MAX_NUM_KEY_POS_IN_PAGE

	keyPos := make([]KeyPos, 0)
	for pageID := pageIDL; pageID <= pageIDR; pageID++ {
		keyPosInPage, err := r.ReadDeserPageAt(pageID)
		if err != nil {
			return nil, err
		}
		keyPos = append(keyPos, keyPosInPage...)
	}

	leftPos := posL % MAX_NUM_KEY_POS_IN_PAGE
	rightPos := (pageIDR - pageIDL) * MAX_NUM_KEY_POS_IN_PAGE + (posR % MAX_NUM_KEY_POS_IN_PAGE)
	
	keyPos = keyPos[leftPos:rightPos + 1]
	
	return keyPos, nil
}


