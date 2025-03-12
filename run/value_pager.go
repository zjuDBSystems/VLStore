package run

import (
	"VLStore/util"
	"os"
)

/*
A helper that writes the key-value pairs into a file with a sequence of pages
According to the disk-optimization objective, the value writing should be in a streaming fashion.
*/
type ValuePageWriter struct {
	File                            *os.File        // file object of the corresponding value file
	KeyValuesInLatestUpdatePage     []util.KeyValue // a preparation vector to obsorb the streaming key-values which are not persisted in the file yet
	SizeKeyValuesInLatestUpdatePage int             // the size of the key-values in the latest update page
	NumStoredPages                  int             //records the number of pages that are stored in the file
	NumKeyValues                    int             //records the number of key-values
}

/* Initialize the writer using a given file name
 */
func NewValuePageWriter(fileName string) (*ValuePageWriter, error) {
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0666)
	if err != nil {
		return nil, err
	}

	return &ValuePageWriter{
		File:                            file,
		KeyValuesInLatestUpdatePage:     make([]util.KeyValue, 0),
		SizeKeyValuesInLatestUpdatePage: 0,
		NumStoredPages:                  0,
		NumKeyValues:                    0,
	}, nil
}

/*
Load the writer from a given file

	num_key_values and num_stored_pages are derived from the file
*/
func LoadValuePageWriter(fileName string) (*ValuePageWriter, error) {
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	numStoredPages := int(fileInfo.Size()) / PAGE_SIZE
	keyValuesInLatestPage := make([]util.KeyValue, 0)
	numKeyValues := 0
	sizeKeyValuesInLatestPage := 0
	if numStoredPages > 0 {
		lastPageOffset := int64((numStoredPages - 1) * PAGE_SIZE)

		//  get last page from file
		bytes := make([]byte, PAGE_SIZE)
		_, err = file.ReadAt(bytes, lastPageOffset)
		if err != nil {
			return nil, err
		}

		page := NewPageFromArray([PAGE_SIZE]byte(bytes))
		pageKeyValues := page.ToKeyValueVector()

		// derive number of key-values in the last page
		numKeyValuesInLastPage := len(pageKeyValues)

		// derive number of key-values
		var numKeyValues int
		if numStoredPages > 1 {
			// read previous pages to calculate total number of key-values
			for i := 0; i < numStoredPages-1; i++ {
				pageOffset := int64(i * PAGE_SIZE)
				_, err = file.ReadAt(bytes, pageOffset)
				if err != nil {
					return nil, err
				}

				currentPage := NewPageFromArray([PAGE_SIZE]byte(bytes))
				numKeyValues += len(currentPage.ToKeyValueVector())
			}
		}
		numKeyValues += numKeyValuesInLastPage

		sizeKeyValuesInLatestPage += 4
		for _, kv := range pageKeyValues {
			sizeKeyValuesInLatestPage += len(kv.Value) + 12 // 8 bytes for key, 4 bytes for value length
		}

		if sizeKeyValuesInLatestPage < PAGE_SIZE {
			// the last page is not full, should not be finalized in the file
			keyValuesInLatestPage = pageKeyValues
			numStoredPages--
		}

	}

	return &ValuePageWriter{
		File:                            file,
		KeyValuesInLatestUpdatePage:     keyValuesInLatestPage,
		SizeKeyValuesInLatestUpdatePage: sizeKeyValuesInLatestPage,
		NumStoredPages:                  numStoredPages,
		NumKeyValues:                    numKeyValues,
	}, nil
}

/*
Streamingly add the key-value to the latest_update_page
Flush the latest_update_page to the file once it is full, and clear it.
*/
func (w *ValuePageWriter) Append(key util.Key, value util.Value) {
	// check if adding this key-value will exceed the page size
	sizeKeyValuesInLatestPage := w.SizeKeyValuesInLatestUpdatePage
	if sizeKeyValuesInLatestPage+len(value)+12 > PAGE_SIZE { // 8 bytes for key, 4 bytes for value length
		// if adding this key-value will exceed the page size, flush the current page
		w.Flush()
	}

	kv := util.KeyValue{Key: key, Value: value}
	w.KeyValuesInLatestUpdatePage = append(w.KeyValuesInLatestUpdatePage, kv)
	w.SizeKeyValuesInLatestUpdatePage += len(value) + 12 // 8 bytes for key, 4 bytes for value length
	w.NumKeyValues++
}

/* Flush the vector in latest update page to the last page in the value file
 */
func (w *ValuePageWriter) Flush() {
	if len(w.KeyValuesInLatestUpdatePage) > 0 {
		// check if the last update page is exceeding the page size
		if w.SizeKeyValuesInLatestUpdatePage > PAGE_SIZE {
			panic("the size latest update page exceeds the page size")
		}

		// convert the key-value vector to a page
		page := NewPageFromKeyValueVector(w.KeyValuesInLatestUpdatePage)

		// compute the offset at which the page should be written in the file
		offset := int64(w.NumStoredPages * PAGE_SIZE)

		// write the page to the file
		_, err := w.File.WriteAt(page.Data[:], offset)
		if err != nil {
			panic(err)
		}

		// clear the key-value vector
		w.KeyValuesInLatestUpdatePage = make([]util.KeyValue, 0)
		w.SizeKeyValuesInLatestUpdatePage = 0
		w.NumStoredPages++
	}
}

/* Transform pager writer to pager reader
 */
func (w *ValuePageWriter) ToValueReader() *ValuePageReader {
	numKeyValues := w.NumKeyValues
	file := w.File

	return &ValuePageReader{
		File:         file,
		NumKeyValues: numKeyValues,
	}
}

/* Transform pager writer to iterator for preparing file merge
 */
func (w *ValuePageWriter) ToKeyValueIterator() *KeyValueIterator {
	numKeyValues := w.NumKeyValues

	return &KeyValueIterator{
		File:             w.File,
		CurPageKeyValues: make([]util.KeyValue, 0),
		CurKeyValuePos:   0,
		NumKeyValues:     numKeyValues,
	}
}

/*
A helper to read key-value from the file

	A LRU cache is used to optimize the read performance.
*/
type ValuePageReader struct {
	File         *os.File // file object of the corresponding value file
	NumKeyValues int      // the number of key-values in the file
}

/*
Load the reader from a given file.

	num_key_values and num_stored_pages are derived from the file
*/
func LoadValuePageReader(fileName string) (*ValuePageReader, error) {
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	numStoredPages := int(fileInfo.Size()) / PAGE_SIZE
	numKeyValues := 0

	if numStoredPages > 0 {
		// read all pages to calculate the total number of key-values
		bytes := make([]byte, PAGE_SIZE)
		for i := 0; i < numStoredPages; i++ {
			pageOffset := int64(i * PAGE_SIZE)
			_, err = file.ReadAt(bytes, pageOffset)
			if err != nil {
				return nil, err
			}

			page := NewPageFromArray([PAGE_SIZE]byte(bytes))
			numKeyValues += len(page.ToKeyValueVector())
		}
	}

	return &ValuePageReader{
		File:         file,
		NumKeyValues: numKeyValues,
	}, nil
}

/* Load the deserialized vector of the page from the file at given page_id
 */
func (r *ValuePageReader) ReadPageAt(pageID int) []util.KeyValue {
	// cache does not contain the page, should load the page from the file
	offset := int64(pageID * PAGE_SIZE)
	bytes := make([]byte, PAGE_SIZE)
	_, err := r.File.ReadAt(bytes, offset)
	if err != nil {
		panic(err)
	}

	page := NewPageFromArray([PAGE_SIZE]byte(bytes))
	keyValues := page.ToKeyValueVector()

	return keyValues
}

/* Load the deserialized vector given the key-value's location range
 */
func (r *ValuePageReader) ReadKeyValuesRange(posL, posR int) []util.KeyValue {
	if posL > posR || posL < 0 || posR > r.NumKeyValues {
		return nil
	}

	result := make([]util.KeyValue, 0, posR-posL+1)
	currentPos := 0
	currentPage := 0

	// traverse the pages until finding the page containing posL
	for {
		keyValues := r.ReadPageAt(currentPage)
		if currentPos+len(keyValues) > posL {
			break
		}
		currentPos += len(keyValues)
		currentPage++
	}

	// collect all key-values in the range
	for currentPos <= posR {
		keyValues := r.ReadPageAt(currentPage)
		for i := 0; i < len(keyValues) && currentPos <= posR; i++ {
			if currentPos >= posL {
				result = append(result, keyValues[i])
			}
			currentPos++
		}
		currentPage++

		// if all pages have been read, exit the loop
		if int64(currentPage*PAGE_SIZE) >= getFileSize(r.File) {
			break
		}
	}

	return result
}

/* Get the size of the file
 */
func getFileSize(f *os.File) int64 {
	info, err := f.Stat()
	if err != nil {
		return 0
	}
	return info.Size()
}

/* Transform pager reader to iterator for preparing file merge, destory the LRU reader instance after transformation
 */
func (r *ValuePageReader) ToKeyValueIterator() *KeyValueIterator {
	numKeyValues := r.NumKeyValues

	return &KeyValueIterator{
		File:             r.File,
		CurPageKeyValues: make([]util.KeyValue, 0),
		CurKeyValuePos:   0,
		NumKeyValues:     numKeyValues,
	}
}

/* Iterator of key-value vector in memory
 */
type InMemKeyValueIterator struct {
	KeyValues   []util.KeyValue // the key-values in memory
	CurValuePos int             // the position of the current value
}

func NewInMemKeyValueIterator(keyValues []util.KeyValue) *InMemKeyValueIterator {
	return &InMemKeyValueIterator{
		KeyValues:   keyValues,
		CurValuePos: 0,
	}
}

func (it *InMemKeyValueIterator) HasNext() bool {
	return it.CurValuePos < len(it.KeyValues)
}

func (it *InMemKeyValueIterator) Next() util.KeyValue {
	keyValue := it.KeyValues[it.CurValuePos]
	it.CurValuePos++
	return keyValue
}

/*
Iterator of a key-value file

	Use a cached vector of page to fetch the key-value one-by-one in a streaming fashion.
	Note that the key-value should be read from the file in a 'Page' unit.
*/
type KeyValueIterator struct {
	File             *os.File
	CurPageKeyValues []util.KeyValue
	CurKeyValuePos   int
	NumKeyValues     int
	CurPage          int
	PosInPage        int
}

/* Create a new key-value iterator by given the file handler and the number of key-values
 */
func CreateKeyValueIteratorWithNumKeyValues(file *os.File, numKeyValues int) *KeyValueIterator {
	return &KeyValueIterator{
		File:             file,
		CurPageKeyValues: make([]util.KeyValue, 0),
		CurKeyValuePos:   0,
		NumKeyValues:     numKeyValues,
		CurPage:          0,
		PosInPage:        0,
	}
}

/*
Create a new key-value iterator by given the file handler.

	The num_key_values is derived from the file (should load the last page to determine the number of key-values).
*/
func CreateKeyValueIterator(file *os.File) (*KeyValueIterator, error) {
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	numStoredPages := int(fileInfo.Size()) / PAGE_SIZE
	numKeyValues := 0

	if numStoredPages > 0 {
		// read all pages to calculate the total number of key-values
		bytes := make([]byte, PAGE_SIZE)
		for i := 0; i < numStoredPages; i++ {
			pageOffset := int64(i * PAGE_SIZE)
			_, err = file.ReadAt(bytes, pageOffset)
			if err != nil {
				return nil, err
			}

			page := NewPageFromArray([PAGE_SIZE]byte(bytes))
			numKeyValues += len(page.ToKeyValueVector())
		}
	}

	return &KeyValueIterator{
		File:             file,
		CurPageKeyValues: make([]util.KeyValue, 0),
		CurKeyValuePos:   0,
		NumKeyValues:     numKeyValues,
		CurPage:          0,
		PosInPage:        0,
	}, nil
}

/* Check if there is a next key-value
 */
func (it *KeyValueIterator) HasNext() bool {
	return it.CurKeyValuePos < it.NumKeyValues
}

/* Get the next key-value
 */
func (it *KeyValueIterator) Next() util.KeyValue {
	if it.CurKeyValuePos >= it.NumKeyValues {
		return util.KeyValue{}
	}

	// the current page has been traversed or the first call of Next()
	if it.PosInPage >= len(it.CurPageKeyValues) {
		// should load a new page from the file
		bytes := make([]byte, PAGE_SIZE)
		offset := int64(it.CurPage * PAGE_SIZE)

		_, err := it.File.ReadAt(bytes, offset)
		if err != nil {
			panic(err)
		}

		page := NewPageFromArray([PAGE_SIZE]byte(bytes))
		it.CurPageKeyValues = page.ToKeyValueVector()
		it.PosInPage = 0
		it.CurPage++
	}

	// get the current key-value
	keyValue := it.CurPageKeyValues[it.PosInPage]

	// update the position
	it.PosInPage++
	it.CurKeyValuePos++

	return keyValue
}
