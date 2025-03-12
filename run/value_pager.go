package run

import (
	"VLStore/util"
	"os"
)

/* A helper that writes the value into a file with a sequence of pages
   According to the disk-optimization objective, the value writing should be in a streaming fashion.
 */
type ValuePageWriter struct {
	File               *os.File // file object of the corresponding value file
	ValuesInLatestUpdatePage []util.Value // a preparation vector to obsorb the streaming values which are not persisted in the file yet
	SizeValuesInLatestUpdatePage int // the size of the values in the latest update page
	NumStoredPages     int //records the number of pages that are stored in the file
	NumValues          int //records the number of values
}

 /* Initialize the writer using a given file name
  */
func NewValuePageWriter(fileName string) (*ValuePageWriter, error) {
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0666)
	if err != nil {
		return nil, err
	}

	return &ValuePageWriter{
		File:               file,
		ValuesInLatestUpdatePage: make([]util.Value, 0),
		SizeValuesInLatestUpdatePage: 0,
		NumStoredPages:     0,
		NumValues:          0,
	}, nil
}

/* Load the writer from a given file
    num_values and num_stored_pages are derived from the file
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
	valuesInLatestPage := make([]util.Value, 0)
	numValues := 0
	sizeValuesInLatestPage := 0
	if numStoredPages > 0 {
		lastPageOffset := int64((numStoredPages - 1) * PAGE_SIZE)

		//  get last page from file
		bytes := make([]byte, PAGE_SIZE)
		_, err = file.ReadAt(bytes, lastPageOffset)
		if err != nil {
			return nil, err
		}

		page := NewPageFromArray([PAGE_SIZE]byte(bytes))
		pageValues := page.ToValueVector()

		// derive number of values in the last page
		numValuesInLastPage := len(pageValues)

		// derive number of values 
		var numValues int
		if numStoredPages > 1 {
			// read previous pages to calculate total number of values
			for i := 0; i < numStoredPages-1; i++ {
				pageOffset := int64(i * PAGE_SIZE)
				_, err = file.ReadAt(bytes, pageOffset)
				if err != nil {
					return nil, err
				}

				currentPage := NewPageFromArray([PAGE_SIZE]byte(bytes))
				numValues += len(currentPage.ToValueVector())
			}
		}
		numValues += numValuesInLastPage


		sizeValuesInLatestPage += 4
		for _, value := range pageValues {
			sizeValuesInLatestPage += len(value) + 4
		}

		if sizeValuesInLatestPage < PAGE_SIZE {
			// the last page is not full, should not be finalized in the file
			valuesInLatestPage = pageValues
			numStoredPages--
		}
		
	}

	return &ValuePageWriter{
		File:               file,
		ValuesInLatestUpdatePage: valuesInLatestPage,
		SizeValuesInLatestUpdatePage: sizeValuesInLatestPage,
		NumStoredPages:     numStoredPages,
		NumValues:          numValues,
	}, nil
}

/* Streamingly add the state to the latest_update_page
   Flush the latest_update_page to the file once it is full, and clear it.
 */
func (w *ValuePageWriter) Append(value util.Value) {
	// check if adding this value will exceed the page size
	sizeValuesInLatestPage := w.SizeValuesInLatestUpdatePage
	if sizeValuesInLatestPage + len(value) + 4 > PAGE_SIZE {
		// if adding this value will exceed the page size, flush the current page
		w.Flush()
	}

	w.ValuesInLatestUpdatePage = append(w.ValuesInLatestUpdatePage, value)
	w.SizeValuesInLatestUpdatePage += len(value) + 4
	w.NumValues++
}

/* Flush the vector in latest update page to the last page in the value file
 */
func (w *ValuePageWriter) Flush() {
	if len(w.ValuesInLatestUpdatePage) > 0 {
		// check if the last update page is exceeding the page size
		if w.SizeValuesInLatestUpdatePage > PAGE_SIZE {
			panic("the size latest update page exceeds the page size")
		}

		// convert the value vector to a page
		page := NewPageFromValueVector(w.ValuesInLatestUpdatePage)

		// compute the offset at which the page should be written in the file
		offset := int64(w.NumStoredPages * PAGE_SIZE)

		// write the page to the file
		_, err := w.File.WriteAt(page.Data[:], offset)
		if err != nil {
			panic(err)
		}

		// clear the value vector
		w.ValuesInLatestUpdatePage = make([]util.Value, 0)
		w.SizeValuesInLatestUpdatePage = 0
		w.NumStoredPages++
	}
}

 /* Transform pager writer to pager reader
  */
func (w *ValuePageWriter) ToValueReader() *ValuePageReader {
	numValues := w.NumValues
	file := w.File

	return &ValuePageReader{
		File:      file,
		NumValues: numValues,
	}
}

/* Transform pager writer to iterator for preparing file merge
 */
func (w *ValuePageWriter) ToValueIterator() *ValueIterator {
	numValues := w.NumValues

	return &ValueIterator{
		File:          w.File,
		CurPageValues: make([]util.Value, 0),
		CurValuePos:   0,
		NumValues:     numValues,
	}
}

/* A helper to read value from the file
   A LRU cache is used to optimize the read performance.
 */
type ValuePageReader struct {
	File      *os.File // file object of the corresponding value file
	NumValues int // the number of values in the file
}

/* Load the reader from a given file. 
   num_values and num_stored_pages are derived from the file
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
	numValues := 0

	if numStoredPages > 0 {
		// read all pages to calculate the total number of values
		bytes := make([]byte, PAGE_SIZE)
		for i := 0; i < numStoredPages; i++ {
			pageOffset := int64(i * PAGE_SIZE)
			_, err = file.ReadAt(bytes, pageOffset)
			if err != nil {
				return nil, err
			}

			page := NewPageFromArray([PAGE_SIZE]byte(bytes))
			numValues += len(page.ToValueVector())
		}
	}

	return &ValuePageReader{
		File:      file,
		NumValues: numValues,
	}, nil
}

/* Load the deserialized vector of the page from the file at given page_id
 */
func (r *ValuePageReader) ReadPageAt(pageID int) []util.Value {

	// cache does not contain the page, should load the page from the file
	offset := int64(pageID * PAGE_SIZE)
	bytes := make([]byte, PAGE_SIZE)
	_, err := r.File.ReadAt(bytes, offset)
	if err != nil {
		panic(err)
	}

	page := NewPageFromArray([PAGE_SIZE]byte(bytes))
	values := page.ToValueVector()

	return values
}

/* Load the deserialized vector given the state's location range
 */
func (r *ValuePageReader) ReadValuesRange(posL, posR int) []util.Value {
	if posL > posR || posL < 0 || posR > r.NumValues {
		return nil
	}

	result := make([]util.Value, 0, posR-posL+1)
	currentPos := 0
	currentPage := 0

	// traverse the pages until finding the page containing posL
	for {
		values := r.ReadPageAt(currentPage)
		if currentPos+len(values) > posL {
			break
		}
		currentPos += len(values)
		currentPage++
	}

	// collect all values in the range
	for currentPos <= posR {
		values := r.ReadPageAt(currentPage)
		for i := 0; i < len(values) && currentPos <= posR; i++ {
			if currentPos >= posL {
				result = append(result, values[i])
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
func (r *ValuePageReader) ToValueIterator() *ValueIterator {
	numValues := r.NumValues

	return &ValueIterator{
		File:          r.File,
		CurPageValues: make([]util.Value, 0),
		CurValuePos:   0,
		NumValues:     numValues,
	}
}


/* Iterator of key-value vector in memory
 */
type InMemKeyValueIterator struct {
	KeyValues []util.KeyValue // the key-values in memory
	CurValuePos int // the position of the current value
}

func NewInMemKeyValueIterator(keyValues []util.KeyValue) *InMemKeyValueIterator {
	return &InMemKeyValueIterator{
		KeyValues: keyValues,
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



/* Iterator of a value file
   Use a cached vector of page to fetch the value one-by-one in a streaming fashion.
   Note that the value should be read from the file in a 'Page' unit.
 */
type ValueIterator struct {
	File          *os.File
	CurPageValues []util.Value
	CurValuePos   int
	NumValues     int
	CurPage       int
	PosInPage     int
}

/* Create a new state iterator by given the file handler and the number of states
 */
func CreateValueIteratorWithNumValues(file *os.File, numValues int) *ValueIterator {
	return &ValueIterator{
		File:          file,
		CurPageValues: make([]util.Value, 0),
		CurValuePos:   0,
		NumValues:     numValues,
		CurPage:       0,
		PosInPage:     0,
	}
}

/* Create a new state iterator by given the file handler.
   The num_states is derived from the file (should load the last page to determine the number of states).
 */
func CreateValueIterator(file *os.File) (*ValueIterator, error) {
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	numStoredPages := int(fileInfo.Size()) / PAGE_SIZE
	numValues := 0

	if numStoredPages > 0 {
		// read all pages to calculate the total number of values
		bytes := make([]byte, PAGE_SIZE)
		for i := 0; i < numStoredPages; i++ {
			pageOffset := int64(i * PAGE_SIZE)
			_, err = file.ReadAt(bytes, pageOffset)
			if err != nil {
				return nil, err
			}

			page := NewPageFromArray([PAGE_SIZE]byte(bytes))
			numValues += len(page.ToValueVector())
		}
	}

	return &ValueIterator{
		File:          file,
		CurPageValues: make([]util.Value, 0),
		CurValuePos:   0,
		NumValues:     numValues,
		CurPage:       0,
		PosInPage:     0,
	}, nil
}

/* Check if there is a next value
 */
func (it *ValueIterator) HasNext() bool {
	return it.CurValuePos < it.NumValues
}

/* Get the next value
 */
func (it *ValueIterator) Next() util.Value {
	if it.CurValuePos >= it.NumValues {
		return nil
	}

	// the current page has been traversed or the first call of Next()
	if it.PosInPage >= len(it.CurPageValues) {
		// should load a new page from the file
		bytes := make([]byte, PAGE_SIZE)
		offset := int64(it.CurPage * PAGE_SIZE)

		_, err := it.File.ReadAt(bytes, offset)
		if err != nil {
			panic(err)
		}

		page := NewPageFromArray([PAGE_SIZE]byte(bytes))
		it.CurPageValues = page.ToValueVector()
		it.PosInPage = 0
		it.CurPage++
	}

	// get the current value
	value := it.CurPageValues[it.PosInPage]

	// update the position
	it.PosInPage++
	it.CurValuePos++

	return value
}
