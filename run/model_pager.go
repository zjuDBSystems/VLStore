package run

import (
	"VLStore/util"
	"io"
	"math"
	"os"
)

const (
	TMP_MODEL_FILE_NAME = "tmp_model_file.dat"
)

/* A helper structure to keep a collection of models and their sharing model_level
 */
type ModelCollections struct {
	V          []*util.KeyModel // vector of models
	ModelLevel int
}

func NewModelCollections() *ModelCollections {
	return &ModelCollections{
		V:          []*util.KeyModel{},
		ModelLevel: 0,
	}
}

func NewModelCollectionsWithLevel(modelLevel int) *ModelCollections {
	return &ModelCollections{
		V:          []*util.KeyModel{},
		ModelLevel: modelLevel,
	}
}

/* A helper that writes the models into a file with a sequence of pages, similar to the StatePageWriter
 */
type ModelPageWriter struct {
	File                  *os.File          // file object of the corresponding index file
	LatestModelCollection *ModelCollections // a preparation vector to obsorb the streaming models which are not persisted in the file yet
	NumStoredPages        int               // records the number of pages that are stored in the file
}

/* Initialize the writer using a given file_name
 */
func CreateModelPageWriter(fileName string, modelLevel int) (*ModelPageWriter, error) {
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0666)
	if err != nil {
		return nil, err
	}

	return &ModelPageWriter{
		File:                  file,
		LatestModelCollection: NewModelCollectionsWithLevel(modelLevel),
		NumStoredPages:        0,
	}, nil
}

/* Load the writer from a given file, cache is empty, num_states and num_stored_pages are derived from the file
 */
func LoadModelPageWriter(fileName string) (*ModelPageWriter, error) {
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	numStoredPages := int(fileInfo.Size()) / PAGE_SIZE
	latestModelCollection := NewModelCollections()

	if numStoredPages > 0 {
		lastPageOffset := (numStoredPages - 1) * PAGE_SIZE
		// get last page from file
		bytes := make([]byte, PAGE_SIZE)
		_, err := file.ReadAt(bytes, int64(lastPageOffset))
		if err != nil {
			return nil, err
		}

		page := NewPageFromArray([PAGE_SIZE]byte(bytes))
		modelCollection := page.ToModelVector()
		pageVec := modelCollection.V
		latestModelCollection.ModelLevel = modelCollection.ModelLevel

		// derive number of models in the last page
		numModelsInLastPage := len(pageVec)

		if numModelsInLastPage != MAX_NUM_MODEL_IN_PAGE {
			// the last page is not full, should not be finalized in the file
			latestModelCollection.V = pageVec
			numStoredPages--
		}
	}

	return &ModelPageWriter{
		File:                  file,
		LatestModelCollection: latestModelCollection,
		NumStoredPages:        numStoredPages,
	}, nil
}

/* Streamingly add the model to the latest collection, if the collection is full, flush it to the file
 */
func (m *ModelPageWriter) Append(model *util.KeyModel) error {
	// add the model
	m.LatestModelCollection.V = append(m.LatestModelCollection.V, model)
	if len(m.LatestModelCollection.V) == MAX_NUM_MODEL_IN_PAGE {
		// vector is full, should be added to a page and flushed the page to the file
		return m.Flush()
	}
	return nil
}

/* Flush the vector in latest update page to the last page in the value file
 */
func (m *ModelPageWriter) Flush() error {
	if len(m.LatestModelCollection.V) != 0 {
		// first put the vector into a page
		page := NewPageFromModelVector(m.LatestModelCollection.V, m.LatestModelCollection.ModelLevel)
		// compute the offset at which the page will be written in the file
		offset := m.NumStoredPages * PAGE_SIZE
		// write the page to the file
		_, err := m.File.WriteAt(page.Data[:], int64(offset))
		if err != nil {
			return err
		}
		// clear the vector
		m.LatestModelCollection.V = m.LatestModelCollection.V[:0]
		m.NumStoredPages++
	}
	return nil
}

/* Transform writer to reader
 */
func (m *ModelPageWriter) ToModelReader() *ModelPageReader {
	file := m.File
	numStoredPages := m.NumStoredPages
	return &ModelPageReader{
		File:           file,
		NumStoredPages: numStoredPages,
	}
}

/* A helper to read the models from the file
 */
type ModelPageReader struct {
	File           *os.File // file object of the corresponding index file
	NumStoredPages int      // records the number of pages that are stored in the file
	PageReads      int      // 统计页面读取次数
}

/*
Load the reader from a given file
cache is empty and num_stored_pages are derived from the file
*/
func LoadModelPageReader(fileName string) (*ModelPageReader, error) {
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	numStoredPages := int(fileInfo.Size()) / PAGE_SIZE

	return &ModelPageReader{
		File:           file,
		NumStoredPages: numStoredPages,
	}, nil
}

/* Load the deserialized vector of the page from the file at given page_id
 */
func (m *ModelPageReader) ReadPageAt(componentID int, pageID int, cacheManager *CacheManager) (*ModelCollections, error) {
	m.PageReads++
	// first check whether the cache contains the page
	modelCollection, ok := cacheManager.GetModelPage(componentID, pageID)
	if ok {
		return modelCollection, nil
	}else {
		// cache does not contain the page, should load the page from the file
		offset := pageID * PAGE_SIZE
		bytes := make([]byte, PAGE_SIZE)
		_, err := m.File.ReadAt(bytes, int64(offset))
		if err != nil {
			return nil, err
		}
		page := NewPageFromArray([PAGE_SIZE]byte(bytes))
		modelCollection := page.ToModelVector()
		// before return the vector, add it to the cache with page_id as the key
		cacheManager.SetModelPage(componentID, pageID, modelCollection)
		return modelCollection, nil
	}
}

/* Query Models in the model file
 */
func (m *ModelPageReader) GetPredKeyPos(componentID int, searchKey util.Key, epsilon int, cacheManager *CacheManager) (int, error) {
	/* First load the last page and find the model that covers the search key
	 */
	lastPageID := m.NumStoredPages - 1
	topModelCollection, err := m.ReadPageAt(componentID, lastPageID, cacheManager)
	if err != nil {
		return 0, err
	}

	modelV := topModelCollection.V
	modelLevel := topModelCollection.ModelLevel

	if modelLevel == 0 {
		// the last page stores the lowest level
		predPos := util.FetchModelAndPredict(modelV, searchKey)
		return predPos, nil
	} else {
		// first search the model in the model_v and then determine the predicted page id range
		predPos := util.FetchModelAndPredict(modelV, searchKey)
		predPageIDLb := int(math.Max(0, float64((predPos-epsilon-1)/MAX_NUM_MODEL_IN_PAGE)))
		predPageIDUb := int(math.Min(float64(lastPageID), float64((predPos+int(epsilon)+1)/MAX_NUM_MODEL_IN_PAGE)))

		modelLevel--
		predPos, err = m.QueryModel(componentID, predPageIDLb, predPageIDUb, searchKey, modelLevel, cacheManager)
		if err != nil {
			return 0, err
		}

		for modelLevel != 0 {
			predPageIDLb = int(math.Max(0, float64((predPos-epsilon-1)/MAX_NUM_MODEL_IN_PAGE)))
			predPageIDUb = int(math.Min(float64(lastPageID), float64((predPos+int(epsilon)+1)/MAX_NUM_MODEL_IN_PAGE)))

			modelLevel--
			predPos, err = m.QueryModel(componentID, predPageIDLb, predPageIDUb, searchKey, modelLevel, cacheManager)
			if err != nil {
				return 0, err
			}
		}

		return predPos, nil
	}
}

func (m *ModelPageReader) QueryModel(runId int, pageIDLb int, pageIDUb int, searchKey util.Key, modelLevel int, cacheManager *CacheManager) (int, error) {
	modelV := []*util.KeyModel{}

	for pageID := pageIDLb; pageID <= pageIDUb; pageID++ {
		collection, err := m.ReadPageAt(runId, pageID, cacheManager)
		if err != nil {
			return 0, err
		}

		if collection.ModelLevel == modelLevel {
			modelV = append(modelV, collection.V...)
		}
	}

	predPos := util.FetchModelAndPredict(modelV, searchKey)
	return predPos, nil
}

/* A model constructor that generates and appends models to the file in a streaming fashion
 */
type StreamModelConstructor struct {
	OutputModelWriter         *ModelPageWriter     // a writer of the output model file
	LowestLevelModelGenerator *util.ModelGenerator // a model generator of the lowest level (learn input is the states)
	Epsilon                   int                  // an upper-bound model prediction error
	StatePos                  int                  // the position of the input state
}

/* Initiate the constructor with the output model file name and the upper error bound
 */
func NewStreamModelConstructor(outputFileName string, epsilon int) (*StreamModelConstructor, error) {
	// create the output model writer
	outputModelWriter, err := CreateModelPageWriter(outputFileName, 0)
	if err != nil {
		return nil, err
	}

	// initiate the model generator for the lowest level
	lowestLevelModelGenerator := util.NewModelGenerator(epsilon)

	return &StreamModelConstructor{
		OutputModelWriter:         outputModelWriter,
		LowestLevelModelGenerator: lowestLevelModelGenerator,
		Epsilon:                   epsilon,
		StatePos:                  0,
	}, nil
}

/* Streaminly append the key to the model generator for the lowest level
 */
func (s *StreamModelConstructor) AppendKey(key util.Key) error {
	pos := s.StatePos
	r := s.LowestLevelModelGenerator.Append(key, pos)
	if !r {
		// finalize the model since the new coming key cannot fit the model within the prediction error bound
		model := s.LowestLevelModelGenerator.FinalizeModel()
		// write the model to the output model writer (can be kept in the latest page cache in memory or be flushed to the file)
		err := s.OutputModelWriter.Append(model)
		if err != nil {
			return err
		}
		// re-insert the key position to the model generator since the previous insertion fails
		s.LowestLevelModelGenerator.Append(key, pos)
	}
	s.StatePos++
	return nil
}

/*
Finalize the append of the key-pos

	End the insertion of the lowest_level_model_generator: finalize the model, append it to the output_model_writer, and flush it to the file
	Recursively build the models upon the previous level and append them to the file in a streaming fashion.
*/
func (s *StreamModelConstructor) FinalizeAppend() error {
	/* First finalize the lowest level models
	 */
	if !s.LowestLevelModelGenerator.IsHullEmpty() {
		model := s.LowestLevelModelGenerator.FinalizeModel()
		err := s.OutputModelWriter.Append(model)
		if err != nil {
			return err
		}
	}

	err := s.OutputModelWriter.Flush()
	if err != nil {
		return err
	}

	/*
		recursively construct models in the upper levels
	*/

	outputModelWriter := s.OutputModelWriter
	// n is the number of page in the previous model level
	n := outputModelWriter.NumStoredPages
	modelLevel := 0

	for n > 1 {
		// n > 1 means we should build an upper level models since the top level models should be kept in a single page
		// increment the model_level
		modelLevel++
		// start_page_id is the id of the starting page that the upper level is learned from
		startPageID := outputModelWriter.NumStoredPages - n
		// initiate a model generator for the upper level models
		modelGenerator := util.NewModelGenerator(s.Epsilon)
		// create a temporary model writer for keeping the upper level models
		tmpModelWriter, err := CreateModelPageWriter(TMP_MODEL_FILE_NAME, modelLevel)
		if err != nil {
			return err
		}
		// pos is the position of the learned input of the upper level models
		pos := startPageID * MAX_NUM_MODEL_IN_PAGE

		for pageID := startPageID; pageID < outputModelWriter.NumStoredPages; pageID++ {
			// read the model page from the file in the output_model_writer at the corresponding offset
			offset := pageID * PAGE_SIZE
			bytes := make([]byte, PAGE_SIZE)
			_, err := outputModelWriter.File.ReadAt(bytes, int64(offset))
			if err != nil {
				return err
			}

			page := NewPageFromArray([PAGE_SIZE]byte(bytes))
			// deserialize the models from the page, these are seen as the learned input of the upper level models
			inputModels := page.ToModelVector().V

			for _, inputModel := range inputModels {
				r := modelGenerator.Append(inputModel.Start, pos)
				if !r {
					outputModel := modelGenerator.FinalizeModel()
					// write the output model to the temporary model write of the upper models
					err := tmpModelWriter.Append(outputModel)
					if err != nil {
						return err
					}
				}
				modelGenerator.Append(inputModel.Start, pos)
				pos++
			}
		}

		// handle the rest of the points in the hull
		if !modelGenerator.IsHullEmpty() {
			outputModel := modelGenerator.FinalizeModel()
			err := tmpModelWriter.Append(outputModel)
			if err != nil {
				return err
			}
		}

		// flush the temporary model writer to the temporary file
		err = tmpModelWriter.Flush()
		if err != nil {
			return err
		}

		// update n as the number of page of the temporary model writer
		n = tmpModelWriter.NumStoredPages
		// concatenate the content of temporary model file to the output model file
		err = ConcatenateFileAToFileB(tmpModelWriter.File, outputModelWriter.File)
		if err != nil {
			return err
		}

		// update the number of pages in the output model file
		outputModelWriter.NumStoredPages += tmpModelWriter.NumStoredPages

		// close the tmp_model_writer and remove the temporary file
		tmpModelWriter.File.Close()
		err = os.Remove(TMP_MODEL_FILE_NAME)
		if err != nil {
			return err
		}
	}

	return nil
}

func ConcatenateFileAToFileB(fileA *os.File, fileB *os.File) error {
	// rewind the cursor of file_a to the start
	_, err := fileA.Seek(0, 0)
	if err != nil {
		return err
	}

	fileInfo, err := fileB.Stat()
	if err != nil {
		return err
	}

	l := fileInfo.Size()
	// seek the cursor of file_b to the end
	_, err = fileB.Seek(l, 0)
	if err != nil {
		return err
	}

	// copy the content in file_a to the end of file_b
	_, err = io.Copy(fileB, fileA)
	return err
}
