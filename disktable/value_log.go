package disktable

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"

	"github.com/valyala/bytebufferpool"
)

type ChunkType = byte
type SegmentID = uint32

const (
	ChunkTypeFull ChunkType = iota
	ChunkTypeFirst
	ChunkTypeMiddle
	ChunkTypeLast
)

var (
	ErrClosed     = errors.New("the segment file is closed")
	ErrInvalidCRC = errors.New("invalid crc, the data may be corrupted")
)

const (
	// 7 Bytes
	// Checksum Length Type
	//    4      2     1
	chunkHeaderSize = 7

	// 32 KB
	blockSize = 32 * 1024

	fileModePerm = 0644

	// uin32 + uint32 + int64 + uin32
	// segmentId + BlockNumber + ChunkOffset + ChunkSize
	maxLen = binary.MaxVarintLen32*3 + binary.MaxVarintLen64
)

// ValueLog represents a single segment file in WAL.
// The segment file is append-only, and the data is written in blocks.
// Each block is 32KB, and the data is written in chunks.
type ValueLog struct {
	fd                 *os.File
	currentBlockNumber uint32
	currentBlockSize   uint32
	closed             bool
	header             []byte
	startupBlock       *startupBlock
	isStartupTraversal bool
}

// segmentReader is used to iterate all the data from the segment file.
// You can call Next to get the next chunk data,
// and io.EOF will be returned when there is no data.
type ValueReader struct {
	valueLog    *ValueLog
	blockNumber uint32
	chunkOffset uint64
}

// There is only one reader(single goroutine) for startup traversal,
// so we can use one block to finish the whole traversal
// to avoid memory allocation.
type startupBlock struct {
	block       []byte
	blockNumber int64
}

// ChunkPosition represents the position of a chunk in a segment file.
// Used to read the data from the segment file.
type ChunkPosition struct {
	// BlockNumber The block number of the chunk in the segment file.
	BlockNumber uint32
	// ChunkOffset The start offset of the chunk in the segment file.
	ChunkOffset uint64
	// ChunkSize How many bytes the chunk data takes up in the segment file.
	ChunkSize uint32
}

var blockPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, blockSize)
	},
}

func getBuffer() []byte {
	return blockPool.Get().([]byte)
}

func putBuffer(buf []byte) {
	blockPool.Put(buf)
}

// openSegmentFile a new segment file.
func OpenValueFile(fileName string) (*ValueLog, error) {
	fd, err := os.OpenFile(
		fileName,
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		fileModePerm,
	)

	if err != nil {
		return nil, err
	}

	// set the current block number and block size.
	offset, err := fd.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, fmt.Errorf("seek to the end of value file %s failed: %v", fileName, err)
	}

	return &ValueLog{
		fd:                 fd,
		header:             make([]byte, chunkHeaderSize),
		currentBlockNumber: uint32(offset / blockSize),
		currentBlockSize:   uint32(offset % blockSize),
		startupBlock: &startupBlock{
			block:       make([]byte, blockSize),
			blockNumber: -1,
		},
		isStartupTraversal: false,
	}, nil
}

// NewReader creates a new segment reader.
// You can call Next to get the next chunk data,
// and io.EOF will be returned when there is no data.
func (v *ValueLog) NewReader() *ValueReader {
	return &ValueReader{
		valueLog:    v,
		blockNumber: 0,
		chunkOffset: 0,
	}
}

// Sync flushes the segment file to disk.
func (v *ValueLog) Sync() error {
	if v.closed {
		return nil
	}
	return v.fd.Sync()
}

// Remove removes the segment file.
func (v *ValueLog) Remove() error {
	if !v.closed {
		v.closed = true
		if err := v.fd.Close(); err != nil {
			return err
		}
	}

	return os.Remove(v.fd.Name())
}

// Close closes the segment file.
func (v *ValueLog) Close() error {
	if v.closed {
		return nil
	}

	v.closed = true
	return v.fd.Close()
}

// Size returns the size of the segment file.
func (v *ValueLog) Size() int64 {
	size := int64(v.currentBlockNumber) * int64(blockSize)
	return size + int64(v.currentBlockSize)
}

// writeToBuffer calculate chunkPosition for data, write data to bytebufferpool, update segment status
// The data will be written in chunks, and the chunk has four types:
// ChunkTypeFull, ChunkTypeFirst, ChunkTypeMiddle, ChunkTypeLast.
//
// Each chunk has a header, and the header contains the length, type and checksum.
// And the payload of the chunk is the real data you want to Write.
func (v *ValueLog) writeToBuffer(data []byte, chunkBuffer *bytebufferpool.ByteBuffer) (*ChunkPosition, error) {
	startBufferLen := chunkBuffer.Len()
	padding := uint32(0)

	if v.closed {
		return nil, ErrClosed
	}

	// if the left block size can not hold the chunk header, padding the block
	if v.currentBlockSize+chunkHeaderSize >= blockSize {
		// padding if necessary
		if v.currentBlockSize < blockSize {
			p := make([]byte, blockSize-v.currentBlockSize)
			chunkBuffer.B = append(chunkBuffer.B, p...)
			padding += blockSize - v.currentBlockSize

			// a new block
			v.currentBlockNumber += 1
			v.currentBlockSize = 0
		}
	}

	// return the start position of the chunk, then the user can use it to read the data.
	position := &ChunkPosition{
		BlockNumber: v.currentBlockNumber,
		ChunkOffset: uint64(v.currentBlockSize),
	}

	dataSize := uint32(len(data))
	// The entire chunk can fit into the block.
	if v.currentBlockSize+dataSize+chunkHeaderSize <= blockSize {
		v.appendChunkBuffer(chunkBuffer, data, ChunkTypeFull)
		position.ChunkSize = dataSize + chunkHeaderSize
	} else {
		// If the size of the data exceeds the size of the block,
		// the data should be written to the block in batches.
		var (
			leftSize             = dataSize
			blockCount    uint32 = 0
			currBlockSize        = v.currentBlockSize
		)

		for leftSize > 0 {
			chunkSize := blockSize - currBlockSize - chunkHeaderSize
			if chunkSize > leftSize {
				chunkSize = leftSize
			}

			var end = dataSize - leftSize + chunkSize
			if end > dataSize {
				end = dataSize
			}

			// append the chunks to the buffer
			var chunkType ChunkType
			switch leftSize {
			case dataSize: // First chunk
				chunkType = ChunkTypeFirst
			case chunkSize: // Last chunk
				chunkType = ChunkTypeLast
			default: // Middle chunk
				chunkType = ChunkTypeMiddle
			}
			v.appendChunkBuffer(chunkBuffer, data[dataSize-leftSize:end], chunkType)

			leftSize -= chunkSize
			blockCount += 1
			currBlockSize = (currBlockSize + chunkSize + chunkHeaderSize) % blockSize
		}
		position.ChunkSize = blockCount*chunkHeaderSize + dataSize
	}

	// the buffer length must be equal to chunkSize+padding length
	endBufferLen := chunkBuffer.Len()
	if position.ChunkSize+padding != uint32(endBufferLen-startBufferLen) {
		return nil, fmt.Errorf("wrong!!! the chunk size %d is not equal to the buffer len %d",
			position.ChunkSize+padding, endBufferLen-startBufferLen)
	}

	// update segment status
	v.currentBlockSize += position.ChunkSize
	if v.currentBlockSize >= blockSize {
		v.currentBlockNumber += v.currentBlockSize / blockSize
		v.currentBlockSize = v.currentBlockSize % blockSize
	}

	return position, nil
}

// writeAll write batch data to the segment file.
func (v *ValueLog) writeAll(data [][]byte) (positions []*ChunkPosition, err error) {
	if v.closed {
		return nil, ErrClosed
	}

	// if any error occurs, restore the segment status
	originBlockNumber := v.currentBlockNumber
	originBlockSize := v.currentBlockSize

	// init chunk buffer
	chunkBuffer := bytebufferpool.Get()
	chunkBuffer.Reset()
	defer func() {
		if err != nil {
			v.currentBlockNumber = originBlockNumber
			v.currentBlockSize = originBlockSize
		}
		bytebufferpool.Put(chunkBuffer)
	}()

	// write all data to the chunk buffer
	var pos *ChunkPosition
	positions = make([]*ChunkPosition, len(data))
	for i := 0; i < len(positions); i++ {
		pos, err = v.writeToBuffer(data[i], chunkBuffer)
		if err != nil {
			return
		}
		positions[i] = pos
	}
	// write the chunk buffer to the segment file
	if err = v.writeChunkBuffer(chunkBuffer); err != nil {
		return
	}
	return
}

// Write writes the data to the segment file.
func (v *ValueLog) Write(data []byte) (pos *ChunkPosition, err error) {
	if v.closed {
		return nil, ErrClosed
	}

	originBlockNumber := v.currentBlockNumber
	originBlockSize := v.currentBlockSize

	// init chunk buffer
	chunkBuffer := bytebufferpool.Get()
	chunkBuffer.Reset()
	defer func() {
		if err != nil {
			v.currentBlockNumber = originBlockNumber
			v.currentBlockSize = originBlockSize
		}
		bytebufferpool.Put(chunkBuffer)
	}()

	// write all data to the chunk buffer
	pos, err = v.writeToBuffer(data, chunkBuffer)
	if err != nil {
		return
	}
	// write the chunk buffer to the segment file
	if err = v.writeChunkBuffer(chunkBuffer); err != nil {
		return
	}

	return
}

func (v *ValueLog) appendChunkBuffer(buf *bytebufferpool.ByteBuffer, data []byte, chunkType ChunkType) {
	// Length	2 Bytes	index:4-5
	binary.LittleEndian.PutUint16(v.header[4:6], uint16(len(data)))
	// Type	1 Byte	index:6
	v.header[6] = chunkType
	// Checksum	4 Bytes index:0-3
	sum := crc32.ChecksumIEEE(v.header[4:])
	sum = crc32.Update(sum, crc32.IEEETable, data)
	binary.LittleEndian.PutUint32(v.header[:4], sum)

	// append the header and data to segment chunk buffer
	buf.B = append(buf.B, v.header...)
	buf.B = append(buf.B, data...)
}

// write the pending chunk buffer to the segment file
func (v *ValueLog) writeChunkBuffer(buf *bytebufferpool.ByteBuffer) error {
	if v.currentBlockSize > blockSize {
		return errors.New("the current block size exceeds the maximum block size")
	}

	// write the data into underlying file
	if _, err := v.fd.Write(buf.Bytes()); err != nil {
		return err
	}

	// the cached block can not be reused again after writes.
	v.startupBlock.blockNumber = -1
	return nil
}

// Read reads the data from the segment file by the block number and chunk offset.
func (v *ValueLog) Read(blockNumber uint32, chunkOffset uint64) ([]byte, error) {
	value, _, err := v.readInternal(blockNumber, chunkOffset)
	return value, err
}

func (v *ValueLog) readInternal(blockNumber uint32, chunkOffset uint64) ([]byte, *ChunkPosition, error) {
	if v.closed {
		return nil, nil, ErrClosed
	}

	var (
		result    []byte
		block     []byte
		segSize   = v.Size()
		nextChunk = &ChunkPosition{BlockNumber: v.currentBlockNumber}
	)

	if v.isStartupTraversal {
		block = v.startupBlock.block
	} else {
		block = getBuffer()
		if len(block) != blockSize {
			block = make([]byte, blockSize)
		}
		defer putBuffer(block)
	}

	for {
		size := int64(blockSize)
		offset := int64(blockNumber) * blockSize
		if size+offset > segSize {
			size = segSize - offset
		}

		if chunkOffset >= uint64(size) {
			return nil, nil, io.EOF
		}

		if v.isStartupTraversal {
			// There are two cases that we should read block from file:
			// 1. the acquired block is not the cached one
			// 2. new writes appended to the block, and the block
			// is still smaller than 32KB, we must read it again because of the new writes.
			if v.startupBlock.blockNumber != int64(blockNumber) || size != blockSize {
				// read block from segment file at the specified offset.
				_, err := v.fd.ReadAt(block[0:size], offset)
				if err != nil {
					return nil, nil, err
				}
				// remember the block
				v.startupBlock.blockNumber = int64(blockNumber)
			}
		} else {
			if _, err := v.fd.ReadAt(block[0:size], offset); err != nil {
				return nil, nil, err
			}
		}

		// header
		header := block[chunkOffset : chunkOffset+chunkHeaderSize]

		// length
		length := binary.LittleEndian.Uint16(header[4:6])

		// copy data
		start := chunkOffset + chunkHeaderSize
		result = append(result, block[start:start+uint64(length)]...)

		// check sum
		checksumEnd := chunkOffset + chunkHeaderSize + uint64(length)
		checksum := crc32.ChecksumIEEE(block[chunkOffset+4 : checksumEnd])
		savedSum := binary.LittleEndian.Uint32(header[:4])
		if savedSum != checksum {
			return nil, nil, ErrInvalidCRC
		}

		// type
		chunkType := header[6]

		if chunkType == ChunkTypeFull || chunkType == ChunkTypeLast {
			nextChunk.BlockNumber = blockNumber
			nextChunk.ChunkOffset = checksumEnd
			// If this is the last chunk in the block, and the left block
			// space are paddings, the next chunk should be in the next block.
			if checksumEnd+chunkHeaderSize >= blockSize {
				nextChunk.BlockNumber += 1
				nextChunk.ChunkOffset = 0
			}
			break
		}
		blockNumber += 1
		chunkOffset = 0
	}
	return result, nextChunk, nil
}

// ReadBatch reads multiple values from the value log.
// It is optimized for performance by minimizing disk I/O.
// The input positions are expected to be sorted physically (by BlockNumber, then ChunkOffset) for efficiency.
func (v *ValueLog) ReadBatch(positions []*ChunkPosition) (map[uint64][]byte, error) {
	if v.closed {
		return nil, ErrClosed
	}

	// The map will be keyed by the starting ChunkOffset of each value for easy lookup by the caller.
	valueMap := make(map[uint64][]byte, len(positions))
	if len(positions) == 0 {
		return valueMap, nil
	}

	blockBuffer := getBuffer()
	defer putBuffer(blockBuffer)

	var currentBlockNumber uint32 = ^uint32(0) // Sentinel value to force initial block read
	var bytesInBlock int64

	// positionIndex tracks which requested position we are currently trying to fulfill.
	positionIndex := 0

	for positionIndex < len(positions) {
		pos := positions[positionIndex]

		// This outer loop reads ONE full logical value, which might be fragmented across several blocks.
		var valueBuf []byte
		currentFragmentBlockNum := pos.BlockNumber
		currentFragmentOffset := pos.ChunkOffset

	fragmentLoop:
		for {
			// Check if the block containing the current fragment is already loaded in our buffer.
			// If not, read the required block from disk.
			if currentFragmentBlockNum != currentBlockNumber {
				diskOffset := int64(currentFragmentBlockNum) * blockSize
				segSize := v.Size()
				if diskOffset >= segSize {
					return nil, io.EOF
				}

				// Determine how many bytes to read for this block.
				bytesInBlock = blockSize
				if diskOffset+blockSize > segSize {
					bytesInBlock = segSize - diskOffset
				}

				_, err := v.fd.ReadAt(blockBuffer[:bytesInBlock], diskOffset)
				if err != nil {
					return nil, err
				}
				currentBlockNumber = currentFragmentBlockNum
			}

			// The correct block is now in blockBuffer. Read the chunk from the buffer.
			if currentFragmentOffset+chunkHeaderSize > uint64(bytesInBlock) {
				return nil, fmt.Errorf("read header failed: offset %d is out of bounds for block %d with size %d",
					currentFragmentOffset, currentBlockNumber, bytesInBlock)
			}

			header := blockBuffer[currentFragmentOffset : currentFragmentOffset+chunkHeaderSize]
			length := binary.LittleEndian.Uint16(header[4:6])
			chunkType := header[6]

			dataStart := currentFragmentOffset + chunkHeaderSize
			dataEnd := dataStart + uint64(length)

			if dataEnd > uint64(bytesInBlock) {
				return nil, fmt.Errorf("read data failed: data end offset %d is out of bounds for block %d with size %d",
					dataEnd, currentBlockNumber, bytesInBlock)
			}

			// Perform checksum validation.
			checksumEnd := currentFragmentOffset + chunkHeaderSize + uint64(length)
			checksum := crc32.ChecksumIEEE(blockBuffer[currentFragmentOffset+4 : checksumEnd])
			savedSum := binary.LittleEndian.Uint32(header[:4])
			if savedSum != checksum {
				return nil, ErrInvalidCRC
			}

			data := blockBuffer[dataStart:dataEnd]
			valueBuf = append(valueBuf, data...)

			switch chunkType {
			case ChunkTypeFull, ChunkTypeLast:
				// The full value has been read. Store it and break the fragment-reading loop.
				valueMap[pos.ChunkOffset] = valueBuf
				positionIndex++
				break fragmentLoop
			case ChunkTypeFirst, ChunkTypeMiddle:
				// The value is fragmented. Prepare to read the next fragment from the subsequent block.
				currentFragmentBlockNum++
				currentFragmentOffset = 0
			}
		}
	}

	return valueMap, nil
}

// Next returns the Next chunk data.
// You can call it repeatedly until io.EOF is returned.
func (v *ValueReader) Next() ([]byte, *ChunkPosition, error) {
	// The segment file is closed
	if v.valueLog.closed {
		return nil, nil, ErrClosed
	}

	// this position describes the current chunk info
	chunkPosition := &ChunkPosition{
		BlockNumber: v.blockNumber,
		ChunkOffset: v.chunkOffset,
	}

	value, nextChunk, err := v.valueLog.readInternal(
		v.blockNumber,
		v.chunkOffset,
	)
	if err != nil {
		return nil, nil, err
	}

	// Calculate the chunk size.
	// Remember that the chunk size is just an estimated value,
	// not accurate, so don't use it for any important logic.
	chunkPosition.ChunkSize =
		nextChunk.BlockNumber*blockSize + uint32(nextChunk.ChunkOffset) -
			(v.valueLog.currentBlockNumber*blockSize + uint32(v.valueLog.currentBlockSize))

	// update the position
	v.blockNumber = nextChunk.BlockNumber
	v.chunkOffset = nextChunk.ChunkOffset

	return value, chunkPosition, nil
}

// Encode encodes the chunk position to a byte slice.
// Return the slice with the actual occupied elements.
// You can decode it by calling wal.DecodeChunkPosition().
func (cp *ChunkPosition) Encode() []byte {
	return cp.encode(true)
}

// EncodeFixedSize encodes the chunk position to a byte slice.
// Return a slice of size "maxLen".
// You can decode it by calling wal.DecodeChunkPosition().
func (cp *ChunkPosition) EncodeFixedSize() []byte {
	return cp.encode(false)
}

// encode the chunk position to a byte slice.
func (cp *ChunkPosition) encode(shrink bool) []byte {
	buf := make([]byte, maxLen)

	var index = 0
	// BlockNumber
	index += binary.PutUvarint(buf[index:], uint64(cp.BlockNumber))
	// ChunkOffset
	index += binary.PutUvarint(buf[index:], uint64(cp.ChunkOffset))
	// ChunkSize
	index += binary.PutUvarint(buf[index:], uint64(cp.ChunkSize))

	if shrink {
		return buf[:index]
	}
	return buf
}

// DecodeChunkPosition decodes the chunk position from a byte slice.
// You can encode it by calling wal.ChunkPosition.Encode().
func DecodeChunkPosition(buf []byte) *ChunkPosition {
	if len(buf) == 0 {
		return nil
	}

	var index = 0
	// BlockNumber
	blockNumber, n := binary.Uvarint(buf[index:])
	index += n
	// ChunkOffset
	chunkOffset, n := binary.Uvarint(buf[index:])
	index += n
	// ChunkSize
	chunkSize, n := binary.Uvarint(buf[index:])
	index += n

	return &ChunkPosition{
		BlockNumber: uint32(blockNumber),
		ChunkOffset: uint64(chunkOffset),
		ChunkSize:   uint32(chunkSize),
	}
}
