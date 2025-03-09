package run

import (
    "VLStore/util"
    "encoding/binary"
)

const (
    PAGE_SIZE = 4096
    MAX_NUM_HASH_IN_PAGE = PAGE_SIZE / util.H256_SIZE - 1 // dedeuction of one is because we need to store some meta-data (i.e., num_of_hash) in the page
)

/* Structure of the page with default 4096 byte
 */
type Page struct {
    Data [PAGE_SIZE]byte
}

/* Initialize a page with 4096 bytes
 */
func NewPage() *Page {
    return &Page{}
}

/* Create a page with the given data block
 */
func NewPageFromArray(data [PAGE_SIZE]byte) *Page {
    return &Page{Data: data}
}

/*
Write value vector to the page
4 bytes num of value | (4 bytes length, value bytes) for each value
*/
func NewPageFromValueVector(values []util.Value) *Page {
    // check the length of the vector is inside the maximum number in page
	totalBytes := 4
	for _, value := range values {
		totalBytes += len(value) + 4
	}

	if totalBytes > PAGE_SIZE {
		panic("value vector size is larger than page size")
	}

    p := NewPage()

    // write the number of value in the front of the page
    numOfValues := uint32(len(values))
    binary.BigEndian.PutUint32(p.Data[0:4], numOfValues)

    // iteratively write each value to the page
    offset := 4
    for _, value := range values {
        // value length
        valueLen := uint32(len(value))
        binary.BigEndian.PutUint32(p.Data[offset:offset+4], valueLen)
        offset += 4

        // value
        copy(p.Data[offset:offset+int(valueLen)], value)
        offset += int(valueLen)
    }

    return p
}

/*
Read the values from a page
*/
func (p *Page) ToValueVector() []util.Value {
    v := make([]util.Value, 0)

    // deserialize the number of value in the page
    numOfValues := binary.BigEndian.Uint32(p.Data[0:4])

    // deserialize each of the value from the page
    offset := 4
    for i := uint32(0); i < numOfValues; i++ {
        // read the value length
        if offset+4 > PAGE_SIZE {
            panic("exceed page size")
        }
        valueLen := binary.BigEndian.Uint32(p.Data[offset : offset+4])
        offset += 4

        // read the value
        if offset+int(valueLen) > PAGE_SIZE {
            panic("exceed page size")
        }
        value := make(util.Value, valueLen)
        copy(value, p.Data[offset:offset+int(valueLen)])
        v = append(v, value)
        offset += int(valueLen)
    }

    return v
}

/* 
Write hash vector to the page
4 bytes num of hash | hash_0, hash_1, ...
*/
func NewPageFromHashVector(hashes []util.H256) *Page {
    // check the length of the vector is inside the maximum number in page
    if len(hashes) > MAX_NUM_HASH_IN_PAGE {
        panic("hash vector size is larger than page size")
    }

    p := NewPage()

    // write the number of hash in the front of the page
    numOfHashes := uint32(len(hashes))
    binary.BigEndian.PutUint32(p.Data[0:4], numOfHashes)

    // iteratively write each hash to the page
    offset := 4
    for _, hash := range hashes {
        copy(p.Data[offset:offset+util.H256_SIZE], hash[:])
        offset += util.H256_SIZE
    }

    return p
}


/*
Read the hashes from a block
*/
func (p *Page) ToHashVector() []util.H256 {
    hashes := make([]util.H256, 0)

    // deserialize the number of hash in the page
    numOfHashes := binary.BigEndian.Uint32(p.Data[0:4])

    // deserialize each of the hash from the page
    offset := 4
    for i := uint32(0); i < numOfHashes; i++ {
        if offset+util.H256_SIZE > PAGE_SIZE {
            panic("exceed page size")
        }
        hash := util.H256{}
        copy(hash[:], p.Data[offset:offset+util.H256_SIZE])
        hashes = append(hashes, hash)
        offset += util.H256_SIZE
    }

    return hashes
}