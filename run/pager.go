package run

import (
    "VLStore/util"
    "encoding/binary"
)

const (
    PAGE_SIZE = 4096
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
