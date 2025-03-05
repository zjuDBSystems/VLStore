package run

import (
    "testing"
    "VLStore/util"
    "fmt"
)

func TestValueAndPageSerDeser(t *testing.T) {
    n := 100
    v := []util.Value{}
    for i := 0; i < n; i++ {
        v = append(v, []byte(fmt.Sprintf("%d", i)))
    }

    p := NewPageFromValueVector(v)
    
    deser_v := p.ToValueVector()

    if len(v) != len(deser_v) {
        t.Errorf("length of the value vector is not the same")
    }

    for i := 0; i < n; i++ {
        if string(v[i]) != string(deser_v[i]) {
            t.Errorf("value at index %d is not the same", i)
        }
    }

	// print the deser_v
	for _, value := range deser_v {
		fmt.Print(string(value) + " ")
	}
}