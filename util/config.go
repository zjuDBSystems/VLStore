package util

import (
	"math"
)

type Configs struct {
	Fanout        int // fanout of the MHT
	Epsilon       int  // upper error bound of the piecewise model
	DirName       string // directory name of the storage
	BaseStateNum  int // number of states in the in-memory level
	SizeRatio     int // ratio of the number of states in between the two consecutive levels
}

func NewConfigs(fanout int, epsilon int, dirName string, baseStateNum int, sizeRatio int) *Configs {
	return &Configs{
		Fanout:        fanout,
		Epsilon:       epsilon,
		DirName:       dirName,
		BaseStateNum:  baseStateNum,
		SizeRatio:     sizeRatio,	
	}
}

func (c *Configs) MaxNumOfStatesInARun(levelId int) int {
	return c.BaseStateNum * int(math.Pow(float64(c.SizeRatio), float64(levelId)))
}


/*
Compute a recommended bitmap size for items_count items
and a fp_p rate of false positives.
fp_p obviously has to be within the [0.0, 1.0] range.
 */
func ComputeBitmapSizeInBytes(itemsCount int, fpP float64) int {
	if itemsCount <= 0 || fpP <= 0.0 || fpP >= 1.0 {
		panic("invalid input")
	}

	// We're using ceil instead of round in order to get an error rate <= the desired.
	// Using round can result in significantly higher error rates.	
	numSlices := math.Ceil(math.Log2(1.0 / fpP))
	sliceLenBits := math.Ceil(float64(itemsCount) / math.Log(2.0))
	totalBits := numSlices * sliceLenBits
	// round up to the next byte
	bufferBytes := (totalBits + 7) / 8
	return int(bufferBytes)
}
