package util

import (
	"encoding/binary"
	"math"
)

/*
 * PGM索引（Piecewise Geometric Model Index）实现
 *
 * PGM索引是一种基于学习的索引结构，它通过构建分段线性模型来近似数据分布，从而实现高效的数据查找。
 * 其核心思想是将传统的B树或哈希表等索引结构替换为机器学习模型。它基于这样一个观察：有序数据的分布
 * 可以被视为一个函数，这个函数将键映射到它们在排序数组中的位置。如果我们能够学习这个函数，就可以
 * 直接预测任何键的位置，而不需要进行传统的二分查找或遍历。
 *
 * PGM索引使用分段线性模型来近似数据分布。对于每个数据段，它学习一个线性函数：
 * f(key) = slope * (key - start) + intercept
 *
 * 其中：
 * - key 是要查找的键
 * - start 是该段的起始键
 * - slope 是斜率
 * - intercept 是截距
 *
 * 这个函数预测给定键在排序数组中的位置。
 *
 * PGM索引的构建过程使用凸包算法来找到最优的分段线性模型。它通过控制参数epsilon来限制预测误差，
 * 当无法在当前误差范围内容纳新点时，就会创建一个新的模型段。
 *
 * PGM索引的主要优势：
 * 1. 空间效率：通常比B树等传统索引结构更节省空间
 * 2. 查询速度：预测位置通常只需要一次模型计算和少量的局部搜索
 * 3. 可调整的误差-空间权衡：通过调整epsilon参数，可以在预测精度和索引大小之间进行权衡
 *
 * 参考自: https://github.com/gvinciguerra/PGM-index/blob/master/include/pgm/piecewise_linear_model.hpp
 */

const (
	// 模型的大小，包括起始键、斜率、截距和最后一个索引
	Model_SIZE = 8 + 8 + 8 + 4
)

// Slope 表示斜率
// 在PGM索引中，斜率用于表示线性模型中的斜率，以及在凸包构建过程中比较不同线段的斜率
type Slope struct {
	Dx int64 // X轴差值
	Dy int64 // Y轴差值
}

// Compare 比较两个斜率，返回:
// -1 如果 s < other
// 0 如果 s = other
// 1 如果 s > other
//
// 在凸包算法中，斜率比较是核心操作，用于确定点是否在凸包内部
func (s *Slope) Compare(other *Slope) int {
	diff := s.Dy*other.Dx - s.Dx*other.Dy
	if diff < 0 {
		return -1
	} else if diff > 0 {
		return 1
	}
	return 0
}

// Point 表示一个点，由键和位置组成
// 在PGM索引中，点表示(key, position)对，用于构建凸包和线性模型
type Point struct {
	X int64 // 键值
	Y int64 // 位置
}

// SubtractPoints 计算两点之间的斜率
// 在凸包算法中，需要计算不同点之间的斜率来确定凸包的边界
func SubtractPoints(a, b *Point) Slope {
	return Slope{
		Dx: a.X - b.X,
		Dy: a.Y - b.Y,
	}
}

// Cross 计算叉积
// 叉积用于判断三点的相对位置关系，是凸包算法的核心操作
// 如果叉积为正，表示三点形成的角是逆时针方向；如果为负，表示顺时针方向
func Cross(o, a, b *Point) int64 {
	oa := SubtractPoints(a, o)
	ob := SubtractPoints(b, o)
	return oa.Dx*ob.Dy - oa.Dy*ob.Dx
}

// OptimalPiecewiseLinearModel 原始PGM生成器
// 这是PGM索引的核心组件，负责使用凸包算法构建最优的分段线性模型
type OptimalPiecewiseLinearModel struct {
	Epsilon      int64    // 允许的最大误差，控制模型的精度和大小
	Lower        []Point  // 下凸包，表示模型的下边界
	Upper        []Point  // 上凸包，表示模型的上边界
	FirstX       int64    // 第一个点的X坐标
	FirstY       int64    // 第一个点的Y坐标
	LastX        int64    // 最后一个点的X坐标
	LastY        int64    // 最后一个点的Y坐标
	LowerStart   int      // 下凸包的起始索引
	UpperStart   int      // 上凸包的起始索引
	PointsInHull int      // 凸包中的点数
	Rectangle    [4]Point // 当前凸包的边界矩形，用于快速判断点是否在凸包内
}

// NewOptimalPiecewiseLinearModel 创建一个新的PGM模型
// epsilon参数控制模型的精度，较小的epsilon会产生更精确但更大的模型
func NewOptimalPiecewiseLinearModel(epsilon int64) *OptimalPiecewiseLinearModel {
	return &OptimalPiecewiseLinearModel{
		Epsilon:      epsilon,
		Lower:        make([]Point, 0),
		Upper:        make([]Point, 0),
		FirstX:       0,
		FirstY:       0,
		LastX:        0,
		LastY:        0,
		LowerStart:   0,
		UpperStart:   0,
		PointsInHull: 0,
		Rectangle:    [4]Point{},
	}
}

// AddPoint 添加一个点到模型中
// 这是凸包算法的核心实现，尝试将新点添加到当前凸包中
// 如果新点可以在当前误差范围内被容纳，返回true；否则返回false，表示需要创建新模型
func (m *OptimalPiecewiseLinearModel) AddPoint(x, y int64) (bool, error) {
	// 检查点是否递增，PGM索引要求键值是严格递增的
	if m.PointsInHull > 0 && x <= m.LastX {
		return false, ErrPointNotIncreasing
	}

	m.LastX = x
	m.LastY = y
	// 创建上下两个点，表示误差范围
	p1 := Point{
		X: x,
		Y: y + m.Epsilon, // 上点，表示位置+误差
	}
	p2 := Point{
		X: x,
		Y: y - m.Epsilon, // 下点，表示位置-误差
	}

	// 特殊情况处理：凸包为空
	if m.PointsInHull == 0 {
		m.FirstX = x
		m.FirstY = y
		m.Rectangle[0] = p1
		m.Rectangle[1] = p2
		m.Upper = m.Upper[:0]
		m.Lower = m.Lower[:0]
		m.Upper = append(m.Upper, p1)
		m.Lower = append(m.Lower, p2)
		m.UpperStart = 0
		m.LowerStart = 0
		m.PointsInHull++
		return true, nil
	}

	// 特殊情况处理：凸包只有一个点
	if m.PointsInHull == 1 {
		m.Rectangle[2] = p2
		m.Rectangle[3] = p1
		m.Upper = append(m.Upper, p1)
		m.Lower = append(m.Lower, p2)
		m.PointsInHull++
		return true, nil
	}

	// 计算斜率，用于判断点是否在凸包内
	slope1 := SubtractPoints(&m.Rectangle[2], &m.Rectangle[0])
	slope2 := SubtractPoints(&m.Rectangle[3], &m.Rectangle[1])

	p1MinusRect2 := SubtractPoints(&p1, &m.Rectangle[2])
	p2MinusRect3 := SubtractPoints(&p2, &m.Rectangle[3])

	// 检查新点是否在当前凸包的误差范围内
	outsideLine1 := p1MinusRect2.Compare(&slope1) < 0
	outsideLine2 := p2MinusRect3.Compare(&slope2) > 0

	// 如果新点不在凸包内，返回false，表示需要创建新模型
	if outsideLine1 || outsideLine2 {
		m.PointsInHull = 0
		m.LastY--
		return false, nil
	}

	// 更新下凸包
	p1MinusRect1 := SubtractPoints(&p1, &m.Rectangle[1])
	if p1MinusRect1.Compare(&slope2) < 0 {
		// 找到极端斜率
		min := SubtractPoints(&m.Lower[m.LowerStart], &p1)
		minI := m.LowerStart
		i := m.LowerStart + 1
		for i < len(m.Lower) {
			val := SubtractPoints(&m.Lower[i], &p1)
			if val.Compare(&min) > 0 {
				break
			}
			min = val
			minI = i
			i++
		}

		m.Rectangle[1] = m.Lower[minI]
		m.Rectangle[3] = p1
		m.LowerStart = minI

		// 更新凸包，移除不再在凸包边界上的点
		end := len(m.Upper)
		for end >= m.UpperStart+2 && Cross(&m.Upper[end-2], &m.Upper[end-1], &p1) <= 0 {
			end--
		}

		m.Upper = m.Upper[:end]
		m.Upper = append(m.Upper, p1)
	}

	// 更新上凸包
	p2MinusRect0 := SubtractPoints(&p2, &m.Rectangle[0])
	if p2MinusRect0.Compare(&slope1) > 0 {
		// 找到极端斜率
		max := SubtractPoints(&m.Upper[m.UpperStart], &p2)
		maxI := m.UpperStart
		i := m.UpperStart + 1
		for i < len(m.Upper) {
			val := SubtractPoints(&m.Upper[i], &p2)
			if val.Compare(&max) < 0 {
				break
			}
			max = val
			maxI = i
			i++
		}

		m.Rectangle[0] = m.Upper[maxI]
		m.Rectangle[2] = p2
		m.UpperStart = maxI

		// 更新凸包，移除不再在凸包边界上的点
		end := len(m.Lower)
		for end >= m.LowerStart+2 && Cross(&m.Lower[end-2], &m.Lower[end-1], &p2) >= 0 {
			end--
		}

		m.Lower = m.Lower[:end]
		m.Lower = append(m.Lower, p2)
	}

	m.PointsInHull++
	return true, nil
}

// Reset 重置模型
// 在完成一个模型段后，需要重置状态以开始构建新的模型段
func (m *OptimalPiecewiseLinearModel) Reset() {
	m.PointsInHull = 0
	m.Lower = m.Lower[:0]
	m.Upper = m.Upper[:0]
}

// GetSegment 获取规范线段
// 从凸包中提取一个线性模型，该模型是上凸包和下凸包所定义的所有可能线性模型的"中间"模型
func (m *OptimalPiecewiseLinearModel) GetSegment() *CanonicalSegment {
	if m.PointsInHull == 1 {
		return NewCanonicalSegment(&m.Rectangle[0], &m.Rectangle[1], m.LastY)
	}
	return NewCanonicalSegmentLong(&m.Rectangle, m.LastY)
}

// GetPointsInHull 获取凸包中的点数
func (m *OptimalPiecewiseLinearModel) GetPointsInHull() int {
	return m.PointsInHull
}

// CanonicalSegment 规范线段
// 表示从凸包中提取的线性模型
type CanonicalSegment struct {
	Rectangle [4]Point // 边界矩形，定义了线性模型的范围
	LastY     int64    // 最后一个点的Y坐标
}

// NewCanonicalSegment 创建一个新的规范线段
// 用于只有一个点的情况
func NewCanonicalSegment(p0, p1 *Point, lastY int64) *CanonicalSegment {
	return &CanonicalSegment{
		Rectangle: [4]Point{*p0, *p1, *p0, *p1},
		LastY:     lastY,
	}
}

// NewCanonicalSegmentLong 创建一个新的规范线段（长版本）
// 用于有多个点的情况
func NewCanonicalSegmentLong(rec *[4]Point, lastY int64) *CanonicalSegment {
	return &CanonicalSegment{
		Rectangle: [4]Point{rec[0], rec[1], rec[2], rec[3]},
		LastY:     lastY,
	}
}

// OnePoint 判断是否只有一个点
// 如果规范线段只表示一个点，则返回true
func (c *CanonicalSegment) OnePoint() bool {
	return c.Rectangle[0].X == c.Rectangle[2].X && c.Rectangle[0].Y == c.Rectangle[2].Y &&
		c.Rectangle[1].X == c.Rectangle[3].X && c.Rectangle[1].Y == c.Rectangle[3].Y
}

// GetLastY 获取最后的Y值
func (c *CanonicalSegment) GetLastY() int64 {
	return c.LastY
}

// GetIntersection 获取交点
// 计算上凸包和下凸包的交点，这个交点定义了线性模型
func (c *CanonicalSegment) GetIntersection() (float64, float64) {
	p0 := &c.Rectangle[0]
	p1 := &c.Rectangle[1]
	p2 := &c.Rectangle[2]
	p3 := &c.Rectangle[3]

	slope1 := SubtractPoints(p2, p0)
	slope2 := SubtractPoints(p3, p1)

	if c.OnePoint() || slope1.Compare(&slope2) == 0 {
		return float64(p0.X), float64(p0.Y)
	}

	// 计算两条线的交点
	p0p1 := SubtractPoints(p1, p0)
	a := slope1.Dx*slope2.Dy - slope1.Dy*slope2.Dx
	b := float64(p0p1.Dx*slope2.Dy-p0p1.Dy*slope2.Dx) / float64(a)

	iX := float64(p0.X) + b*float64(slope1.Dx)
	iY := float64(p0.Y) + b*float64(slope1.Dy)

	return iX, iY
}

// GetFloatingPointSegment 获取浮点线段
// 从规范线段中提取线性模型的斜率和截距
func (c *CanonicalSegment) GetFloatingPointSegment(origin int64) (float64, float64) {
	if c.OnePoint() {
		return 0, float64(c.Rectangle[0].Y+c.Rectangle[1].Y) / 2
	}

	// 计算交点和斜率范围
	iX, iY := c.GetIntersection()
	minSlope, maxSlope := c.GetSlopeRange()

	// 取平均斜率作为模型的斜率
	slope := (minSlope + maxSlope) / 2

	// 计算截距
	intercept := iY - (iX-float64(origin))*slope

	return slope, intercept
}

// GetSlopeRange 获取斜率范围
// 返回线性模型可能的最小和最大斜率
func (c *CanonicalSegment) GetSlopeRange() (float64, float64) {
	if c.OnePoint() {
		return 0, 1
	}

	// 计算最小和最大斜率
	minSlope := SubtractPoints(&c.Rectangle[2], &c.Rectangle[0])
	maxSlope := SubtractPoints(&c.Rectangle[3], &c.Rectangle[1])

	return float64(minSlope.Dy) / float64(minSlope.Dx), float64(maxSlope.Dy) / float64(maxSlope.Dx)
}

// KeyModel 键模型
// 表示一个数据段的线性模型
type KeyModel struct {
	Start     Key     // 该段的起始键
	Slope     float64 // 线性模型的斜率
	Intercept float64 // 线性模型的截距
	LastIndex uint32  // 该段的最后一个位置
}

// ModelGenerator 模型生成器
// 负责构建PGM索引的模型段
type ModelGenerator struct {
	Start    Key                          // 当前模型段的起始键
	Pgm      *OptimalPiecewiseLinearModel // 用于构建凸包的模型
	HasStart bool                         // 是否已设置起始键
}

// NewModelGenerator 创建一个新的模型生成器
// epsilon参数控制模型的精度
func NewModelGenerator(epsilon int64) *ModelGenerator {
	return &ModelGenerator{
		Start:    0,
		Pgm:      NewOptimalPiecewiseLinearModel(epsilon),
		HasStart: false,
	}
}

// Append 添加一个键值对到模型中
// 尝试将键值对添加到当前模型段，如果成功返回true，否则返回false表示需要创建新模型段
func (m *ModelGenerator) Append(key Key, pos int) bool {
	// 设置模型的起始键
	if !m.HasStart {
		m.Start = key
		m.HasStart = true
	}

	// 处理相同键的情况
	if key == Key(m.Pgm.LastX) && int64(pos) == m.Pgm.LastY {
		return true
	}

	// 尝试将点插入到凸包中
	result, _ := m.Pgm.AddPoint(int64(key), int64(pos))
	return result
}

// FinalizeModel 完成模型
// 从当前凸包中提取线性模型，并重置状态以开始构建新的模型段
func (m *ModelGenerator) FinalizeModel() *KeyModel {
	// 获取规范线段
	seg := m.Pgm.GetSegment()
	start := m.Start
	lastPos := seg.GetLastY()

	// 提取线性模型的斜率和截距
	slope, intercept := seg.GetFloatingPointSegment(int64(start))

	// 重置状态
	m.Pgm.Reset()
	m.HasStart = false

	// 创建并返回模型
	return &KeyModel{
		Start:     start,
		Slope:     slope,
		Intercept: intercept,
		LastIndex: uint32(lastPos),
	}
}

// IsHullEmpty 判断凸包是否为空
// 用于检查是否有未完成的模型段
func (m *ModelGenerator) IsHullEmpty() bool {
	return m.Pgm.GetPointsInHull() == 0
}

// MODEL_SIZE 模型的字节大小
// Key(8) + slope(8) + intercept(8) + lastIndex(4) = 28字节
const MODEL_SIZE = 8 + 8 + 8 + 4

// GetSlopeIntercept 获取斜率和截距
// 返回线性模型的斜率和截距
func (k *KeyModel) GetSlopeIntercept() (float64, float64) {
	return k.Slope, k.Intercept
}

// GetStart 获取起始键
// 返回该模型段的起始键
func (k *KeyModel) GetStart() Key {
	return k.Start
}

// GetLastIndex 获取最后索引
// 返回该模型段的最后一个位置
func (k *KeyModel) GetLastIndex() uint32 {
	return k.LastIndex
}

// Predict 预测位置
// 使用线性模型预测给定键的位置
func (k *KeyModel) Predict(key Key) int {
	// 使用线性模型计算预测位置
	pos := float64(key-k.Start)*k.Slope + k.Intercept
	posInteger := int(math.Floor(pos))
	maxIndex := int(k.LastIndex)

	// 确保预测位置不超过最大索引
	if posInteger > maxIndex {
		return maxIndex
	}
	return posInteger
}

// ToBytes 将模型转换为字节
// 用于序列化模型以便存储或传输
func (k *KeyModel) ToBytes() []byte {
	totalBytes := make([]byte, MODEL_SIZE)
	binary.BigEndian.PutUint64(totalBytes[0:8], uint64(k.Start))
	binary.BigEndian.PutUint64(totalBytes[8:16], math.Float64bits(k.Slope))
	binary.BigEndian.PutUint64(totalBytes[16:24], math.Float64bits(k.Intercept))
	binary.BigEndian.PutUint32(totalBytes[24:28], k.LastIndex)

	return totalBytes
}

// KeyModelFromBytes 从字节创建模型
// 用于反序列化存储或传输的模型
func KeyModelFromBytes(bytes []byte) *KeyModel {
	start := Key(binary.BigEndian.Uint64(bytes[0:8]))
	slope := math.Float64frombits(binary.BigEndian.Uint64(bytes[8:16]))
	intercept := math.Float64frombits(binary.BigEndian.Uint64(bytes[16:24]))
	lastIndex := binary.BigEndian.Uint32(bytes[24:28])

	return &KeyModel{
		Start:     start,
		Slope:     slope,
		Intercept: intercept,
		LastIndex: lastIndex,
	}
}

// FetchModelAndPredict 获取模型并预测
// 在模型数组中找到适用于给定键的模型，并使用该模型预测位置
func FetchModelAndPredict(finalModels []*KeyModel, key Key) int {
	modelLen := len(finalModels)
	if modelLen == 0 {
		return 0
	}

	// 使用二分查找找到适用的模型
	var index int
	l := 0
	r := modelLen - 1

	for l <= r && l >= 0 && r <= modelLen-1 {
		m := l + (r-l)/2
		if finalModels[m].Start < key {
			l = m + 1
		} else if finalModels[m].Start > key {
			r = m - 1
		} else {
			// 找到精确匹配的模型
			index = m
			return finalModels[index].Predict(key)
		}
	}

	// 处理边界情况
	index = l

	if index == modelLen {
		index--
	}

	if key < finalModels[index].Start && index > 0 {
		index--
	}

	// 使用找到的模型预测位置
	return finalModels[index].Predict(key)
}

// ErrPointNotIncreasing 点不递增错误
// PGM索引要求键值是严格递增的
var ErrPointNotIncreasing = NewError("point does not increase")

// Error 自定义错误
type Error struct {
	message string
}

// NewError 创建一个新的错误
func NewError(message string) *Error {
	return &Error{message: message}
}

// Error 实现error接口
func (e *Error) Error() string {
	return e.message
}
