# storage2 第二阶段开发计划

基于 Lance Rust 功能对比分析，制定以下开发计划。

**更新日期**: 2026-03-15

---

## 概览

| 优先级 | 功能模块 | 预估代码量 | 依赖 |
|--------|----------|------------|------|
| **P1** | 向量量化 (PQ/SQ/RQ) | ~1200 行 | IVFIndex, HNSWIndex |
| **P2** | 全文搜索索引 (FTS) | ~1500 行 | 无 |
| **P3** | 删除位图加载 | ~200 行 | DeletionFile 结构 |
| **P4** | 索引查询规划 | ~400 行 | IndexSelector |
| **P5** | 向量索引增量更新 | ~500 行 | 向量索引 |
| **P6** | DynamoDB 外部锁 | ~600 行 | AWS SDK |
| **合计** | | **~4400 行** | |

---

## P1: 向量量化 (Product Quantization / Scalar Quantization)

### 1.1 背景与目标

当前 `IVFIndex` 和 `HNSWIndex` 存储原始 float32 向量，内存占用大：
- 1M 向量 × 768 维 × 4 bytes = **3GB 内存**

量化可将内存压缩 4-32 倍：
- **PQ (乘积量化)**: 768 维 → 96 bytes (8 倍压缩)
- **SQ (标量量化)**: float32 → int8 (4 倍压缩)
- **RQ (RaBitQ)**: float32 → 1 bit/dim (32 倍压缩)

### 1.2 设计方案

#### 1.2.1 量化接口定义

```go
// quantization.go (新文件)

package storage2

// QuantizationType 量化类型
type QuantizationType int

const (
    QuantizationNone QuantizationType = iota
    QuantizationPQ   // Product Quantization
    QuantizationSQ   // Scalar Quantization (int8)
    QuantizationRQ   // RaBitQ (1-bit per dimension)
)

// Quantizer 量化器接口
type Quantizer interface {
    // Train 训练量化器
    Train(vectors [][]float32) error
    
    // Encode 编码向量
    Encode(vector []float32) ([]byte, error)
    
    // Decode 解码向量 (用于精确距离计算)
    Decode(code []byte) ([]float32, error)
    
    // ComputeDistance 使用量化码计算距离
    ComputeDistance(query []float32, code []byte) (float32, error)
    
    // ComputeDistanceTable 预计算查询向量的距离表 (PQ 优化)
    ComputeDistanceTable(query []float32) (interface{}, error)
    
    // ComputeDistanceWithTable 使用距离表计算距离
    ComputeDistanceWithTable(table interface{}, code []byte) float32
    
    // CodeSize 返回编码后的字节数
    CodeSize() int
    
    // Type 返回量化类型
    Type() QuantizationType
}
```

#### 1.2.2 PQ (Product Quantization) 实现

```go
// pq_quantizer.go (新文件)

package storage2

import (
    "encoding/binary"
    "fmt"
    "math"
    "math/rand"
)

// PQQuantizer 乘积量化器
type PQQuantizer struct {
    dimension    int       // 原始向量维度
    numSubvectors int      // 子向量数量 (M)
    bitsPerCode  int       // 每个子向量的编码位数 (通常 8)
    numCentroids int       // 每个子向量的聚类中心数 (2^bitsPerCode)
    subDimension int       // 每个子向量的维度 (dimension / numSubvectors)
    
    // 码本: [numSubvectors][numCentroids][subDimension]
    codebooks [][][]float32
    
    metricType MetricType
    trained    bool
}

// PQConfig PQ 配置
type PQConfig struct {
    Dimension     int        // 向量维度
    NumSubvectors int        // 子向量数量 (默认 dimension/8)
    BitsPerCode   int        // 编码位数 (默认 8，即 256 个聚类中心)
    MetricType    MetricType // 距离类型
    KMeansIters   int        // K-means 迭代次数
}

// NewPQQuantizer 创建 PQ 量化器
func NewPQQuantizer(cfg PQConfig) (*PQQuantizer, error) {
    if cfg.Dimension <= 0 {
        return nil, fmt.Errorf("dimension must be positive")
    }
    
    numSubvectors := cfg.NumSubvectors
    if numSubvectors <= 0 {
        numSubvectors = cfg.Dimension / 8
        if numSubvectors < 1 {
            numSubvectors = 1
        }
    }
    
    if cfg.Dimension % numSubvectors != 0 {
        return nil, fmt.Errorf("dimension %d must be divisible by numSubvectors %d", 
            cfg.Dimension, numSubvectors)
    }
    
    bitsPerCode := cfg.BitsPerCode
    if bitsPerCode <= 0 {
        bitsPerCode = 8
    }
    if bitsPerCode > 8 {
        return nil, fmt.Errorf("bitsPerCode must be <= 8")
    }
    
    return &PQQuantizer{
        dimension:     cfg.Dimension,
        numSubvectors: numSubvectors,
        bitsPerCode:   bitsPerCode,
        numCentroids:  1 << bitsPerCode,
        subDimension:  cfg.Dimension / numSubvectors,
        metricType:    cfg.MetricType,
        trained:       false,
    }, nil
}

// Train 训练 PQ 码本
func (pq *PQQuantizer) Train(vectors [][]float32) error {
    if len(vectors) == 0 {
        return fmt.Errorf("no training vectors provided")
    }
    
    // 初始化码本
    pq.codebooks = make([][][]float32, pq.numSubvectors)
    
    // 为每个子向量空间训练独立的 K-means
    for m := 0; m < pq.numSubvectors; m++ {
        // 提取第 m 个子向量
        subVectors := make([][]float32, len(vectors))
        for i, vec := range vectors {
            start := m * pq.subDimension
            end := start + pq.subDimension
            subVectors[i] = vec[start:end]
        }
        
        // K-means 聚类
        centroids, err := pq.kmeans(subVectors, pq.numCentroids, 20)
        if err != nil {
            return fmt.Errorf("kmeans for subvector %d: %w", m, err)
        }
        
        pq.codebooks[m] = centroids
    }
    
    pq.trained = true
    return nil
}

// Encode 编码向量为 PQ 码
func (pq *PQQuantizer) Encode(vector []float32) ([]byte, error) {
    if !pq.trained {
        return nil, fmt.Errorf("quantizer not trained")
    }
    if len(vector) != pq.dimension {
        return nil, fmt.Errorf("vector dimension mismatch: got %d, expected %d", 
            len(vector), pq.dimension)
    }
    
    code := make([]byte, pq.numSubvectors)
    
    for m := 0; m < pq.numSubvectors; m++ {
        // 提取子向量
        start := m * pq.subDimension
        end := start + pq.subDimension
        subVector := vector[start:end]
        
        // 找到最近的聚类中心
        minDist := float32(math.MaxFloat32)
        minIdx := 0
        
        for k := 0; k < pq.numCentroids; k++ {
            dist := pq.l2DistanceSquared(subVector, pq.codebooks[m][k])
            if dist < minDist {
                minDist = dist
                minIdx = k
            }
        }
        
        code[m] = byte(minIdx)
    }
    
    return code, nil
}

// Decode 解码 PQ 码为近似向量
func (pq *PQQuantizer) Decode(code []byte) ([]float32, error) {
    if !pq.trained {
        return nil, fmt.Errorf("quantizer not trained")
    }
    if len(code) != pq.numSubvectors {
        return nil, fmt.Errorf("code length mismatch")
    }
    
    vector := make([]float32, pq.dimension)
    
    for m := 0; m < pq.numSubvectors; m++ {
        centroidIdx := int(code[m])
        start := m * pq.subDimension
        copy(vector[start:], pq.codebooks[m][centroidIdx])
    }
    
    return vector, nil
}

// ComputeDistanceTable 预计算查询向量到所有聚类中心的距离
func (pq *PQQuantizer) ComputeDistanceTable(query []float32) (interface{}, error) {
    if !pq.trained {
        return nil, fmt.Errorf("quantizer not trained")
    }
    
    // 距离表: [numSubvectors][numCentroids]
    table := make([][]float32, pq.numSubvectors)
    
    for m := 0; m < pq.numSubvectors; m++ {
        table[m] = make([]float32, pq.numCentroids)
        
        start := m * pq.subDimension
        end := start + pq.subDimension
        subQuery := query[start:end]
        
        for k := 0; k < pq.numCentroids; k++ {
            table[m][k] = pq.l2DistanceSquared(subQuery, pq.codebooks[m][k])
        }
    }
    
    return table, nil
}

// ComputeDistanceWithTable 使用距离表计算距离 (ADC: Asymmetric Distance Computation)
func (pq *PQQuantizer) ComputeDistanceWithTable(table interface{}, code []byte) float32 {
    distTable := table.([][]float32)
    
    var dist float32
    for m := 0; m < pq.numSubvectors; m++ {
        centroidIdx := int(code[m])
        dist += distTable[m][centroidIdx]
    }
    
    return float32(math.Sqrt(float64(dist)))
}

// ComputeDistance 直接计算距离 (较慢，用于单次查询)
func (pq *PQQuantizer) ComputeDistance(query []float32, code []byte) (float32, error) {
    table, err := pq.ComputeDistanceTable(query)
    if err != nil {
        return 0, err
    }
    return pq.ComputeDistanceWithTable(table, code), nil
}

// CodeSize 返回编码字节数
func (pq *PQQuantizer) CodeSize() int {
    return pq.numSubvectors // 每个子向量 1 字节
}

// Type 返回量化类型
func (pq *PQQuantizer) Type() QuantizationType {
    return QuantizationPQ
}

// kmeans 执行 K-means 聚类
func (pq *PQQuantizer) kmeans(vectors [][]float32, k int, maxIters int) ([][]float32, error) {
    if len(vectors) == 0 {
        return nil, fmt.Errorf("no vectors for kmeans")
    }
    
    dim := len(vectors[0])
    
    // 随机初始化聚类中心
    centroids := make([][]float32, k)
    for i := 0; i < k; i++ {
        centroids[i] = make([]float32, dim)
        copy(centroids[i], vectors[rand.Intn(len(vectors))])
    }
    
    // 迭代
    assignments := make([]int, len(vectors))
    
    for iter := 0; iter < maxIters; iter++ {
        // 分配阶段
        for i, vec := range vectors {
            minDist := float32(math.MaxFloat32)
            minIdx := 0
            for j, centroid := range centroids {
                dist := pq.l2DistanceSquared(vec, centroid)
                if dist < minDist {
                    minDist = dist
                    minIdx = j
                }
            }
            assignments[i] = minIdx
        }
        
        // 更新阶段
        newCentroids := make([][]float32, k)
        counts := make([]int, k)
        
        for i := 0; i < k; i++ {
            newCentroids[i] = make([]float32, dim)
        }
        
        for i, vec := range vectors {
            cluster := assignments[i]
            counts[cluster]++
            for j := 0; j < dim; j++ {
                newCentroids[cluster][j] += vec[j]
            }
        }
        
        for i := 0; i < k; i++ {
            if counts[i] > 0 {
                for j := 0; j < dim; j++ {
                    newCentroids[i][j] /= float32(counts[i])
                }
                centroids[i] = newCentroids[i]
            }
        }
    }
    
    return centroids, nil
}

// l2DistanceSquared 计算 L2 距离的平方
func (pq *PQQuantizer) l2DistanceSquared(a, b []float32) float32 {
    var sum float32
    for i := 0; i < len(a) && i < len(b); i++ {
        diff := a[i] - b[i]
        sum += diff * diff
    }
    return sum
}

// Marshal 序列化 PQ 码本
func (pq *PQQuantizer) Marshal() ([]byte, error) {
    // 简化序列化: 配置 + 码本数据
    // 格式: [4]dimension [4]numSubvectors [4]bitsPerCode [4]metricType [codebook data]
    
    headerSize := 16
    codebookSize := pq.numSubvectors * pq.numCentroids * pq.subDimension * 4
    data := make([]byte, headerSize+codebookSize)
    
    binary.LittleEndian.PutUint32(data[0:4], uint32(pq.dimension))
    binary.LittleEndian.PutUint32(data[4:8], uint32(pq.numSubvectors))
    binary.LittleEndian.PutUint32(data[8:12], uint32(pq.bitsPerCode))
    binary.LittleEndian.PutUint32(data[12:16], uint32(pq.metricType))
    
    offset := headerSize
    for m := 0; m < pq.numSubvectors; m++ {
        for k := 0; k < pq.numCentroids; k++ {
            for d := 0; d < pq.subDimension; d++ {
                bits := math.Float32bits(pq.codebooks[m][k][d])
                binary.LittleEndian.PutUint32(data[offset:], bits)
                offset += 4
            }
        }
    }
    
    return data, nil
}

// Unmarshal 反序列化 PQ 码本
func (pq *PQQuantizer) Unmarshal(data []byte) error {
    if len(data) < 16 {
        return fmt.Errorf("invalid data length")
    }
    
    pq.dimension = int(binary.LittleEndian.Uint32(data[0:4]))
    pq.numSubvectors = int(binary.LittleEndian.Uint32(data[4:8]))
    pq.bitsPerCode = int(binary.LittleEndian.Uint32(data[8:12]))
    pq.metricType = MetricType(binary.LittleEndian.Uint32(data[12:16]))
    pq.numCentroids = 1 << pq.bitsPerCode
    pq.subDimension = pq.dimension / pq.numSubvectors
    
    // 读取码本
    pq.codebooks = make([][][]float32, pq.numSubvectors)
    offset := 16
    
    for m := 0; m < pq.numSubvectors; m++ {
        pq.codebooks[m] = make([][]float32, pq.numCentroids)
        for k := 0; k < pq.numCentroids; k++ {
            pq.codebooks[m][k] = make([]float32, pq.subDimension)
            for d := 0; d < pq.subDimension; d++ {
                bits := binary.LittleEndian.Uint32(data[offset:])
                pq.codebooks[m][k][d] = math.Float32frombits(bits)
                offset += 4
            }
        }
    }
    
    pq.trained = true
    return nil
}
```

#### 1.2.3 SQ (Scalar Quantization) 实现

```go
// sq_quantizer.go (新文件)

package storage2

// SQQuantizer 标量量化器 (int8)
type SQQuantizer struct {
    dimension  int
    mins       []float32  // 每个维度的最小值
    scales     []float32  // 每个维度的缩放因子
    metricType MetricType
    trained    bool
}

// NewSQQuantizer 创建 SQ 量化器
func NewSQQuantizer(dimension int, metric MetricType) *SQQuantizer {
    return &SQQuantizer{
        dimension:  dimension,
        metricType: metric,
    }
}

// Train 训练 SQ 量化器 (计算每个维度的 min/max)
func (sq *SQQuantizer) Train(vectors [][]float32) error {
    if len(vectors) == 0 {
        return fmt.Errorf("no training vectors")
    }
    
    sq.mins = make([]float32, sq.dimension)
    maxs := make([]float32, sq.dimension)
    sq.scales = make([]float32, sq.dimension)
    
    // 初始化
    for d := 0; d < sq.dimension; d++ {
        sq.mins[d] = math.MaxFloat32
        maxs[d] = -math.MaxFloat32
    }
    
    // 统计每个维度的 min/max
    for _, vec := range vectors {
        for d := 0; d < sq.dimension && d < len(vec); d++ {
            if vec[d] < sq.mins[d] {
                sq.mins[d] = vec[d]
            }
            if vec[d] > maxs[d] {
                maxs[d] = vec[d]
            }
        }
    }
    
    // 计算缩放因子
    for d := 0; d < sq.dimension; d++ {
        rangeVal := maxs[d] - sq.mins[d]
        if rangeVal > 0 {
            sq.scales[d] = 255.0 / rangeVal
        } else {
            sq.scales[d] = 1.0
        }
    }
    
    sq.trained = true
    return nil
}

// Encode 编码向量为 int8
func (sq *SQQuantizer) Encode(vector []float32) ([]byte, error) {
    if !sq.trained {
        return nil, fmt.Errorf("quantizer not trained")
    }
    
    code := make([]byte, sq.dimension)
    for d := 0; d < sq.dimension; d++ {
        val := (vector[d] - sq.mins[d]) * sq.scales[d]
        if val < 0 {
            val = 0
        }
        if val > 255 {
            val = 255
        }
        code[d] = byte(val)
    }
    return code, nil
}

// Decode 解码 int8 为 float32
func (sq *SQQuantizer) Decode(code []byte) ([]float32, error) {
    if !sq.trained {
        return nil, fmt.Errorf("quantizer not trained")
    }
    
    vector := make([]float32, sq.dimension)
    for d := 0; d < sq.dimension; d++ {
        vector[d] = float32(code[d])/sq.scales[d] + sq.mins[d]
    }
    return vector, nil
}

// ComputeDistance 计算距离
func (sq *SQQuantizer) ComputeDistance(query []float32, code []byte) (float32, error) {
    decoded, err := sq.Decode(code)
    if err != nil {
        return 0, err
    }
    return l2Distance(query, decoded), nil
}

// ComputeDistanceTable SQ 不需要距离表
func (sq *SQQuantizer) ComputeDistanceTable(query []float32) (interface{}, error) {
    return query, nil
}

// ComputeDistanceWithTable SQ 直接计算
func (sq *SQQuantizer) ComputeDistanceWithTable(table interface{}, code []byte) float32 {
    query := table.([]float32)
    decoded, _ := sq.Decode(code)
    return l2Distance(query, decoded)
}

// CodeSize 返回编码字节数
func (sq *SQQuantizer) CodeSize() int {
    return sq.dimension
}

// Type 返回量化类型
func (sq *SQQuantizer) Type() QuantizationType {
    return QuantizationSQ
}
```

#### 1.2.4 集成到 IVFIndex

```go
// ivf_pq_index.go (新文件)

package storage2

// IVFPQIndex 带 PQ 量化的 IVF 索引
type IVFPQIndex struct {
    *IVFIndex
    quantizer    Quantizer
    codes        map[uint64][]byte  // rowID -> PQ code
    useQuantizer bool
}

// NewIVFPQIndex 创建 IVF-PQ 索引
func NewIVFPQIndex(name string, columnIdx int, dimension int, metric MetricType, 
    quantizerType QuantizationType) (*IVFPQIndex, error) {
    
    ivf := NewIVFIndex(name, columnIdx, dimension, metric)
    
    var quantizer Quantizer
    var err error
    
    switch quantizerType {
    case QuantizationPQ:
        quantizer, err = NewPQQuantizer(PQConfig{
            Dimension:     dimension,
            NumSubvectors: dimension / 8,
            BitsPerCode:   8,
            MetricType:    metric,
        })
    case QuantizationSQ:
        quantizer = NewSQQuantizer(dimension, metric)
    case QuantizationNone:
        // 不使用量化
    default:
        return nil, fmt.Errorf("unsupported quantizer type: %v", quantizerType)
    }
    
    if err != nil {
        return nil, err
    }
    
    return &IVFPQIndex{
        IVFIndex:     ivf,
        quantizer:    quantizer,
        codes:        make(map[uint64][]byte),
        useQuantizer: quantizer != nil,
    }, nil
}

// Train 训练 IVF 聚类 + PQ 量化器
func (idx *IVFPQIndex) Train(vectors [][]float32) error {
    // 1. 训练 IVF 聚类中心
    if err := idx.IVFIndex.Train(vectors); err != nil {
        return err
    }
    
    // 2. 训练量化器
    if idx.useQuantizer && idx.quantizer != nil {
        // 计算残差向量 (向量 - 聚类中心)
        residuals := make([][]float32, len(vectors))
        for i, vec := range vectors {
            // 找到最近的聚类中心
            minDist := float32(math.MaxFloat32)
            minCentroid := 0
            for j, centroid := range idx.centroids {
                if centroid != nil {
                    dist := l2Distance(vec, centroid)
                    if dist < minDist {
                        minDist = dist
                        minCentroid = j
                    }
                }
            }
            
            // 计算残差
            residuals[i] = make([]float32, idx.dimension)
            for d := 0; d < idx.dimension; d++ {
                residuals[i][d] = vec[d] - idx.centroids[minCentroid][d]
            }
        }
        
        // 用残差训练 PQ
        if err := idx.quantizer.Train(residuals); err != nil {
            return fmt.Errorf("train quantizer: %w", err)
        }
    }
    
    return nil
}

// Insert 插入向量
func (idx *IVFPQIndex) Insert(rowID uint64, vector []float32) error {
    idx.mu.Lock()
    defer idx.mu.Unlock()
    
    // 找到最近的聚类中心
    minDist := float32(math.MaxFloat32)
    closestCluster := 0
    
    for i, centroid := range idx.centroids {
        if centroid != nil {
            dist := l2Distance(vector, centroid)
            if dist < minDist {
                minDist = dist
                closestCluster = i
            }
        }
    }
    
    // 添加到倒排列表
    idx.invertedLists[closestCluster] = append(idx.invertedLists[closestCluster], rowID)
    
    if idx.useQuantizer {
        // 计算残差并量化
        residual := make([]float32, idx.dimension)
        for d := 0; d < idx.dimension; d++ {
            residual[d] = vector[d] - idx.centroids[closestCluster][d]
        }
        
        code, err := idx.quantizer.Encode(residual)
        if err != nil {
            return err
        }
        idx.codes[rowID] = code
    } else {
        // 存储原始向量
        idx.vectors[rowID] = make([]float32, len(vector))
        copy(idx.vectors[rowID], vector)
    }
    
    idx.stats.NumEntries++
    return nil
}

// ANNSearch 近似最近邻搜索
func (idx *IVFPQIndex) ANNSearch(ctx context.Context, queryVector []float32, limit int) ([]float32, []uint64, error) {
    idx.mu.RLock()
    defer idx.mu.RUnlock()
    
    if !idx.useQuantizer {
        return idx.IVFIndex.ANNSearch(ctx, queryVector, limit)
    }
    
    // 找到 nprobe 个最近的聚类
    centroidDistances := make([]struct {
        idx      int
        distance float32
    }, idx.nlist)
    
    for i, centroid := range idx.centroids {
        if centroid != nil {
            centroidDistances[i].idx = i
            centroidDistances[i].distance = l2Distance(queryVector, centroid)
        } else {
            centroidDistances[i].idx = i
            centroidDistances[i].distance = math.MaxFloat32
        }
    }
    
    sort.Slice(centroidDistances, func(i, j int) bool {
        return centroidDistances[i].distance < centroidDistances[j].distance
    })
    
    // 搜索每个聚类
    type result struct {
        rowID    uint64
        distance float32
    }
    var results []result
    
    for i := 0; i < idx.nprobe && i < idx.nlist; i++ {
        clusterIdx := centroidDistances[i].idx
        centroid := idx.centroids[clusterIdx]
        
        // 计算查询向量相对于聚类中心的残差
        queryResidual := make([]float32, idx.dimension)
        for d := 0; d < idx.dimension; d++ {
            queryResidual[d] = queryVector[d] - centroid[d]
        }
        
        // 预计算距离表
        distTable, _ := idx.quantizer.ComputeDistanceTable(queryResidual)
        
        // 搜索该聚类中的所有向量
        for _, rowID := range idx.invertedLists[clusterIdx] {
            if code, ok := idx.codes[rowID]; ok {
                dist := idx.quantizer.ComputeDistanceWithTable(distTable, code)
                results = append(results, result{rowID: rowID, distance: dist})
            }
        }
    }
    
    // 排序返回 top-k
    sort.Slice(results, func(i, j int) bool {
        return results[i].distance < results[j].distance
    })
    
    if limit > len(results) {
        limit = len(results)
    }
    
    distances := make([]float32, limit)
    rowIDs := make([]uint64, limit)
    for i := 0; i < limit; i++ {
        distances[i] = results[i].distance
        rowIDs[i] = results[i].rowID
    }
    
    return distances, rowIDs, nil
}
```

### 1.3 测试用例

```go
// quantization_test.go

func TestPQQuantizer(t *testing.T) {
    dim := 128
    numVectors := 10000
    
    // 生成随机训练数据
    vectors := make([][]float32, numVectors)
    for i := range vectors {
        vectors[i] = make([]float32, dim)
        for j := range vectors[i] {
            vectors[i][j] = rand.Float32()
        }
    }
    
    // 创建并训练 PQ
    pq, err := NewPQQuantizer(PQConfig{
        Dimension:     dim,
        NumSubvectors: 16,
        BitsPerCode:   8,
        MetricType:    L2Metric,
    })
    if err != nil {
        t.Fatal(err)
    }
    
    if err := pq.Train(vectors[:1000]); err != nil {
        t.Fatal(err)
    }
    
    // 测试编码/解码
    testVec := vectors[0]
    code, err := pq.Encode(testVec)
    if err != nil {
        t.Fatal(err)
    }
    
    if len(code) != 16 {
        t.Errorf("expected code length 16, got %d", len(code))
    }
    
    decoded, err := pq.Decode(code)
    if err != nil {
        t.Fatal(err)
    }
    
    // 验证重建误差在合理范围内
    reconstructionError := l2Distance(testVec, decoded)
    t.Logf("Reconstruction error: %f", reconstructionError)
    
    // 测试距离计算
    query := vectors[1]
    exactDist := l2Distance(query, testVec)
    approxDist, _ := pq.ComputeDistance(query, code)
    
    t.Logf("Exact distance: %f, Approximate distance: %f", exactDist, approxDist)
}

func TestIVFPQIndex(t *testing.T) {
    dim := 128
    numVectors := 10000
    
    // 创建索引
    idx, err := NewIVFPQIndex("test_ivfpq", 0, dim, L2Metric, QuantizationPQ)
    if err != nil {
        t.Fatal(err)
    }
    
    // 生成训练数据
    vectors := make([][]float32, numVectors)
    for i := range vectors {
        vectors[i] = make([]float32, dim)
        for j := range vectors[i] {
            vectors[i][j] = rand.Float32()
        }
    }
    
    // 训练
    if err := idx.Train(vectors[:1000]); err != nil {
        t.Fatal(err)
    }
    
    // 插入
    for i, vec := range vectors {
        if err := idx.Insert(uint64(i), vec); err != nil {
            t.Fatal(err)
        }
    }
    
    // 搜索
    query := vectors[0]
    distances, rowIDs, err := idx.ANNSearch(context.Background(), query, 10)
    if err != nil {
        t.Fatal(err)
    }
    
    t.Logf("Search results: %v", rowIDs)
    t.Logf("Distances: %v", distances)
    
    // 验证第一个结果应该是查询向量本身
    if rowIDs[0] != 0 {
        t.Logf("Warning: first result is not the query vector itself")
    }
}
```

### 1.4 预估工作量

| 文件 | 内容 | 代码行数 |
|------|------|----------|
| `quantization.go` | 接口定义 | ~50 行 |
| `pq_quantizer.go` | PQ 量化器 | ~400 行 |
| `sq_quantizer.go` | SQ 量化器 | ~150 行 |
| `rq_quantizer.go` | RaBitQ 量化器 | ~200 行 |
| `ivf_pq_index.go` | IVF-PQ 集成 | ~300 行 |
| `quantization_test.go` | 测试 | ~200 行 |
| **合计** | | **~1300 行** |

---

## P2: 全文搜索索引 (Full-Text Search)

### 2.1 背景与目标

Lance Rust 提供了基于 Tantivy 的全文搜索索引，支持：
- 倒排索引 (Inverted Index)
- BM25 评分
- 多语言分词
- 短语查询、布尔查询

### 2.2 设计方案

#### 2.2.1 倒排索引结构

```go
// fts_index.go (新文件)

package storage2

import (
    "context"
    "fmt"
    "math"
    "sort"
    "strings"
    "sync"
    "unicode"
)

// FTSIndex 全文搜索索引
type FTSIndex struct {
    name      string
    columnIdx int
    indexType IndexType
    
    // 倒排索引: term -> posting list
    invertedIndex map[string]*PostingList
    
    // 文档统计
    docLengths   map[uint64]int   // docID -> 文档长度
    totalDocs    int              // 总文档数
    avgDocLength float64          // 平均文档长度
    totalTerms   int64            // 总词项数
    
    // 分词器
    tokenizer Tokenizer
    
    mu    sync.RWMutex
    stats IndexStats
}

// PostingList 倒排列表
type PostingList struct {
    DocFreq  int             // 包含该词的文档数
    Postings []Posting       // 文档列表
}

// Posting 单个文档的词项信息
type Posting struct {
    DocID     uint64   // 文档ID (rowID)
    TermFreq  int      // 词频
    Positions []int    // 词项位置 (用于短语查询)
}

// Tokenizer 分词器接口
type Tokenizer interface {
    Tokenize(text string) []Token
}

// Token 词项
type Token struct {
    Term     string
    Position int
}

// SimpleTokenizer 简单分词器 (空白+标点分词)
type SimpleTokenizer struct {
    lowercase bool
}

// NewSimpleTokenizer 创建简单分词器
func NewSimpleTokenizer() *SimpleTokenizer {
    return &SimpleTokenizer{lowercase: true}
}

// Tokenize 分词
func (t *SimpleTokenizer) Tokenize(text string) []Token {
    var tokens []Token
    var currentToken strings.Builder
    position := 0
    
    for _, r := range text {
        if unicode.IsLetter(r) || unicode.IsNumber(r) {
            if t.lowercase {
                currentToken.WriteRune(unicode.ToLower(r))
            } else {
                currentToken.WriteRune(r)
            }
        } else {
            if currentToken.Len() > 0 {
                tokens = append(tokens, Token{
                    Term:     currentToken.String(),
                    Position: position,
                })
                position++
                currentToken.Reset()
            }
        }
    }
    
    if currentToken.Len() > 0 {
        tokens = append(tokens, Token{
            Term:     currentToken.String(),
            Position: position,
        })
    }
    
    return tokens
}

// NewFTSIndex 创建全文搜索索引
func NewFTSIndex(name string, columnIdx int) *FTSIndex {
    return &FTSIndex{
        name:          name,
        columnIdx:     columnIdx,
        indexType:     InvertedIndex,
        invertedIndex: make(map[string]*PostingList),
        docLengths:    make(map[uint64]int),
        tokenizer:     NewSimpleTokenizer(),
    }
}

// Name 返回索引名称
func (idx *FTSIndex) Name() string {
    return idx.name
}

// Type 返回索引类型
func (idx *FTSIndex) Type() IndexType {
    return idx.indexType
}

// Columns 返回列索引
func (idx *FTSIndex) Columns() []int {
    return []int{idx.columnIdx}
}

// IndexDocument 索引文档
func (idx *FTSIndex) IndexDocument(docID uint64, text string) error {
    idx.mu.Lock()
    defer idx.mu.Unlock()
    
    tokens := idx.tokenizer.Tokenize(text)
    if len(tokens) == 0 {
        return nil
    }
    
    // 统计词频和位置
    termStats := make(map[string]*struct {
        freq      int
        positions []int
    })
    
    for _, token := range tokens {
        if _, ok := termStats[token.Term]; !ok {
            termStats[token.Term] = &struct {
                freq      int
                positions []int
            }{}
        }
        termStats[token.Term].freq++
        termStats[token.Term].positions = append(termStats[token.Term].positions, token.Position)
    }
    
    // 更新倒排索引
    for term, stats := range termStats {
        if _, ok := idx.invertedIndex[term]; !ok {
            idx.invertedIndex[term] = &PostingList{
                DocFreq:  0,
                Postings: make([]Posting, 0),
            }
        }
        
        pl := idx.invertedIndex[term]
        pl.DocFreq++
        pl.Postings = append(pl.Postings, Posting{
            DocID:     docID,
            TermFreq:  stats.freq,
            Positions: stats.positions,
        })
    }
    
    // 更新文档统计
    idx.docLengths[docID] = len(tokens)
    idx.totalDocs++
    idx.totalTerms += int64(len(tokens))
    idx.avgDocLength = float64(idx.totalTerms) / float64(idx.totalDocs)
    
    return nil
}

// Search 搜索 (返回 BM25 排序的结果)
func (idx *FTSIndex) Search(ctx context.Context, query interface{}, limit int) ([]uint64, error) {
    queryStr, ok := query.(string)
    if !ok {
        return nil, fmt.Errorf("query must be string")
    }
    
    results, err := idx.SearchWithScores(ctx, queryStr, limit)
    if err != nil {
        return nil, err
    }
    
    rowIDs := make([]uint64, len(results))
    for i, r := range results {
        rowIDs[i] = r.DocID
    }
    return rowIDs, nil
}

// SearchResult 搜索结果
type SearchResult struct {
    DocID uint64
    Score float64
}

// SearchWithScores 带分数的搜索
func (idx *FTSIndex) SearchWithScores(ctx context.Context, queryStr string, limit int) ([]SearchResult, error) {
    idx.mu.RLock()
    defer idx.mu.RUnlock()
    
    tokens := idx.tokenizer.Tokenize(queryStr)
    if len(tokens) == 0 {
        return nil, nil
    }
    
    // 收集所有匹配的文档
    docScores := make(map[uint64]float64)
    
    for _, token := range tokens {
        pl, ok := idx.invertedIndex[token.Term]
        if !ok {
            continue
        }
        
        // 计算 IDF
        idf := idx.computeIDF(pl.DocFreq)
        
        for _, posting := range pl.Postings {
            // 计算 BM25 分数
            tf := float64(posting.TermFreq)
            docLen := float64(idx.docLengths[posting.DocID])
            score := idx.computeBM25(tf, idf, docLen)
            
            docScores[posting.DocID] += score
        }
    }
    
    // 排序
    results := make([]SearchResult, 0, len(docScores))
    for docID, score := range docScores {
        results = append(results, SearchResult{DocID: docID, Score: score})
    }
    
    sort.Slice(results, func(i, j int) bool {
        return results[i].Score > results[j].Score
    })
    
    if limit > len(results) {
        limit = len(results)
    }
    
    return results[:limit], nil
}

// BM25 参数
const (
    bm25K1 = 1.2
    bm25B  = 0.75
)

// computeIDF 计算 IDF
func (idx *FTSIndex) computeIDF(docFreq int) float64 {
    n := float64(idx.totalDocs)
    df := float64(docFreq)
    return math.Log(1 + (n-df+0.5)/(df+0.5))
}

// computeBM25 计算 BM25 分数
func (idx *FTSIndex) computeBM25(tf, idf, docLen float64) float64 {
    numerator := tf * (bm25K1 + 1)
    denominator := tf + bm25K1*(1-bm25B+bm25B*docLen/idx.avgDocLength)
    return idf * numerator / denominator
}

// PhraseSearch 短语搜索
func (idx *FTSIndex) PhraseSearch(ctx context.Context, phrase string, limit int) ([]SearchResult, error) {
    idx.mu.RLock()
    defer idx.mu.RUnlock()
    
    tokens := idx.tokenizer.Tokenize(phrase)
    if len(tokens) < 2 {
        return idx.SearchWithScores(ctx, phrase, limit)
    }
    
    // 获取第一个词的文档列表
    firstTerm := tokens[0].Term
    firstPL, ok := idx.invertedIndex[firstTerm]
    if !ok {
        return nil, nil
    }
    
    var results []SearchResult
    
    // 检查每个包含第一个词的文档
    for _, posting := range firstPL.Postings {
        docID := posting.DocID
        
        // 检查该文档是否包含完整短语
        if idx.documentContainsPhrase(docID, tokens) {
            // 简化: 使用匹配词数作为分数
            score := float64(len(tokens))
            results = append(results, SearchResult{DocID: docID, Score: score})
        }
    }
    
    sort.Slice(results, func(i, j int) bool {
        return results[i].Score > results[j].Score
    })
    
    if limit > len(results) {
        limit = len(results)
    }
    
    return results[:limit], nil
}

// documentContainsPhrase 检查文档是否包含短语
func (idx *FTSIndex) documentContainsPhrase(docID uint64, tokens []Token) bool {
    // 收集每个词在文档中的位置
    termPositions := make([][]int, len(tokens))
    
    for i, token := range tokens {
        pl, ok := idx.invertedIndex[token.Term]
        if !ok {
            return false
        }
        
        for _, posting := range pl.Postings {
            if posting.DocID == docID {
                termPositions[i] = posting.Positions
                break
            }
        }
        
        if termPositions[i] == nil {
            return false
        }
    }
    
    // 检查是否存在连续位置
    for _, pos0 := range termPositions[0] {
        match := true
        for i := 1; i < len(tokens); i++ {
            expectedPos := pos0 + i
            found := false
            for _, pos := range termPositions[i] {
                if pos == expectedPos {
                    found = true
                    break
                }
            }
            if !found {
                match = false
                break
            }
        }
        if match {
            return true
        }
    }
    
    return false
}

// BooleanQuery 布尔查询
type BooleanQuery struct {
    Must    []string  // 必须包含
    Should  []string  // 应该包含 (提高分数)
    MustNot []string  // 不能包含
}

// BooleanSearch 布尔查询搜索
func (idx *FTSIndex) BooleanSearch(ctx context.Context, query BooleanQuery, limit int) ([]SearchResult, error) {
    idx.mu.RLock()
    defer idx.mu.RUnlock()
    
    // 收集 must 条件的文档
    mustDocs := make(map[uint64]float64)
    
    if len(query.Must) > 0 {
        // 初始化为第一个 must 词的文档
        firstTerm := query.Must[0]
        if pl, ok := idx.invertedIndex[firstTerm]; ok {
            for _, posting := range pl.Postings {
                mustDocs[posting.DocID] = float64(posting.TermFreq)
            }
        }
        
        // 与其他 must 词取交集
        for i := 1; i < len(query.Must); i++ {
            term := query.Must[i]
            pl, ok := idx.invertedIndex[term]
            if !ok {
                return nil, nil // 没有匹配
            }
            
            termDocs := make(map[uint64]bool)
            for _, posting := range pl.Postings {
                termDocs[posting.DocID] = true
            }
            
            for docID := range mustDocs {
                if !termDocs[docID] {
                    delete(mustDocs, docID)
                }
            }
        }
    }
    
    // 排除 must_not 文档
    for _, term := range query.MustNot {
        if pl, ok := idx.invertedIndex[term]; ok {
            for _, posting := range pl.Postings {
                delete(mustDocs, posting.DocID)
            }
        }
    }
    
    // 用 should 词提升分数
    for _, term := range query.Should {
        if pl, ok := idx.invertedIndex[term]; ok {
            for _, posting := range pl.Postings {
                if _, ok := mustDocs[posting.DocID]; ok {
                    mustDocs[posting.DocID] += float64(posting.TermFreq)
                }
            }
        }
    }
    
    // 排序返回
    results := make([]SearchResult, 0, len(mustDocs))
    for docID, score := range mustDocs {
        results = append(results, SearchResult{DocID: docID, Score: score})
    }
    
    sort.Slice(results, func(i, j int) bool {
        return results[i].Score > results[j].Score
    })
    
    if limit > len(results) {
        limit = len(results)
    }
    
    return results[:limit], nil
}

// Statistics 返回索引统计
func (idx *FTSIndex) Statistics() IndexStats {
    idx.mu.RLock()
    defer idx.mu.RUnlock()
    
    return IndexStats{
        NumEntries: uint64(idx.totalDocs),
        SizeBytes:  uint64(len(idx.invertedIndex) * 100), // 估算
        IndexType:  "fts",
    }
}

// Ensure FTSIndex implements InvertedIndexImpl
var _ InvertedIndexImpl = (*FTSIndex)(nil)
```

#### 2.2.2 中文分词器

```go
// tokenizer_chinese.go (新文件)

package storage2

import (
    "strings"
    "unicode"
)

// ChineseTokenizer 简单中文分词器 (基于单字切分 + 常用词典)
type ChineseTokenizer struct {
    dictionary map[string]bool
    maxWordLen int
}

// NewChineseTokenizer 创建中文分词器
func NewChineseTokenizer() *ChineseTokenizer {
    return &ChineseTokenizer{
        dictionary: make(map[string]bool),
        maxWordLen: 4,
    }
}

// AddWord 添加词典词
func (t *ChineseTokenizer) AddWord(word string) {
    t.dictionary[word] = true
}

// Tokenize 分词 (正向最大匹配)
func (t *ChineseTokenizer) Tokenize(text string) []Token {
    var tokens []Token
    runes := []rune(text)
    position := 0
    i := 0
    
    for i < len(runes) {
        r := runes[i]
        
        // 非中文字符: 使用简单分词
        if !isChinese(r) {
            if unicode.IsLetter(r) || unicode.IsNumber(r) {
                var word strings.Builder
                for i < len(runes) && (unicode.IsLetter(runes[i]) || unicode.IsNumber(runes[i])) {
                    word.WriteRune(unicode.ToLower(runes[i]))
                    i++
                }
                if word.Len() > 0 {
                    tokens = append(tokens, Token{Term: word.String(), Position: position})
                    position++
                }
            } else {
                i++
            }
            continue
        }
        
        // 中文字符: 正向最大匹配
        matched := false
        for length := t.maxWordLen; length > 1; length-- {
            if i+length > len(runes) {
                continue
            }
            word := string(runes[i : i+length])
            if t.dictionary[word] {
                tokens = append(tokens, Token{Term: word, Position: position})
                position++
                i += length
                matched = true
                break
            }
        }
        
        // 没有匹配到词典词，单字切分
        if !matched {
            tokens = append(tokens, Token{Term: string(r), Position: position})
            position++
            i++
        }
    }
    
    return tokens
}

// isChinese 判断是否是中文字符
func isChinese(r rune) bool {
    return r >= 0x4e00 && r <= 0x9fff
}
```

### 2.3 集成到 IndexManager

```go
// index.go 修改

// CreateInvertedIndex creates an inverted index on a column
func (m *IndexManager) CreateInvertedIndex(ctx context.Context, name string, columnIdx int) error {
    idx := NewFTSIndex(name, columnIdx)
    m.indexes[name] = idx
    return nil
}

// IndexDocument indexes a document in the FTS index
func (m *IndexManager) IndexDocument(ctx context.Context, indexName string, docID uint64, text string) error {
    idx, ok := m.indexes[indexName]
    if !ok {
        return fmt.Errorf("index %s not found", indexName)
    }
    
    ftsIdx, ok := idx.(*FTSIndex)
    if !ok {
        return fmt.Errorf("index %s is not an FTS index", indexName)
    }
    
    return ftsIdx.IndexDocument(docID, text)
}

// FTSSearch performs a full-text search
func (m *IndexManager) FTSSearch(ctx context.Context, indexName string, query string, limit int) ([]SearchResult, error) {
    idx, ok := m.indexes[indexName]
    if !ok {
        return nil, fmt.Errorf("index %s not found", indexName)
    }
    
    ftsIdx, ok := idx.(*FTSIndex)
    if !ok {
        return nil, fmt.Errorf("index %s is not an FTS index", indexName)
    }
    
    return ftsIdx.SearchWithScores(ctx, query, limit)
}
```

### 2.4 预估工作量

| 文件 | 内容 | 代码行数 |
|------|------|----------|
| `fts_index.go` | 全文搜索索引 | ~500 行 |
| `tokenizer_chinese.go` | 中文分词器 | ~150 行 |
| `fts_test.go` | 测试 | ~200 行 |
| `index.go` 修改 | 集成 | ~50 行 |
| **合计** | | **~900 行** |

---

## P3: 删除位图加载

### 3.1 现状

`compaction_worker.go:207` 有 TODO：
```go
// TODO: Load actual deletion bitmap from frag.DeletionFile
```

### 3.2 实现方案

```go
// deletion_bitmap.go (新文件)

package storage2

import (
    "context"
    "encoding/binary"
    "fmt"
    "io"

    "github.com/RoaringBitmap/roaring/roaring64"
)

// DeletionBitmap 删除位图
type DeletionBitmap struct {
    bitmap *roaring64.Bitmap
}

// NewDeletionBitmap 创建删除位图
func NewDeletionBitmap() *DeletionBitmap {
    return &DeletionBitmap{
        bitmap: roaring64.New(),
    }
}

// MarkDeleted 标记行为已删除
func (db *DeletionBitmap) MarkDeleted(rowID uint64) {
    db.bitmap.Add(rowID)
}

// IsDeleted 检查行是否已删除
func (db *DeletionBitmap) IsDeleted(rowID uint64) bool {
    return db.bitmap.Contains(rowID)
}

// Count 返回已删除的行数
func (db *DeletionBitmap) Count() uint64 {
    return db.bitmap.GetCardinality()
}

// Marshal 序列化删除位图
func (db *DeletionBitmap) Marshal() ([]byte, error) {
    return db.bitmap.ToBytes()
}

// Unmarshal 反序列化删除位图
func (db *DeletionBitmap) Unmarshal(data []byte) error {
    db.bitmap = roaring64.New()
    return db.bitmap.UnmarshalBinary(data)
}

// LoadDeletionBitmap 从存储加载删除位图
func LoadDeletionBitmap(ctx context.Context, store ObjectStoreExt, path string) (*DeletionBitmap, error) {
    data, err := store.ReadRange(ctx, path, ReadOptions{})
    if err != nil {
        return nil, fmt.Errorf("read deletion file: %w", err)
    }
    
    db := NewDeletionBitmap()
    if err := db.Unmarshal(data); err != nil {
        return nil, fmt.Errorf("unmarshal deletion bitmap: %w", err)
    }
    
    return db, nil
}

// SaveDeletionBitmap 保存删除位图到存储
func SaveDeletionBitmap(ctx context.Context, store ObjectStoreExt, path string, db *DeletionBitmap) error {
    data, err := db.Marshal()
    if err != nil {
        return fmt.Errorf("marshal deletion bitmap: %w", err)
    }
    
    return store.Write(path, data)
}

// LoadFragmentDeletionBitmap 加载 fragment 的删除位图
func LoadFragmentDeletionBitmap(ctx context.Context, store ObjectStoreExt, basePath string, frag *DataFragment) (*DeletionBitmap, error) {
    if frag.DeletionFile == nil || frag.DeletionFile.Path == "" {
        return nil, nil // 没有删除文件
    }
    
    fullPath := fmt.Sprintf("%s/%s", basePath, frag.DeletionFile.Path)
    return LoadDeletionBitmap(ctx, store, fullPath)
}
```

### 3.3 集成到 compaction_worker.go

```go
// compaction_worker.go 修改

func (w *LocalCompactionWorker) filterDeletedRows(ctx context.Context, frag *DataFragment, chunks []*chunk.Chunk, cfg *WorkerConfig) []*chunk.Chunk {
    if frag.DeletionFile == nil {
        return chunks
    }

    // Load actual deletion bitmap
    deletionBitmap, err := LoadFragmentDeletionBitmap(ctx, w.store, cfg.BasePath, frag)
    if err != nil || deletionBitmap == nil {
        return chunks
    }

    if deletionBitmap.Count() == 0 {
        return chunks
    }

    var filtered []*chunk.Chunk
    var rowOffset uint64

    for _, c := range chunks {
        // 检查该 chunk 是否有已删除的行
        hasDeleted := false
        for i := 0; i < c.Card(); i++ {
            if deletionBitmap.IsDeleted(rowOffset + uint64(i)) {
                hasDeleted = true
                break
            }
        }

        if !hasDeleted {
            filtered = append(filtered, c)
        } else {
            // 创建新 chunk，排除已删除的行
            newChunk := filterChunkRows(c, rowOffset, deletionBitmap)
            if newChunk != nil && newChunk.Card() > 0 {
                filtered = append(filtered, newChunk)
            }
        }

        rowOffset += uint64(c.Card())
    }

    return filtered
}

// filterChunkRows 从 chunk 中过滤已删除的行
func filterChunkRows(c *chunk.Chunk, startRowID uint64, deletionBitmap *DeletionBitmap) *chunk.Chunk {
    // 收集未删除的行索引
    var keepRows []int
    for i := 0; i < c.Card(); i++ {
        if !deletionBitmap.IsDeleted(startRowID + uint64(i)) {
            keepRows = append(keepRows, i)
        }
    }

    if len(keepRows) == 0 {
        return nil
    }

    if len(keepRows) == c.Card() {
        return c // 没有删除
    }

    // 创建新 chunk
    newChunk := chunk.NewEmptyChunk(len(c.Data))
    for colIdx := range c.Data {
        for _, rowIdx := range keepRows {
            val := c.Data[colIdx].GetValue(rowIdx)
            newChunk.Data[colIdx].SetValue(newChunk.Card(), val)
        }
    }
    newChunk.SetCard(len(keepRows))

    return newChunk
}
```

### 3.4 预估工作量

| 文件 | 内容 | 代码行数 |
|------|------|----------|
| `deletion_bitmap.go` | 删除位图 | ~100 行 |
| `compaction_worker.go` 修改 | 集成 | ~80 行 |
| `deletion_bitmap_test.go` | 测试 | ~50 行 |
| **合计** | | **~230 行** |

---

## P4: 索引查询规划

### 4.1 现状

`index.go:524` 有 TODO：
```go
func (p *IndexPlanner) PlanQuery(predicate FilterPredicate) ([]uint64, bool) {
    // TODO: Implement query planning with index selection
    return nil, false
}
```

### 4.2 实现方案

```go
// index_planner.go (新文件)

package storage2

import (
    "context"
    "sort"
)

// QueryPlan 查询计划
type QueryPlan struct {
    IndexName   string
    IndexType   IndexType
    EstCost     float64
    EstRows     uint64
    Predicates  []FilterPredicate
    UseFullScan bool
}

// IndexPlannerV2 增强的索引规划器
type IndexPlannerV2 struct {
    manager     *IndexManager
    tableStats  *TableStatistics
}

// TableStatistics 表统计信息
type TableStatistics struct {
    TotalRows     uint64
    ColumnStats   map[int]*ColumnStatistics
}

// ColumnStatistics 列统计信息
type ColumnStatistics struct {
    DistinctCount uint64
    NullCount     uint64
    MinValue      interface{}
    MaxValue      interface{}
}

// NewIndexPlannerV2 创建索引规划器
func NewIndexPlannerV2(manager *IndexManager) *IndexPlannerV2 {
    return &IndexPlannerV2{
        manager: manager,
    }
}

// SetTableStats 设置表统计信息
func (p *IndexPlannerV2) SetTableStats(stats *TableStatistics) {
    p.tableStats = stats
}

// PlanQuery 规划查询
func (p *IndexPlannerV2) PlanQuery(ctx context.Context, predicate FilterPredicate) (*QueryPlan, error) {
    if predicate == nil {
        return &QueryPlan{UseFullScan: true}, nil
    }

    // 收集所有可能的索引
    candidates := p.collectCandidateIndexes(predicate)
    if len(candidates) == 0 {
        return &QueryPlan{UseFullScan: true}, nil
    }

    // 估算每个索引的代价
    var bestPlan *QueryPlan
    bestCost := float64(1e18)

    for _, candidate := range candidates {
        cost := p.estimateCost(ctx, candidate, predicate)
        if cost < bestCost {
            bestCost = cost
            bestPlan = candidate
        }
    }

    // 与全表扫描比较
    fullScanCost := p.estimateFullScanCost()
    if fullScanCost < bestCost {
        return &QueryPlan{UseFullScan: true, EstCost: fullScanCost}, nil
    }

    return bestPlan, nil
}

// collectCandidateIndexes 收集候选索引
func (p *IndexPlannerV2) collectCandidateIndexes(predicate FilterPredicate) []*QueryPlan {
    var candidates []*QueryPlan

    // 提取谓词中涉及的列
    columns := p.extractPredicateColumns(predicate)

    // 检查每个索引是否可以使用
    for _, metadata := range p.manager.ListIndexes() {
        idx, ok := p.manager.GetIndex(metadata.Name)
        if !ok {
            continue
        }

        // 检查索引列是否匹配谓词列
        if p.indexMatchesPredicate(idx, columns) {
            candidates = append(candidates, &QueryPlan{
                IndexName:  metadata.Name,
                IndexType:  metadata.Type,
                Predicates: []FilterPredicate{predicate},
            })
        }
    }

    return candidates
}

// extractPredicateColumns 提取谓词涉及的列
func (p *IndexPlannerV2) extractPredicateColumns(predicate FilterPredicate) []int {
    var columns []int

    switch pred := predicate.(type) {
    case *ColumnPredicate:
        columns = append(columns, pred.ColumnIndex)
    case *AndPredicate:
        columns = append(columns, p.extractPredicateColumns(pred.Left)...)
        columns = append(columns, p.extractPredicateColumns(pred.Right)...)
    case *OrPredicate:
        columns = append(columns, p.extractPredicateColumns(pred.Left)...)
        columns = append(columns, p.extractPredicateColumns(pred.Right)...)
    }

    return columns
}

// indexMatchesPredicate 检查索引是否匹配谓词
func (p *IndexPlannerV2) indexMatchesPredicate(idx Index, columns []int) bool {
    idxColumns := idx.Columns()
    if len(idxColumns) == 0 {
        return false
    }

    // 检查索引的第一列是否在谓词列中
    for _, col := range columns {
        if idxColumns[0] == col {
            return true
        }
    }
    return false
}

// estimateCost 估算使用索引的代价
func (p *IndexPlannerV2) estimateCost(ctx context.Context, plan *QueryPlan, predicate FilterPredicate) float64 {
    idx, ok := p.manager.GetIndex(plan.IndexName)
    if !ok {
        return 1e18
    }

    stats := idx.Statistics()
    
    // 估算选择率
    selectivity := p.estimateSelectivity(predicate)
    
    // 估算 I/O 代价
    estRows := uint64(float64(stats.NumEntries) * selectivity)
    plan.EstRows = estRows

    // 索引查找代价 + 回表代价
    indexLookupCost := float64(estRows) * 0.1  // 索引查找
    fetchCost := float64(estRows) * 1.0         // 回表读取

    plan.EstCost = indexLookupCost + fetchCost
    return plan.EstCost
}

// estimateSelectivity 估算选择率
func (p *IndexPlannerV2) estimateSelectivity(predicate FilterPredicate) float64 {
    switch pred := predicate.(type) {
    case *ColumnPredicate:
        switch pred.Op {
        case Eq:
            // 等值查询: 1 / distinct_count
            if p.tableStats != nil {
                if colStats, ok := p.tableStats.ColumnStats[pred.ColumnIndex]; ok {
                    if colStats.DistinctCount > 0 {
                        return 1.0 / float64(colStats.DistinctCount)
                    }
                }
            }
            return 0.01 // 默认 1%
        case Lt, Le, Gt, Ge:
            return 0.33 // 范围查询默认 33%
        case Ne:
            return 0.99 // 不等于默认 99%
        }
    case *AndPredicate:
        return p.estimateSelectivity(pred.Left) * p.estimateSelectivity(pred.Right)
    case *OrPredicate:
        sel1 := p.estimateSelectivity(pred.Left)
        sel2 := p.estimateSelectivity(pred.Right)
        return sel1 + sel2 - sel1*sel2
    }
    return 1.0
}

// estimateFullScanCost 估算全表扫描代价
func (p *IndexPlannerV2) estimateFullScanCost() float64 {
    if p.tableStats == nil {
        return 1e6
    }
    return float64(p.tableStats.TotalRows) * 1.0
}

// ExecutePlan 执行查询计划
func (p *IndexPlannerV2) ExecutePlan(ctx context.Context, plan *QueryPlan) ([]uint64, error) {
    if plan.UseFullScan {
        return nil, nil // 返回 nil 表示使用全表扫描
    }

    idx, ok := p.manager.GetIndex(plan.IndexName)
    if !ok {
        return nil, nil
    }

    // 使用索引执行查询
    if len(plan.Predicates) > 0 {
        return idx.Search(ctx, plan.Predicates[0], int(plan.EstRows))
    }

    return nil, nil
}
```

### 4.3 预估工作量

| 文件 | 内容 | 代码行数 |
|------|------|----------|
| `index_planner.go` | 索引规划器 | ~350 行 |
| `index_planner_test.go` | 测试 | ~100 行 |
| **合计** | | **~450 行** |

---

## P5: 向量索引增量更新

### 5.1 背景

当前向量索引需要完全重建才能添加新向量。Lance Rust 支持增量更新。

### 5.2 实现方案

```go
// ivf_index.go 修改

// IVFIndexConfig 索引配置
type IVFIndexConfig struct {
    Dimension       int
    Nlist           int
    Nprobe          int
    MetricType      MetricType
    // 增量更新配置
    EnableIncremental   bool
    IncrementalBuffer   int     // 新向量缓冲区大小
    MergeThreshold      int     // 触发合并的阈值
}

// IncrementalIVFIndex 支持增量更新的 IVF 索引
type IncrementalIVFIndex struct {
    *IVFIndex
    
    // 增量缓冲区
    pendingVectors map[uint64][]float32
    pendingCount   int
    bufferSize     int
    mergeThreshold int
    
    // 是否需要重建
    needsRebuild bool
}

// NewIncrementalIVFIndex 创建增量 IVF 索引
func NewIncrementalIVFIndex(name string, columnIdx int, cfg IVFIndexConfig) *IncrementalIVFIndex {
    ivf := NewIVFIndex(name, columnIdx, cfg.Dimension, cfg.MetricType)
    
    bufferSize := cfg.IncrementalBuffer
    if bufferSize <= 0 {
        bufferSize = 10000
    }
    
    mergeThreshold := cfg.MergeThreshold
    if mergeThreshold <= 0 {
        mergeThreshold = 5000
    }
    
    return &IncrementalIVFIndex{
        IVFIndex:       ivf,
        pendingVectors: make(map[uint64][]float32),
        bufferSize:     bufferSize,
        mergeThreshold: mergeThreshold,
    }
}

// InsertIncremental 增量插入向量
func (idx *IncrementalIVFIndex) InsertIncremental(rowID uint64, vector []float32) error {
    idx.mu.Lock()
    defer idx.mu.Unlock()
    
    // 如果索引未训练，先添加到缓冲区
    if len(idx.centroids) == 0 || idx.centroids[0] == nil {
        idx.pendingVectors[rowID] = make([]float32, len(vector))
        copy(idx.pendingVectors[rowID], vector)
        idx.pendingCount++
        
        if idx.pendingCount >= idx.bufferSize {
            idx.needsRebuild = true
        }
        return nil
    }
    
    // 索引已训练，直接插入
    // 找到最近的聚类中心
    minDist := float32(math.MaxFloat32)
    closestCluster := 0
    
    for i, centroid := range idx.centroids {
        if centroid != nil {
            dist := l2Distance(vector, centroid)
            if dist < minDist {
                minDist = dist
                closestCluster = i
            }
        }
    }
    
    // 存储向量
    idx.vectors[rowID] = make([]float32, len(vector))
    copy(idx.vectors[rowID], vector)
    
    // 添加到倒排列表
    idx.invertedLists[closestCluster] = append(idx.invertedLists[closestCluster], rowID)
    idx.stats.NumEntries++
    
    // 检查是否需要触发合并
    if idx.pendingCount >= idx.mergeThreshold {
        idx.needsRebuild = true
    }
    
    return nil
}

// NeedsRebuild 检查是否需要重建索引
func (idx *IncrementalIVFIndex) NeedsRebuild() bool {
    idx.mu.RLock()
    defer idx.mu.RUnlock()
    return idx.needsRebuild
}

// Rebuild 重建索引
func (idx *IncrementalIVFIndex) Rebuild(ctx context.Context) error {
    idx.mu.Lock()
    defer idx.mu.Unlock()
    
    // 收集所有向量
    allVectors := make([][]float32, 0, len(idx.vectors)+len(idx.pendingVectors))
    allRowIDs := make([]uint64, 0, len(idx.vectors)+len(idx.pendingVectors))
    
    for rowID, vec := range idx.vectors {
        allVectors = append(allVectors, vec)
        allRowIDs = append(allRowIDs, rowID)
    }
    
    for rowID, vec := range idx.pendingVectors {
        allVectors = append(allVectors, vec)
        allRowIDs = append(allRowIDs, rowID)
    }
    
    // 重新训练聚类中心
    if err := idx.IVFIndex.Train(allVectors); err != nil {
        return err
    }
    
    // 清空旧数据
    idx.vectors = make(map[uint64][]float32)
    for i := range idx.invertedLists {
        idx.invertedLists[i] = nil
    }
    
    // 重新插入所有向量
    for i, vec := range allVectors {
        rowID := allRowIDs[i]
        idx.vectors[rowID] = vec
        
        // 找到最近的聚类中心
        minDist := float32(math.MaxFloat32)
        closestCluster := 0
        for j, centroid := range idx.centroids {
            if centroid != nil {
                dist := l2Distance(vec, centroid)
                if dist < minDist {
                    minDist = dist
                    closestCluster = j
                }
            }
        }
        idx.invertedLists[closestCluster] = append(idx.invertedLists[closestCluster], rowID)
    }
    
    // 清空缓冲区
    idx.pendingVectors = make(map[uint64][]float32)
    idx.pendingCount = 0
    idx.needsRebuild = false
    idx.stats.NumEntries = uint64(len(allVectors))
    
    return nil
}

// ANNSearchWithPending 搜索时包含待处理向量
func (idx *IncrementalIVFIndex) ANNSearchWithPending(ctx context.Context, queryVector []float32, limit int) ([]float32, []uint64, error) {
    idx.mu.RLock()
    defer idx.mu.RUnlock()
    
    // 搜索主索引
    distances, rowIDs, err := idx.IVFIndex.ANNSearch(ctx, queryVector, limit)
    if err != nil {
        return nil, nil, err
    }
    
    // 暴力搜索待处理向量
    type result struct {
        rowID    uint64
        distance float32
    }
    
    var pendingResults []result
    for rowID, vec := range idx.pendingVectors {
        dist := l2Distance(queryVector, vec)
        pendingResults = append(pendingResults, result{rowID: rowID, distance: dist})
    }
    
    // 合并结果
    allResults := make([]result, len(distances)+len(pendingResults))
    for i := range distances {
        allResults[i] = result{rowID: rowIDs[i], distance: distances[i]}
    }
    copy(allResults[len(distances):], pendingResults)
    
    // 排序
    sort.Slice(allResults, func(i, j int) bool {
        return allResults[i].distance < allResults[j].distance
    })
    
    // 返回 top-k
    if limit > len(allResults) {
        limit = len(allResults)
    }
    
    finalDistances := make([]float32, limit)
    finalRowIDs := make([]uint64, limit)
    for i := 0; i < limit; i++ {
        finalDistances[i] = allResults[i].distance
        finalRowIDs[i] = allResults[i].rowID
    }
    
    return finalDistances, finalRowIDs, nil
}
```

### 5.3 预估工作量

| 文件 | 内容 | 代码行数 |
|------|------|----------|
| `ivf_index.go` 修改 | 增量更新 | ~300 行 |
| `hnsw_index.go` 修改 | 增量更新 | ~150 行 |
| `incremental_test.go` | 测试 | ~100 行 |
| **合计** | | **~550 行** |

---

## P6: DynamoDB 外部锁 (可选)

### 6.1 设计方案

```go
// dynamodb_lock.go (新文件)

package storage2

import (
    "context"
    "errors"
    "fmt"
    "time"

    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/service/dynamodb"
    "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
    "github.com/google/uuid"
)

var (
    ErrLockHeld      = errors.New("lock is held by another owner")
    ErrLockNotHeld   = errors.New("lock is not held")
    ErrLockExpired   = errors.New("lock has expired")
)

// DynamoDBLock DynamoDB 分布式锁
type DynamoDBLock struct {
    client    *dynamodb.Client
    tableName string
    ttl       time.Duration
}

// DynamoDBLockConfig 锁配置
type DynamoDBLockConfig struct {
    TableName string
    TTL       time.Duration
    Region    string
}

// LockItem 锁记录
type LockItem struct {
    DatasetURI string    // 主键
    LockID     string    // 锁 ID
    Owner      string    // 锁持有者
    ExpiresAt  int64     // 过期时间 (Unix timestamp)
    Version    int64     // 乐观锁版本
    AcquiredAt int64     // 获取时间
}

// NewDynamoDBLock 创建 DynamoDB 锁
func NewDynamoDBLock(cfg DynamoDBLockConfig, client *dynamodb.Client) *DynamoDBLock {
    ttl := cfg.TTL
    if ttl <= 0 {
        ttl = 30 * time.Second
    }
    
    return &DynamoDBLock{
        client:    client,
        tableName: cfg.TableName,
        ttl:       ttl,
    }
}

// Acquire 获取锁
func (l *DynamoDBLock) Acquire(ctx context.Context, datasetURI string, owner string) (*LockItem, error) {
    now := time.Now()
    item := LockItem{
        DatasetURI: datasetURI,
        LockID:     uuid.New().String(),
        Owner:      owner,
        ExpiresAt:  now.Add(l.ttl).Unix(),
        Version:    1,
        AcquiredAt: now.Unix(),
    }
    
    // 尝试获取锁 (条件: 锁不存在 OR 锁已过期)
    _, err := l.client.PutItem(ctx, &dynamodb.PutItemInput{
        TableName: aws.String(l.tableName),
        Item: map[string]types.AttributeValue{
            "pk":          &types.AttributeValueMemberS{Value: datasetURI},
            "lock_id":     &types.AttributeValueMemberS{Value: item.LockID},
            "owner":       &types.AttributeValueMemberS{Value: item.Owner},
            "expires_at":  &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", item.ExpiresAt)},
            "version":     &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", item.Version)},
            "acquired_at": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", item.AcquiredAt)},
        },
        ConditionExpression: aws.String("attribute_not_exists(pk) OR expires_at < :now"),
        ExpressionAttributeValues: map[string]types.AttributeValue{
            ":now": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", now.Unix())},
        },
    })
    
    if err != nil {
        var cce *types.ConditionalCheckFailedException
        if errors.As(err, &cce) {
            return nil, ErrLockHeld
        }
        return nil, fmt.Errorf("dynamodb put item: %w", err)
    }
    
    return &item, nil
}

// Release 释放锁
func (l *DynamoDBLock) Release(ctx context.Context, item *LockItem) error {
    _, err := l.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
        TableName: aws.String(l.tableName),
        Key: map[string]types.AttributeValue{
            "pk": &types.AttributeValueMemberS{Value: item.DatasetURI},
        },
        ConditionExpression: aws.String("lock_id = :lid"),
        ExpressionAttributeValues: map[string]types.AttributeValue{
            ":lid": &types.AttributeValueMemberS{Value: item.LockID},
        },
    })
    
    if err != nil {
        var cce *types.ConditionalCheckFailedException
        if errors.As(err, &cce) {
            return ErrLockNotHeld
        }
        return fmt.Errorf("dynamodb delete item: %w", err)
    }
    
    return nil
}

// Renew 续租锁
func (l *DynamoDBLock) Renew(ctx context.Context, item *LockItem) (*LockItem, error) {
    newExpiresAt := time.Now().Add(l.ttl).Unix()
    
    _, err := l.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
        TableName: aws.String(l.tableName),
        Key: map[string]types.AttributeValue{
            "pk": &types.AttributeValueMemberS{Value: item.DatasetURI},
        },
        UpdateExpression: aws.String("SET expires_at = :expires, version = version + :one"),
        ConditionExpression: aws.String("lock_id = :lid AND expires_at > :now"),
        ExpressionAttributeValues: map[string]types.AttributeValue{
            ":expires": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", newExpiresAt)},
            ":lid":     &types.AttributeValueMemberS{Value: item.LockID},
            ":now":     &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", time.Now().Unix())},
            ":one":     &types.AttributeValueMemberN{Value: "1"},
        },
    })
    
    if err != nil {
        var cce *types.ConditionalCheckFailedException
        if errors.As(err, &cce) {
            return nil, ErrLockExpired
        }
        return nil, fmt.Errorf("dynamodb update item: %w", err)
    }
    
    item.ExpiresAt = newExpiresAt
    item.Version++
    return item, nil
}

// DynamoDBCommitHandler 使用 DynamoDB 锁的 CommitHandler
type DynamoDBCommitHandler struct {
    basePath string
    lock     *DynamoDBLock
    inner    CommitHandler
    owner    string
}

// NewDynamoDBCommitHandler 创建 DynamoDB CommitHandler
func NewDynamoDBCommitHandler(basePath string, lock *DynamoDBLock, inner CommitHandler) *DynamoDBCommitHandler {
    return &DynamoDBCommitHandler{
        basePath: basePath,
        lock:     lock,
        inner:    inner,
        owner:    uuid.New().String(),
    }
}

// Commit 提交 manifest (使用分布式锁)
func (h *DynamoDBCommitHandler) Commit(ctx context.Context, manifest *Manifest, currentVersion uint64) error {
    // 获取锁
    lockItem, err := h.lock.Acquire(ctx, h.basePath, h.owner)
    if err != nil {
        return fmt.Errorf("acquire lock: %w", err)
    }
    defer h.lock.Release(ctx, lockItem)
    
    // 执行提交
    return h.inner.Commit(ctx, manifest, currentVersion)
}

// ResolveLatestVersion 解析最新版本
func (h *DynamoDBCommitHandler) ResolveLatestVersion(ctx context.Context) (uint64, error) {
    return h.inner.ResolveLatestVersion(ctx)
}

// ResolveVersion 解析版本路径
func (h *DynamoDBCommitHandler) ResolveVersion(ctx context.Context, version uint64) (string, error) {
    return h.inner.ResolveVersion(ctx, version)
}
```

### 6.2 预估工作量

| 文件 | 内容 | 代码行数 |
|------|------|----------|
| `dynamodb_lock.go` | DynamoDB 锁 | ~300 行 |
| `dynamodb_commit.go` | CommitHandler 集成 | ~150 行 |
| `dynamodb_lock_test.go` | 测试 | ~150 行 |
| **合计** | | **~600 行** |

---

## 执行顺序与依赖图

```
P1: 向量量化 ─────────────────────────────> IVF/HNSW 已存在
    │
    ├── PQ Quantizer
    ├── SQ Quantizer
    └── IVF-PQ Index

P2: 全文搜索 ─────────────────────────────> 无依赖
    │
    ├── 倒排索引
    ├── BM25 评分
    └── 中文分词

P3: 删除位图 ─────────────────────────────> roaring bitmap 库
    │
    └── compaction_worker 集成

P4: 索引规划 ─────────────────────────────> IndexSelector 已存在
    │
    └── 代价估算

P5: 增量更新 ─────────────────────────────> P1 (可选)
    │
    └── 缓冲区管理

P6: DynamoDB 锁 ──────────────────────────> AWS SDK
    │
    └── CommitHandler 集成
```

---

## 总预估

| 阶段 | 新增文件 | 修改文件 | 代码行数 | 依赖 |
|------|----------|----------|----------|------|
| P1 | 5 | 1 | ~1300 | 无 |
| P2 | 2 | 1 | ~900 | 无 |
| P3 | 1 | 1 | ~230 | roaring |
| P4 | 1 | 1 | ~450 | 无 |
| P5 | 0 | 2 | ~550 | P1 |
| P6 | 2 | 0 | ~600 | AWS SDK |
| **合计** | **11** | **6** | **~4030** | |

---

## 建议执行顺序

1. **P3: 删除位图** - 最小改动，修复压缩正确性
2. **P1: 向量量化** - 核心性能提升
3. **P2: 全文搜索** - 新功能，独立开发
4. **P4: 索引规划** - 优化查询性能
5. **P5: 增量更新** - 依赖 P1
6. **P6: DynamoDB 锁** - 可选，生产环境需要
