package model

// 一个chunk元数据
type OneChunkMeta struct {
	Sn      int // chunk sn
	StartSn int // 第一个数据sn
	EndSn   int // 最后一个数据sn
}

type ChunkMeta struct {
}

// 缓存的数据集处理状态
type CacheDatasetProcessStatus struct {
	ChunkTotal           int   // chunk 总数
	ChunkProcessedCount  int   // chunk 完成数
	NextChunkStartDataSn int   // 下一个 chunk 的第一个数据的 sn
	ResumePointOffset    int64 // 断点续传偏移量, 表示已完成的chunk扫描了多少字节
}

// 数据源扩展数据
type DataSourceExtend struct {
}
