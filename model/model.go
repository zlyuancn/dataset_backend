package model

// 一个chunk元数据
type OneChunkMeta struct {
	ChunkSn      int    // chunk sn
	StartValueSn int    // 第一个 value 的 sn
	EndValueSn   int    // 最后一个 value 的 sn
	Checksum     uint32 // 数据校验和(实际持久化数据)
}

type ChunkMeta []OneChunkMeta

// 缓存的数据集处理状态
type CacheDatasetProcessStatus struct {
	ChunkTotal            int32 // chunk 总数
	ChunkProcessedCount   int32 // chunk 完成数
	ValueProcessedCount   int64 // 当前已完成处理的数据总数
	NextChunkStartValueSn int64 // 下一个 chunk 的第一个数据的 sn
	ResumePointOffset     int64 // 断点续传偏移量, 表示已完成的chunk扫描了多少字节
}

// 停止标记
type StopFlag byte

const (
	StopFlag_None StopFlag = iota // 无
	StopFlag_Stop                 // 停止信号
)
