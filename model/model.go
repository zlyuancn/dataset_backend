package model

// chunk 数据
type ChunkData struct {
	ChunkSn      int32  // chunk sn
	StartValueSn int64  // 第一个 value 的 sn
	EndValueSn   int64  // 最后一个 value 的 sn
	ChunkData    []byte // chunk数据
	ScanByteNum  int64  // 已扫描rd的字节数
}

// 一个chunk元数据
type OneChunkMeta struct {
	ChunkSn      int32  // chunk sn
	StartValueSn int64  // 第一个 value 的 sn
	EndValueSn   int64  // 最后一个 value 的 sn
	Checksum     uint32 // 数据校验和(原始数据, 不是压缩后的数据)
	IsCompressed bool   // 这个 chunk 是否使用了压缩
	ScanByteNum  int64  // 已扫描rd的字节数
}

type ChunkMeta []*OneChunkMeta

// 缓存的数据集处理状态
type CacheDatasetProcessStatus struct {
	DataStreamLen      int64 // 数据流长度
	ChunkSizeLimit     int32 // chunk 大小限制
	ChunkTotal         int32 // chunk 总数
	ChunkFinishedCount int32 // chunk 完成数
	ValueFinishedCount int64 // 当前已完成处理的数据总数
	ResumePointOffset  int64 // 断点续传偏移量, 表示已完成的chunk扫描了多少字节
}

// 停止标记
type StopFlag byte

const (
	StopFlag_None StopFlag = iota // 无
	StopFlag_Stop                 // 停止信号
)
