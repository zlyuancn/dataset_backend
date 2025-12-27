package chunk_store

import (
	"bytes"
	"io"

	"github.com/pierrec/lz4"

	"github.com/zlyuancn/dataset/conf"
	"github.com/zlyuancn/dataset/pb"
)

type Compressor interface {
	// 压缩
	Compress([]byte) ([]byte, error)
	// 解压缩
	UnCompress([]byte) ([]byte, error)
}

var compressors = map[pb.CompressType]Compressor{
	pb.CompressType_CompressorType_None: newNoneCompressor(),
	pb.CompressType_CompressorType_Lz4:  newLz4Compressor(),
}

func newCompressor(t pb.CompressType) Compressor {
	c, ok := compressors[t]
	if !ok {
		c = newNoneCompressor()
	}
	return c
}

// 压缩
func Compress(t pb.CompressType, data []byte) ([]byte, bool, error) {
	if len(data) == 0 {
		return data, true, nil
	}

	c := newCompressor(t)
	newData, err := c.Compress(data)
	if err != nil {
		return nil, false, err
	}

	// 检查压缩比
	if conf.Conf.ChunkCompressedRatioLimit > 0 {
		if len(newData)*100 > len(data)*conf.Conf.ChunkCompressedRatioLimit { // 避免浮点运算
			// 压缩收益不足，视为未压缩成功
			return nil, false, nil
		}
	}

	// 尝试立即解压
	bs, err := c.UnCompress(newData)
	if err != nil {
		// 无法解压
		return nil, false, err
	}

	// 对比数据
	if bytes.Compare(data, bs) != 0 {
		// 解压数据与原始数据不一致
		return nil, false, nil
	}
	return newData, true, nil
}

// 解压缩
func UnCompress(t pb.CompressType, data []byte) ([]byte, error) {
	if len(data) == 0 {
		return data, nil
	}

	c := newCompressor(t)
	return c.UnCompress(data)
}

type noneCompressor struct{}

func (noneCompressor) Compress(data []byte) ([]byte, error) {
	return data, nil
}

func (noneCompressor) UnCompress(data []byte) ([]byte, error) {
	return data, nil
}

func newNoneCompressor() Compressor {
	return noneCompressor{}
}

type lz4Compressor struct{}

func (lz4Compressor) Compress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := lz4.NewWriter(&buf)
	_, err := w.Write(data)
	if err != nil {
		return nil, err
	}
	err = w.Close()
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (lz4Compressor) UnCompress(data []byte) ([]byte, error) {
	r := lz4.NewReader(bytes.NewReader(data))
	return io.ReadAll(r)
}

func newLz4Compressor() Compressor {
	return lz4Compressor{}
}
