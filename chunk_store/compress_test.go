package chunk_store

import (
	"bytes"
	"testing"

	"github.com/zlyuancn/dataset_backend/pb"
)

func TestCompress_UnCompress(t *testing.T) {
	testCases := []struct {
		name string
		typ  pb.CompressType
		data []byte
	}{
		{
			name: "None - empty",
			typ:  pb.CompressType_CompressorType_None,
			data: []byte{},
		},
		{
			name: "None - normal",
			typ:  pb.CompressType_CompressorType_None,
			data: []byte("hello"),
		},
		{
			name: "LZ4 - empty",
			typ:  pb.CompressType_CompressorType_Lz4,
			data: []byte{},
		},
		{
			name: "LZ4 - normal",
			typ:  pb.CompressType_CompressorType_Lz4,
			data: []byte("hello world, this is a test for lz4 compression!"),
		},
		{
			name: "LZ4 - large data",
			typ:  pb.CompressType_CompressorType_Lz4,
			data: bytes.Repeat([]byte("abc123"), 1000),
		},
		{
			name: "Unknown type (fallback to None)",
			typ:  pb.CompressType(999),
			data: []byte("fallback test"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 压缩
			compressed, ok, err := Compress(tc.typ, tc.data)
			if err != nil {
				t.Fatalf("Compress failed: %v", err)
			}
			if !ok {
				t.Fatalf("Compress returned ok=false")
			}

			// 解压
			decompressed, err := UnCompress(tc.typ, compressed)
			if err != nil {
				t.Fatalf("UnCompress failed: %v", err)
			}

			// 验证数据一致
			if !bytes.Equal(tc.data, decompressed) {
				t.Errorf("Round-trip data mismatch.\nWant: %q\nGot:  %q", tc.data, decompressed)
			}

			// 特别地：None 压缩应返回原始数据（即 compressed == data）
			if tc.typ == pb.CompressType_CompressorType_None || tc.typ == 999 {
				if !bytes.Equal(compressed, tc.data) {
					t.Errorf("None compressor should return original data as compressed output")
				}
			}
		})
	}
}

func TestCompress_InvalidLZ4Data(t *testing.T) {
	// 用非法 LZ4 数据尝试解压，应报错
	_, err := UnCompress(pb.CompressType_CompressorType_Lz4, []byte("not lz4 data"))
	if err == nil {
		t.Error("Expected error when uncompressing invalid LZ4 data")
	}
}

func TestNoneCompressorDirect(t *testing.T) {
	c := newNoneCompressor()
	data := []byte("test")

	comp, err := c.Compress(data)
	if err != nil {
		t.Fatalf("NoneCompressor Compress failed: err=%v", err)
	}
	if !bytes.Equal(comp, data) {
		t.Errorf("NoneCompressor Compress should return input data")
	}

	decomp, err := c.UnCompress(comp)
	if err != nil {
		t.Fatalf("NoneCompressor UnCompress failed: %v", err)
	}
	if !bytes.Equal(decomp, data) {
		t.Errorf("NoneCompressor UnCompress mismatch")
	}
}

func TestLz4CompressorDirect(t *testing.T) {
	c := newLz4Compressor()
	data := []byte("lz4 test data with some content!")

	comp, err := c.Compress(data)
	if err != nil {
		t.Fatalf("LZ4 Compress failed: err=%v", err)
	}

	decomp, err := c.UnCompress(comp)
	if err != nil {
		t.Fatalf("LZ4 UnCompress failed: %v", err)
	}

	if !bytes.Equal(data, decomp) {
		t.Errorf("LZ4 round-trip failed.\nWant: %q\nGot: %q", data, decomp)
	}
}
