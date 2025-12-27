package chunk_store

import (
	"context"
	"testing"

	"github.com/zlyuancn/dataset_backend/conf"
	"github.com/zlyuancn/dataset_backend/model"
	"github.com/zlyuancn/dataset_backend/pb"
)

func TestChunkStore(t *testing.T) {
	const chunkTotal = 100         // chunk总数
	const finishedChunkCount = 30  // chunk已完成数
	const oneChunkValueCount = 100 // 一个 chunk 有多少 value
	const oneValueSize = 100       // 一个 value 数据有多长

	conf.Conf.ChunkStoreThreadCount = 20

	lastFinishedChunkSn := finishedChunkCount - 1

	de := &pb.DatasetExtend{
		DataProcess:  &pb.DataProcess{},
		ChunkProcess: &pb.ChunkProcess{},
		ValueProcess: &pb.ValueProcess{},
	}
	fcb := func(meta *model.OneChunkMeta) {
		t.Logf("fcb chunkData=%+v", meta)
		lastFinishedChunkSn = int(meta.ChunkSn)
	}
	lcb := func(meta *model.OneChunkMeta) {
		t.Logf("lcb!!!! chunkData=%+v", meta)
		lastFinishedChunkSn = int(meta.ChunkSn)
	}

	cs, err := NewChunkStore(context.Background(), 0, de, fcb, lcb)
	if err != nil {
		t.Fatalf("NewChunkStore fail. err=%s", err)
		return
	}
	defer cs.Close()

	cs.Init(&ResumePoint{
		ChunkFinishedCount: finishedChunkCount,
		ValueFinishedCount: finishedChunkCount * oneChunkValueCount,
		ResumePointOffset:  finishedChunkCount * oneChunkValueCount * oneValueSize,
	})

	lastChunkSn := -1
	for i := 0; i < chunkTotal-finishedChunkCount; i++ {
		lastChunkSn = i
		cs.FlushChunk(context.Background(), &model.ChunkData{
			ChunkSn:      int32(i),
			StartValueSn: int64(i * oneChunkValueCount),
			EndValueSn:   int64((i+1)*oneChunkValueCount - 1),
			ChunkData:    nil,
			ScanByteNum:  int64((i + 1) * oneChunkValueCount * oneValueSize),
		})
	}

	err = cs.WaitNoChunkSnOffset(context.Background(), int32(lastChunkSn))
	if err != nil {
		t.Fatalf("Wait fail. err=%s", err)
		return
	}

	if lastFinishedChunkSn != chunkTotal-1 {
		t.Fatalf("lastFinishedChunkSn(%d) != chunkCount-1(%d)", lastFinishedChunkSn, chunkTotal-1)
		return
	}

	t.Log("ok")
}
