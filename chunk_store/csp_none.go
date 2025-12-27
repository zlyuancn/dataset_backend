package chunk_store

import (
	"context"
	"errors"

	"github.com/zlyuancn/dataset/pb"
)

type noneCsp struct{}

func (noneCsp) FlushChunk(ctx context.Context, chunkSn int32, chunkData []byte) error {
	return nil
}

func (noneCsp) LoadChunk(ctx context.Context, chunkSn int32) ([]byte, error) {
	return nil, errors.New("noneCsp can not LoadChunk")
}

func newNoneCsp(ctx context.Context, datasetId uint, de *pb.DatasetExtend) (ChunkStorePersist, error) {
	return noneCsp{}, nil
}
