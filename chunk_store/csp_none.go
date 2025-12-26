package chunk_store

import (
	"context"
	"errors"

	"github.com/zlyuancn/splitter"

	"github.com/zlyuancn/dataset/model"
	"github.com/zlyuancn/dataset/pb"
)

type noneCsp struct{}

func (noneCsp) FlushChunk(ctx context.Context, args *splitter.FlushChunkArgs) error {
	return nil
}

func (noneCsp) LoadChunk(ctx context.Context, oneChunkMeta *model.OneChunkMeta) ([]byte, error) {
	return nil, errors.New("noneCsp can not LoadChunk")
}

func newNoneCsp(ctx context.Context, datasetId uint, de *pb.DatasetExtend) (ChunkStorePersist, error) {
	return noneCsp{}, nil
}
