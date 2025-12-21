package chunk_store

import (
	"context"

	"github.com/zlyuancn/splitter"

	"github.com/zlyuancn/dataset/pb"
)

type noneCsp struct{}

func (noneCsp) FlushChunk(ctx context.Context, args *splitter.FlushChunkArgs) error { return nil }

func newNoneCsp(ctx context.Context, datasetId uint, cp *pb.ChunkProcess) (ChunkStorePersist, error) {
	return noneCsp{}, nil
}
