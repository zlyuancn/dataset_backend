package chunk_store

import (
	"context"

	"github.com/zlyuancn/dataset/pb"
	"github.com/zlyuancn/splitter"
)

type noneCsp struct{}

func (noneCsp) FlushChunk(ctx context.Context, args *splitter.FlushChunkArgs) error { return nil }

func newNoneCsp(ctx context.Context, cp *pb.ChunkProcess) (ChunkStorePersist, error) {
	return noneCsp{}, nil
}
