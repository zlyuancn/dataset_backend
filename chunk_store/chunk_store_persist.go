package chunk_store

import (
	"context"
	"fmt"

	"github.com/zlyuancn/splitter"

	"github.com/zlyuancn/dataset/pb"
)

type ChunkStorePersist interface {
	FlushChunk(ctx context.Context, args *splitter.FlushChunkArgs) error
}

type cspCreatorFunc = func(ctx context.Context, datasetId uint, de *pb.DatasetExtend) (ChunkStorePersist, error)

var cspCreator = map[int32]cspCreatorFunc{
	0: newNoneCsp,
	1: newRedisCsp,
}

func NewChunkStorePersist(ctx context.Context, datasetId uint, de *pb.DatasetExtend) (ChunkStorePersist, error) {
	c, ok := cspCreator[de.GetChunkProcess().GetStoreType()]
	if !ok {
		return nil, fmt.Errorf("not support StoreType type=%d", de.GetChunkProcess().GetStoreType())
	}
	return c(ctx, datasetId, de)
}
