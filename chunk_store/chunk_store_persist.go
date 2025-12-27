package chunk_store

import (
	"context"
	"fmt"

	"github.com/zlyuancn/dataset/pb"
)

type ChunkStorePersist interface {
	FlushChunk(ctx context.Context, chunkSn int32, chunkData []byte) error
	LoadChunk(ctx context.Context, chunkSn int32) ([]byte, error)
}

type CspCreator = func(ctx context.Context, datasetId uint, de *pb.DatasetExtend) (ChunkStorePersist, error)

var cspCreators = map[int32]CspCreator{
	0: newNoneCsp,
	1: newRedisCsp,
}

func NewChunkStorePersist(ctx context.Context, datasetId uint, de *pb.DatasetExtend) (ChunkStorePersist, error) {
	c, ok := cspCreators[de.GetChunkProcess().GetStoreType()]
	if !ok {
		return nil, fmt.Errorf("not support StoreType type=%d", de.GetChunkProcess().GetStoreType())
	}
	return c(ctx, datasetId, de)
}
