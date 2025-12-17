package chunk_store

import (
	"context"

	"github.com/zlyuancn/splitter"
)

type ChunkStorePersist interface {
	FlushChunk(ctx context.Context, args *splitter.FlushChunkArgs) error
}
