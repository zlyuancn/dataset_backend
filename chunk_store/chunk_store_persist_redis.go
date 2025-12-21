package chunk_store

import (
	"context"
	"fmt"

	"github.com/zlyuancn/splitter"

	"github.com/zlyuancn/dataset/client/db"
	"github.com/zlyuancn/dataset/conf"
	"github.com/zlyuancn/dataset/pb"
)

type redisCsp struct {
	datasetId uint
}

func newRedisCsp(ctx context.Context, datasetId uint, cp *pb.ChunkProcess) (ChunkStorePersist, error) {
	r := &redisCsp{
		datasetId: datasetId,
	}
	return r, nil
}

func (r *redisCsp) FlushChunk(ctx context.Context, args *splitter.FlushChunkArgs) error {
	rdb, err := db.GetChunkStoreRedis()
	if err != nil {
		return err
	}

	key := fmt.Sprintf(conf.Conf.ChunkStoreRedisKeyFormat, r.datasetId, args.ChunkSn)
	err = rdb.Set(ctx, key, args.ChunkData, 0).Err()
	return err
}
