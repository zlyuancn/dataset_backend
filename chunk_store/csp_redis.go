package chunk_store

import (
	"context"
	"fmt"

	"github.com/zlyuancn/dataset_backend/client/db"
	"github.com/zlyuancn/dataset_backend/conf"
	"github.com/zlyuancn/dataset_backend/pb"
)

type redisCsp struct {
	datasetId uint
	de        *pb.DatasetExtend
}

func newRedisCsp(ctx context.Context, datasetId uint, de *pb.DatasetExtend) (ChunkStorePersist, error) {
	r := &redisCsp{
		datasetId: datasetId,
		de:        de,
	}
	return r, nil
}

func (r *redisCsp) FlushChunk(ctx context.Context, chunkSn int32, chunkData []byte) error {
	rdb, err := db.GetChunkStoreRedis()
	if err != nil {
		return err
	}

	key := fmt.Sprintf(conf.Conf.ChunkStoreRedisKeyFormat, r.datasetId, chunkSn)
	err = rdb.Set(ctx, key, chunkData, 0).Err()
	return err
}

func (r *redisCsp) LoadChunk(ctx context.Context, chunkSn int32) ([]byte, error) {
	rdb, err := db.GetChunkStoreRedis()
	if err != nil {
		return nil, err
	}

	key := fmt.Sprintf(conf.Conf.ChunkStoreRedisKeyFormat, r.datasetId, chunkSn)
	return rdb.Get(ctx, key).Bytes()
}
