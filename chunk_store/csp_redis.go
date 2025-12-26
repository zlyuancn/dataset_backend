package chunk_store

import (
	"context"
	"fmt"

	"github.com/zlyuancn/splitter"

	"github.com/zlyuancn/dataset/client/db"
	"github.com/zlyuancn/dataset/conf"
	"github.com/zlyuancn/dataset/model"
	"github.com/zlyuancn/dataset/pb"
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

func (r *redisCsp) FlushChunk(ctx context.Context, args *splitter.FlushChunkArgs) error {
	rdb, err := db.GetChunkStoreRedis()
	if err != nil {
		return err
	}

	key := fmt.Sprintf(conf.Conf.ChunkStoreRedisKeyFormat, r.datasetId, args.ChunkSn)
	err = rdb.Set(ctx, key, args.ChunkData, 0).Err()
	return err
}

func (r *redisCsp) LoadChunk(ctx context.Context, oneChunkMeta *model.OneChunkMeta) ([]byte, error) {
	rdb, err := db.GetChunkStoreRedis()
	if err != nil {
		return nil, err
	}

	key := fmt.Sprintf(conf.Conf.ChunkStoreRedisKeyFormat, r.datasetId, oneChunkMeta.ChunkSn)
	return rdb.Get(ctx, key).Bytes()
}
