package module

import (
	"context"
	"strconv"

	"github.com/zlyuancn/dataset/client/db"
	"github.com/zlyuancn/dataset/model"
)

var Dataset = &datasetCli{}

type datasetCli struct{}

// 写入或删除停止标记
func (*datasetCli) SetStopFlag(ctx context.Context, datasetId int, flag model.StopFlag) error {
	key := CacheKey.GetStopFlag(datasetId)
	rdb, err := db.GetRedis()
	if err != nil {
		return err
	}
	switch flag {
	case model.StopFlag_None:
		err = rdb.Del(ctx, key).Err()
	case model.StopFlag_Stop:
		err = rdb.SetNX(ctx, key, strconv.Itoa(int(model.StopFlag_Stop)), 0).Err()
	}
	return err
}
