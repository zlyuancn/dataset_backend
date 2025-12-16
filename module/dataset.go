package module

import (
	"context"
	"strconv"

	"github.com/zly-app/cache/v2"
	"github.com/zly-app/component/sqlx"
	"github.com/zly-app/zapp/log"
	"github.com/zly-app/zapp/pkg/utils"
	"github.com/zlyuancn/dataset/client/db"
	"github.com/zlyuancn/dataset/conf"
	"github.com/zlyuancn/dataset/dao/dataset"
	"github.com/zlyuancn/dataset/model"
	"go.uber.org/zap"
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

// 获取数据集信息, 使用缓存
func (*datasetCli) GetDatasetInfoByCache(ctx context.Context, datasetId uint) (*dataset.Model, error) {
	key := CacheKey.GetDatasetInfo(int(datasetId))
	ret := &dataset.Model{}
	err := cache.GetDefCache().Get(ctx, key, ret, cache.WithLoadFn(func(ctx context.Context, key string) (interface{}, error) {
		v, err := dataset.GetOneByDatasetId(ctx, int(datasetId))
		if err == sqlx.ErrNoRows {
			return nil, nil
		}
		return v, err
	}), cache.WithExpire(conf.Conf.DatasetInfoCacheTtl))
	return ret, err
}

// 批量获取数据集信息, 使用缓存
func (d *datasetCli) BatchGetDatasetInfoByCache(ctx context.Context, datasetId []uint) ([]*dataset.Model, error) {
	// 批量获取数据
	lines, err := utils.GoQuery(datasetId, func(id uint) (*dataset.Model, error) {
		line, err := d.GetDatasetInfoByCache(ctx, id)
		if err != nil {
			log.Error(ctx, "BatchGetDatasetInfoByCache call GetDatasetInfoByCache fail.", zap.Uint("id", id), zap.Error(err))
			return nil, err
		}
		return line, nil
	}, true)
	if err != nil {
		log.Error(ctx, "BatchGetDatasetInfoByCache call query fail.", zap.Error(err))
		return nil, err
	}
	return lines, nil
}
