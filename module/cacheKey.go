package module

import (
	"strconv"

	"github.com/zlyuancn/dataset/conf"
)

var CacheKey = cacheKeyCli{}

type cacheKeyCli struct{}

// 数据集信息缓存key
func (cacheKeyCli) GetDatasetInfo(datasetId int) string {
	return conf.Conf.DatasetInfoKeyPrefix + strconv.Itoa(datasetId)
}

// 停止标记key
func (*cacheKeyCli) GetStopFlag(datasetId int) string {
	return conf.Conf.StopProcessFlagPrefix + strconv.Itoa(datasetId)
}
