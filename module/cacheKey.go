package module

import (
	"strconv"

	"github.com/zlyuancn/dataset/conf"
)

var CacheKey = cacheKeyCli{}

type cacheKeyCli struct{}

// 数据集信息缓存key
func (cacheKeyCli) GetDatasetId(datasetId int) string {
	return conf.Conf.DatasetOpLockKeyPrefix + strconv.Itoa(datasetId)
}
