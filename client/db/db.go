package db

import (
	"github.com/zly-app/component/redis"
	"github.com/zly-app/component/sqlx"

	"github.com/zlyuancn/dataset/conf"
)

func GetSqlx() sqlx.Client {
	return sqlx.GetClient(conf.Conf.SqlxName)
}
func GetRedis() (redis.UniversalClient, error) {
	return redis.GetClient(conf.Conf.RedisName)
}
func GetChunkStoreRedis() (redis.UniversalClient, error) {
	return redis.GetClient(conf.Conf.ChunkStoreRedisName)
}
