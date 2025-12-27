package module

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math/rand"
	"sort"
	"strconv"

	"github.com/bytedance/sonic"
	"github.com/hashicorp/golang-lru/v2"
	"github.com/zly-app/zapp/component/gpool"
	"github.com/zly-app/zapp/log"
	"github.com/zly-app/zapp/pkg/utils"
	"github.com/zlyuancn/splitter"
	"github.com/zlyuancn/zsingleflight"
	"go.uber.org/zap"

	"github.com/zlyuancn/dataset_backend/chunk_store"
	"github.com/zlyuancn/dataset_backend/conf"
	"github.com/zlyuancn/dataset_backend/model"
	"github.com/zlyuancn/dataset_backend/pb"
)

type queryCli struct {
	chunkMetaCache  *lru.Cache[int64, model.ChunkMeta] // chunk meta . key=datasetId
	metasSfc        zsingleflight.ISingleFlight[model.ChunkMeta]
	chunkValueCache *lru.Cache[string, []string] // chunk value. key=datasetId_sn
	valueSfc        zsingleflight.ISingleFlight[[]string]
}

var Query *queryCli

func InitQuery() {
	q, err := newQueryCli()
	if err != nil {
		log.Fatal("init query fail", zap.Error(err))
	}
	Query = q
}

func newQueryCli() (*queryCli, error) {
	chunkMetaCache, err := lru.New[int64, model.ChunkMeta](conf.Conf.ChunkMetaLruCacheCount)
	if err != nil {
		log.Error("newQueryCli fail. create Chunk Meta Lru fail.", zap.Error(err))
		return nil, err
	}

	chunkValueCache, err := lru.New[string, []string](conf.Conf.ChunkDataLruCacheCount)
	if err != nil {
		log.Error("newQueryCli fail. create Chunk Data Lru fail.", zap.Error(err))
		return nil, err
	}

	q := &queryCli{
		chunkMetaCache:  chunkMetaCache,
		metasSfc:        zsingleflight.New[model.ChunkMeta](),
		chunkValueCache: chunkValueCache,
		valueSfc:        zsingleflight.New[[]string](),
	}
	return q, nil
}

func (q *queryCli) GetValue(ctx context.Context, datasetId, valueSn int64) (int32, string, error) {
	// 获取元数据
	metas, err := q.getChunkMetas(ctx, datasetId)
	if err != nil {
		log.Error(ctx, "GetValue call getChunkMetas fail.", zap.Error(err))
		return 0, "", err
	}

	// 二分查找定位 chunk
	meta, err := q.findChunkByValueSn(metas, valueSn)
	if err != nil {
		log.Error(ctx, "GetValue call findChunkByValueSn fail.", zap.Error(err))
		return 0, "", err
	}

	// 获取数据
	values, err := q.getChunkValues(ctx, datasetId, meta)
	if err != nil {
		log.Error(ctx, "GetValue call getChunkValues fail.", zap.Error(err))
		return 0, "", err
	}

	// 定位具体value
	localIndex := valueSn - meta.StartValueSn
	if localIndex < 0 || localIndex >= int64(len(values)) {
		err = errors.New("Failed to locate the index of value")
		log.Error(ctx, "GetValue fail.", zap.Error(err))
		return 0, "", err
	}

	// 尝试加载下一个 chunk sn 的数据
	q.tryLoadNextChunk(ctx, datasetId, metas, meta, valueSn)

	return meta.ChunkSn, values[localIndex], nil
}

// 尝试缓存下一个 chunk
func (q *queryCli) tryLoadNextChunk(ctx context.Context, datasetId int64, metas model.ChunkMeta, nowChunkMeta *model.OneChunkMeta, valueSn int64) {
	// 检查是否开启预加载
	if conf.Conf.ChunkPreloadByValueExpendRatio == 0 {
		return
	}

	// 检查当前是否为最后一个chunk
	if nowChunkMeta.ChunkSn >= metas[len(metas)-1].ChunkSn {
		return
	}

	// 检查 valueSn 在当前 chunk 位置的百分比以上. 表示触发预加载阈值
	localIndex := valueSn - nowChunkMeta.StartValueSn + 1
	if localIndex*100 < (nowChunkMeta.EndValueSn-nowChunkMeta.StartValueSn+1)*int64(conf.Conf.ChunkPreloadByValueExpendRatio) {
		return
	}

	// 概率计算. 根据比例得到要进行预加载的当前value个数
	if conf.Conf.ChunkPreloadProbabilityWithValueCount > 0 {
		residue := (nowChunkMeta.EndValueSn - nowChunkMeta.StartValueSn + 1) * (100 - int64(conf.Conf.ChunkPreloadByValueExpendRatio)) / 100
		// 概率尽量达到有10个value会触发加载
		if residue > int64(conf.Conf.ChunkPreloadProbabilityWithValueCount) { // 超过n个就要计算概率
			if rand.Int63n(residue) >= int64(conf.Conf.ChunkPreloadProbabilityWithValueCount) { // 未命中概率
				return
			}
		}
	}

	nextChunkSn := nowChunkMeta.ChunkSn + 1
	// 尝试获取数据
	cacheKey := strconv.FormatInt(datasetId, 32) + "_" + strconv.FormatInt(int64(nextChunkSn), 32)
	_, ok := q.chunkValueCache.Peek(cacheKey) // 不会更新“最近使用”状态
	if ok {
		// 已经有数据了
		return
	}

	// 获取下一个ChunkMeta
	if int(nextChunkSn) >= len(metas) { // 超出索引
		return
	}
	nextChunkMeta := metas[nextChunkSn] // 这里必然是有序的从 0 开始

	// 通过查询来加载下一个数据
	cloneCtx := utils.Ctx.CloneContext(ctx)
	log.Info(ctx, "tryLoadNextChunk", zap.Any("nextChunkMeta", nextChunkMeta))
	gpool.GetDefGPool().Go(func() error {
		values, err := q.getChunkValuesFromDb(cloneCtx, datasetId, nextChunkMeta)
		if err != nil {
			log.Error(ctx, "tryLoadNextChunk call getChunkValuesFromDb fail.", zap.Error(err))
			return err
		}
		// 写入缓存
		q.chunkValueCache.Add(cacheKey, values)
		return nil
	}, nil)
}

// 获取数据的 chunk meta. 当未命中缓存时才去db加载
func (q *queryCli) getChunkMetas(ctx context.Context, datasetId int64) (model.ChunkMeta, error) {
	v, ok := q.chunkMetaCache.Get(datasetId)
	if ok {
		return v, nil
	}

	// sfc
	chunkMeta, err := q.metasSfc.Do(ctx, strconv.FormatInt(datasetId, 32), func(ctx context.Context, key string) (model.ChunkMeta, error) {
		chunkMeta, err := q.getChunkMetasFromDb(ctx, datasetId)
		if err != nil {
			log.Error(ctx, "getChunkMetas call getChunkMetasFromDb fail.", zap.Error(err))
			return nil, err
		}

		// 写入缓存
		q.chunkMetaCache.Add(datasetId, chunkMeta)
		return chunkMeta, nil
	})
	return chunkMeta, err
}
func (q *queryCli) getChunkMetasFromDb(ctx context.Context, datasetId int64) (model.ChunkMeta, error) {
	// 从数据库加载
	line, err := Dataset.GetDatasetInfoByCache(ctx, uint(datasetId))
	if err != nil {
		log.Error(ctx, "getChunkMetas call GetDatasetInfoByCache fail.", zap.Error(err))
		return nil, err
	}
	if line.Status != byte(pb.Status_Status_Finished) {
		err := errors.New("dataset not finished.")
		log.Error(ctx, "getChunkMetas call GetDatasetInfoByCache fail.", zap.Error(err))
		return nil, err
	}

	// 解析 ChunkMeta
	chunkMeta := make(model.ChunkMeta, 0)
	err = sonic.UnmarshalString(line.ChunkMeta, &chunkMeta)
	if err != nil {
		log.Error(ctx, "getChunkMetas call UnmarshalString db ChunkMeta fail.", zap.Error(err))
		return nil, err
	}
	sort.Slice(chunkMeta, func(i, j int) bool {
		return chunkMeta[i].ChunkSn < chunkMeta[j].ChunkSn
	})
	return chunkMeta, nil
}

// 二分查找valueSn
func (q *queryCli) findChunkByValueSn(metas model.ChunkMeta, valueSn int64) (*model.OneChunkMeta, error) {
	if len(metas) == 0 {
		return nil, errors.New("find chunk by sn fail. metas count is zero")
	}

	// 找到最小符合要求的 one chunk meta
	i := sort.Search(len(metas), func(i int) bool {
		return metas[i].EndValueSn >= valueSn
	})
	if i >= len(metas) {
		return nil, errors.New("find chunk by sn fail. not in range")
	}
	meta := metas[i]
	if valueSn >= meta.StartValueSn && valueSn <= meta.EndValueSn { // 检查
		return meta, nil
	}

	return nil, errors.New("find chunk ok. but value sn not in range")
}

// 获取数据集指定valueSn的数据
func (q *queryCli) getChunkValues(ctx context.Context, datasetId int64, meta *model.OneChunkMeta) ([]string, error) {
	cacheKey := strconv.FormatInt(datasetId, 32) + "_" + strconv.FormatInt(int64(meta.ChunkSn), 32)
	v, ok := q.chunkValueCache.Get(cacheKey)
	if ok {
		return v, nil
	}

	values, err := q.valueSfc.Do(ctx, cacheKey, func(ctx context.Context, key string) ([]string, error) {
		values, err := q.getChunkValuesFromDb(ctx, datasetId, meta)
		if err != nil {
			log.Error(ctx, "getChunkValues call getChunkValuesFromDb fail.", zap.Error(err))
			return nil, err
		}
		// 写入缓存
		q.chunkValueCache.Add(cacheKey, values)
		return values, nil
	})
	return values, err
}
func (q *queryCli) getChunkValuesFromDb(ctx context.Context, datasetId int64, meta *model.OneChunkMeta) ([]string, error) {
	// 从数据库加载数据集信息
	d, err := Dataset.GetDatasetInfoByCache(ctx, uint(datasetId))
	if err != nil {
		log.Error(ctx, "getChunkValues call GetDatasetInfoByCache fail.", zap.Error(err))
		return nil, err
	}
	de := &pb.DatasetExtend{}
	err = sonic.UnmarshalString(d.DatasetExtend, de)
	if err != nil {
		log.Error("getChunkValues call UnmarshalString DatasetExtend fail.", zap.Error(err))
		return nil, err
	}
	// 加载chunk数据
	cs, err := chunk_store.NewChunkStore(ctx, uint(datasetId), de, nil, nil)
	if err != nil {
		log.Error(ctx, "getChunkValues call NewChunkStore fail.", zap.Error(err))
		return nil, err
	}
	bs, err := cs.LoadChunk(ctx, meta)
	if err != nil {
		log.Error(ctx, "getChunkValues call LoadChunk fail.", zap.Error(err))
		return nil, err
	}

	// 数据解析
	vr := splitter.NewValueReader(bytes.NewReader(bs), []byte(de.GetValueProcess().GetDelim()), len(bs))
	values := make([]string, 0, meta.EndValueSn+1-meta.StartValueSn)
	for {
		value, err := vr.Next() // 获取下一个值
		if err != nil && err != io.EOF {
			log.Error(ctx, "getChunkValues call ValueReader.Next fail.", zap.Error(err))
			return nil, err
		}
		if len(value) > 0 {
			values = append(values, string(value))
		}
		if err == io.EOF {
			break
		}
	}

	// value数量检查
	if len(values) != int(meta.EndValueSn+1-meta.StartValueSn) {
		err := errors.New("get values length not match chunk meta.")
		log.Error(ctx, "getChunkValues fail.", zap.Error(err))
		return nil, err
	}

	return values, nil
}
