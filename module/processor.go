package module

import (
	"context"
	"hash/crc32"
	"io"
	"sort"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/bytedance/sonic"
	"github.com/zly-app/component/redis"
	"github.com/zly-app/zapp/component/gpool"
	"github.com/zly-app/zapp/log"
	"github.com/zly-app/zapp/pkg/utils"
	"github.com/zlyuancn/dataset/chunk_store"
	"github.com/zlyuancn/dataset/client/db"
	"github.com/zlyuancn/dataset/conf"
	"github.com/zlyuancn/dataset/dao/dataset_list"
	"github.com/zlyuancn/dataset/datasource"
	"github.com/zlyuancn/dataset/handler"
	"github.com/zlyuancn/dataset/model"
	"github.com/zlyuancn/dataset/pb"
	"github.com/zlyuancn/dataset/value_filter"
	"github.com/zlyuancn/redis_tool"
	"github.com/zlyuancn/splitter"
	"go.uber.org/zap"
)

var Processor = processorCli{}

type processorCli struct{}

// 创建处理器
func (processorCli) CreateProcessor(ctx context.Context, datasetInfo *dataset_list.Model) {
	// 加运行处理锁
	lockKey := CacheKey.GetRunProcessLock(int(datasetInfo.DatasetId))
	unlock, renew, err := redis_tool.AutoLock(ctx, lockKey, time.Duration(conf.Conf.RunLockExtraTtl)*time.Second)
	if err == redis_tool.LockIsUsedByAnother { // 加锁失败
		return
	}
	if err != nil { // 加锁异常
		log.Error("CreateProcessor call AutoLock fail.", zap.Error(err))
		return
	}

	// 创建启动器
	p, err := newProcessorLauncher(datasetInfo, unlock, renew)
	if err != nil {
		log.Error("CreateProcessor call newProcessorLauncher fail.", zap.Error(err))
		return
	}

	handler.Trigger(ctx, handler.AfterRunDatasetProcess, &handler.Info{
		Dataset: datasetInfo,
	})

	// 运行
	gpool.GetDefGPool().Go(func() error {
		p.Run()
		return nil
	}, nil)
}

type processorLauncher struct {
	ctx    context.Context
	cancel context.CancelFunc

	datasetInfo *dataset_list.Model
	de          *pb.DatasetExtend
	pStatus     *model.CacheDatasetProcessStatus // 处理状态
	chunkMeta   model.ChunkMeta                  // chunk元数据

	lockKeyUnlock redis_tool.KeyUnlock   // 任务处理锁解锁方法
	lockKeyRenew  redis_tool.KeyTtlRenew // 任务处理锁续期方法

	ds datasource.DataSource    // 数据源处理器
	cs chunk_store.ChunkStore   // chunk 储存
	vf value_filter.ValueFilter // 值过滤器

	stopChan         chan struct{} // 停止信号
	onceStop         int32         // 只调用一次stop
	renewKeyStopChan chan struct{} // key自动续期具有独立的停止信号

	isFinished  bool           // 是否已完成
	gotStopFlag model.StopFlag // 停止标记
	statusInfo  string         // 停止时的状态信息传递
}

func newProcessorLauncher(datasetInfo *dataset_list.Model,
	unlock redis_tool.KeyUnlock, renew redis_tool.KeyTtlRenew) (*processorLauncher, error) {

	de := &pb.DatasetExtend{}
	err := sonic.UnmarshalString(datasetInfo.DatasetExtend, de)
	if err != nil {
		log.Error("newProcessorLauncher UnmarshalString DatasetExtend fail.", zap.Error(err))
		return nil, err
	}

	p := &processorLauncher{
		de:          de,
		datasetInfo: datasetInfo,

		lockKeyUnlock: unlock,
		lockKeyRenew:  renew,

		stopChan:         make(chan struct{}),
		renewKeyStopChan: make(chan struct{}),
	}
	p.ctx, p.cancel = context.WithCancel(context.Background())

	// 从cache加载状态
	err = p.loadCacheStatus()
	if err != nil {
		log.Error(p.ctx, "newProcessorLauncher call loadCacheStatus fail.", zap.Any("dataset", datasetInfo), zap.Error(err))
		return nil, err
	}

	// 加载元数据
	err = p.loadChunkMeta()
	if err != nil {
		log.Error(p.ctx, "newProcessorLauncher call loadChunkMeta fail.", zap.Any("dataset", datasetInfo), zap.Error(err))
		return nil, err
	}

	// 值过滤器
	p.vf, err = value_filter.NewValueFilter(de.GetValueProcess())
	if err != nil {
		log.Error(p.ctx, "newProcessorLauncher call NewValueFilter fail", zap.Any("dataset", datasetInfo), zap.Error(err))
		return nil, err
	}

	// 构造数据源
	p.ds, err = datasource.NewDataSource(p.ctx, de.GetDataProcess())
	if err != nil {
		log.Error(p.ctx, "newProcessorLauncher call NewDataSource fail", zap.Any("dataset", datasetInfo), zap.Error(err))
		return nil, err
	}

	// chunk 储存
	p.cs, err = chunk_store.NewChunkStore(p.ctx, de.GetChunkProcess(), p.flushChunkHandler, p.flushLastedChunkHandler)
	if err != nil {
		log.Error(p.ctx, "newProcessorLauncher call NewChunkStore fail", zap.Any("dataset", datasetInfo), zap.Error(err))
		return nil, err
	}

	return p, nil
}

// 从redis加载状态, 对于服务突然宕机, 状态是不会写入到db中, 而运行中的任务的实际状态都应该以redis为准
func (p *processorLauncher) loadCacheStatus() error {
	status, ok, err := Dataset.LoadCacheProcessStatus(p.ctx, int(p.datasetInfo.DatasetId))
	if err != nil {
		return err
	}
	if ok {
		p.pStatus = status
	}
	return nil
}

// 加载元数据
func (p *processorLauncher) loadChunkMeta() error {
	chunkMeta := make(model.ChunkMeta, 0)
	err := sonic.UnmarshalString(p.datasetInfo.ChunkMeta, &chunkMeta)
	if err != nil {
		log.Error(p.ctx, "loadChunkMeta call UnmarshalString db ChunkMeta fail.", zap.Error(err))
		return err
	}

	// 加载缓存的 ChunkMeta
	key := CacheKey.GetChunkMeta(int(p.datasetInfo.DatasetId))
	var meta map[string]string
	rdb, err := db.GetRedis()
	if err == nil {
		meta, err = rdb.HGetAll(p.ctx, key).Result()
	}
	if err != nil {
		log.Error(p.ctx, "loadChunkMeta call HGetAll fail.", zap.Error(err))
		return err
	}

	// 将 meta 转为本地元数据
	chunkMeta = make(model.ChunkMeta, 0, len(meta))
	for _, v := range meta {
		one := &model.OneChunkMeta{}
		err := sonic.UnmarshalString(v, one)
		if err != nil {
			log.Error(p.ctx, "loadChunkMeta call UnmarshalString cache OneChunkMeta fail.", zap.String("one", v), zap.Error(err))
			return err
		}
		if one.ChunkSn < p.pStatus.ChunkFinishedCount { // 仅保留断点内的
			chunkMeta = append(chunkMeta, one)
		}
	}
	// 拼上db中的chunkMeta
	for _, one := range p.chunkMeta {
		field := strconv.Itoa(int(one.ChunkSn))
		if _, ok := meta[field]; ok { // 如果缓存中已存在则忽略
			continue
		}
		if one.ChunkSn < p.pStatus.ChunkFinishedCount { // 仅保留断点内的
			chunkMeta = append(chunkMeta, one)
		}
	}
	sort.Slice(chunkMeta, func(i, j int) bool {
		return chunkMeta[i].ChunkSn < chunkMeta[j].ChunkSn
	})
	p.chunkMeta = chunkMeta
	return nil
}

// 写入处理状态到缓存, 失败的后果是重跑部分数据
func (p *processorLauncher) writeCacheStatus() error {
	key := CacheKey.GetCacheDatasetProcessStatus(int(p.datasetInfo.DatasetId))
	v, err := sonic.MarshalString(p.pStatus)
	if err != nil {
		log.Error(p.ctx, "writeCacheStatus fail.", zap.Any("datasetInfo", p.datasetInfo), zap.Any("status", p.pStatus), zap.Error(err))
		return err
	}

	rdb, err := db.GetRedis()
	if err != nil {
		return err
	}
	err = rdb.Set(p.ctx, key, v, 0).Err()
	if err != nil {
		log.Error(p.ctx, "writeCacheStatus fail.", zap.Error(err))
		return err
	}
	return nil
}

// 循环对运行锁续期
func (p *processorLauncher) loopLockKeyRenew() {
	t := time.NewTicker(time.Duration(conf.Conf.RunLockRenewInterval) * time.Second)
	defer t.Stop()

	errCount := 0
	for {
		select {
		case <-p.renewKeyStopChan:
			_ = p.lockKeyUnlock() // 主动解锁
			return
		case <-t.C:
			err := p.lockKeyRenew(p.ctx, time.Duration(conf.Conf.RunLockExtraTtl)*time.Second)
			if err != nil {
				log.Error(p.ctx, "lockKeyRenew fail.", zap.Uint("datasetId", p.datasetInfo.DatasetId), zap.Error(err))
				errCount++
			} else {
				errCount = 0
			}
			if errCount >= conf.Conf.RunLockRenewMaxContinuousErrCount {
				log.Error(p.ctx, "lockKeyRenew Continuous fail.", zap.Uint("datasetId", p.datasetInfo.DatasetId), zap.Error(err))
				p.submitStopFlag("lockKeyRenew Continuous fail")
			}
		}
	}
}

// 循环检查停止flag
func (p *processorLauncher) loopCheckStopFlag() {
	t := time.NewTicker(time.Duration(conf.Conf.CheckStopFlagInterval) * time.Second)
	defer t.Stop()

	for {
		select {
		case <-p.stopChan:
			return
		case <-t.C:
			flag, _ := Dataset.GetStopFlag(p.ctx, int(p.datasetInfo.DatasetId))
			if flag != model.StopFlag_None {
				log.Warn(p.ctx, "loopCheckStopFlag got stop flag")
				p.gotStopFlag = flag
				p.submitStopFlag("got stop flag is " + strconv.Itoa(int(flag)))
			}
		}
	}
}

// 尝试恢复断点
func (p *processorLauncher) tryResumePointOffset() {
	// 尝试断点续传
	if p.pStatus.ResumePointOffset > 0 {
		resume, err := p.ds.SetBreakpoint(p.ctx, p.pStatus.ResumePointOffset)
		if err != nil {
			log.Error(p.ctx, "try ResumePointOffset fail.", zap.Error(err))
		}
		if resume {
			return
		}
	}

	// 重新开始处理
	dataStreamLen := p.ds.GetDataStreamLen(p.ctx)
	chunkSizeLimit := conf.Conf.ChunkSizeLimit
	p.pStatus = &model.CacheDatasetProcessStatus{
		DataStreamLen:  dataStreamLen,
		ChunkSizeLimit: chunkSizeLimit,
		ChunkTotal:     0,
	}
	if dataStreamLen > 0 && chunkSizeLimit > 0 {
		p.pStatus.ChunkTotal = int32((dataStreamLen-1)/int64(chunkSizeLimit) + 1)
	}

	// 本地chunk也失效了
	p.chunkMeta = make(model.ChunkMeta, 0)
}

// 处理完成的每个 chunk 回调
func (p *processorLauncher) flushChunkHandler(args *splitter.FlushChunkArgs) {
	oneChunk := &model.OneChunkMeta{
		ChunkSn:      int32(args.ChunkSn),
		StartValueSn: args.StartValueSn,
		EndValueSn:   args.EndValueSn,
		Checksum:     crc32.ChecksumIEEE(args.ChunkData),
	}
	p.chunkMeta = append(p.chunkMeta, oneChunk)

	v, err := sonic.MarshalString(oneChunk)
	if err != nil {
		log.Error(p.ctx, "flushChunkHandler call MarshalString fail", zap.Error(err))
		p.submitStopFlag("flushChunkHandler call MarshalString fail. err=%s" + err.Error())
		return
	}

	// 保存 ChunkMeta
	key := CacheKey.GetChunkMeta(int(p.datasetInfo.DatasetId))
	field := strconv.Itoa(int(oneChunk.ChunkSn))
	rdb, err := db.GetRedis()
	if err == nil {
		err = rdb.HSet(p.ctx, key, field, v).Err()
	}
	if err != nil {
		log.Error(p.ctx, "flushChunkHandler call HSet fail", zap.Error(err))
		p.submitStopFlag("flushChunkHandler call HSet fail. err=%s" + err.Error())
		return
	}
}

// 处理完成的最后已完成 chunk 回调
func (p *processorLauncher) flushLastedChunkHandler(args *splitter.FlushChunkArgs) {
	p.pStatus.ChunkFinishedCount = int32(args.ChunkSn + 1)
	p.pStatus.ValueFinishedCount = args.EndValueSn + 1
	p.pStatus.ResumePointOffset = args.ScanByteNum

	// 持久化状态
	err := p.writeCacheStatus()
	if err != nil {
		log.Error(p.ctx, "flushLastedChunkHandler call writeCacheStatus fail.", zap.Error(err))
	}
}

func (p *processorLauncher) runSplitter(sp splitter.Splitter, rd io.Reader) (bool, error) {
	// 开始处理
	runSplitStopCh := make(chan error, 1)
	go func() {
		err := sp.RunSplit(rd)
		runSplitStopCh <- err
	}()

	// 等待处理
	select {
	case <-p.stopChan:
		sp.Stop()
		return false, nil
	case err := <-runSplitStopCh:
		if err == splitter.ErrSplitterIsStarted {
			return false, nil
		}
		if err != nil {
			p.submitStopFlag("RunSplit fail. err=" + err.Error())
			log.Error(p.ctx, "RunSplit fail", zap.Error(err))
			return false, err
		}
		return true, nil
	}
}

func (p *processorLauncher) Run() {
	log.Warn(p.ctx, "dataset run process", zap.Any("datasetInfo", p.datasetInfo))

	go p.loopLockKeyRenew() // 循环续期
	// go p.loopWriteProgressStatus() // 循环写入处理状态
	go p.loopCheckStopFlag() // 循环检查停止flag

	defer func() {
		p.submitStopFlag("stop") // 这里防止中途panic无法关闭上面的循环处理协程
		p.stopSideEffect()       // 处理停止后副作用
	}()

	// 尝试恢复断点
	p.tryResumePointOffset()

	// 为 ChunkStore 设置断点记录
	p.cs.Init(&chunk_store.ResumePoint{
		ChunkFinishedCount: p.pStatus.ChunkFinishedCount,
		ValueFinishedCount: p.pStatus.ValueFinishedCount,
		ResumePointOffset:  p.pStatus.ResumePointOffset,
	})

	// 获取读取器
	rd, err := p.ds.GetReader(p.ctx)
	if err != nil {
		p.submitStopFlag("GetReader fail. err=" + err.Error())
		log.Error(p.ctx, "GetReader fail", zap.Error(err))
		return
	}

	// 开始读取
	lastChunkSn := int32(-1)
	sp := splitter.NewSplitter(splitter.Conf{
		Delim:          []byte(p.de.GetValueProcess().GetDelim()),
		ChunkSizeLimit: int(p.pStatus.ChunkSizeLimit),
		FlushChunkHandler: func(args *splitter.FlushChunkArgs) {
			lastChunkSn = int32(args.ChunkSn)
			p.cs.FlushChunk(p.ctx, args)
		},
		ValueMaxScanSizeLimit: conf.Conf.ValueMaxScanSizeLimit,
		ValueFilter:           p.vf.Handler,
	})

	// 开始处理分隔
	splitterIsFinished, err := p.runSplitter(sp, rd)
	if err != nil {
		p.submitStopFlag("RunSplit fail. err=" + err.Error())
		log.Error(p.ctx, "RunSplit fail", zap.Error(err))
		return
	}
	if !splitterIsFinished { // 如果不是分隔完成就返回, 必然是停止运行
		return
	}

	// 可能之前就处理完成, 但是由于某种情况中断了, 所以这里分割器就读不到数据
	if lastChunkSn == -1 && p.pStatus.ChunkFinishedCount > 0 {
		// 标记为已完成
		p.isFinished = true
		return
	}

	if lastChunkSn == -1 {
		p.submitStopFlag("lastChunkSn is -1")
		log.Error(p.ctx, "Run fail. lastChunkSn is -1")
		return
	}

	// 等待持久化完成
	err = p.cs.WaitNoChunkSnOffset(p.ctx, lastChunkSn)
	sp.Stop()

	// 检查错误
	if err == chunk_store.ErrStop || err == context.Canceled {
		return
	}
	if err != nil {
		p.submitStopFlag("ChunkStore err=%s" + err.Error())
		log.Error(p.ctx, "Run call WaitNoChunkSnOffset fail.", zap.Error(err))
		return
	}

	// 标记为已完成
	p.isFinished = true
}

// 内部发起停止信号
func (p *processorLauncher) submitStopFlag(statusInfo string) {
	if atomic.AddInt32(&p.onceStop, 1) == 1 {
		p.statusInfo = statusInfo
		close(p.stopChan)
		p.cancel()
	}
}

// 停止后置逻辑
func (p *processorLauncher) stopSideEffect() {
	defer func() {
		close(p.renewKeyStopChan) // 停止续期
	}()

	// 替换ctx
	p.ctx = utils.Ctx.CloneContext(p.ctx)
	p.ctx = utils.Trace.CtxStart(p.ctx, "stopSideEffect")
	defer utils.Trace.CtxEnd(p.ctx)

	p.ds.Close()
	p.cs.Close()

	isStopped := false
	status := p.datasetInfo.Status
	statusInfo := p.statusInfo

	if p.isFinished {
		statusInfo = "finished"
		status = byte(pb.Status_Status_Finished)
	}

	// 检查停止标记
	switch p.gotStopFlag {
	case model.StopFlag_Stop:
		isStopped = true
		status = byte(pb.Status_Status_Stopped)
	}

	// 处理 ChunkMeta
	chunkMeta, err := sonic.MarshalString(p.chunkMeta)
	if err != nil {
		log.Error(p.ctx, "stopSideEffect call MarshalString chunkMeta fail.", zap.Error(err))
		return // 这里失败等重试
	}

	// 更新数据
	t := time.Now()
	updateData := map[string]any{
		"chunk_meta":     chunkMeta,
		"value_total":    p.pStatus.ValueFinishedCount,
		"processed_time": t.Unix(),
		"status":         int(status),
		"status_info":    statusInfo,
	}
	err = dataset_list.UpdateOne(p.ctx, int(p.datasetInfo.DatasetId), updateData, 0)
	if err != nil {
		log.Error(p.ctx, "stopSideEffect call UpdateOne fail.", zap.Error(err))
		return // 这里失败等重试
	}

	// 更新 datasetInfo
	p.datasetInfo.ChunkMeta = chunkMeta
	p.datasetInfo.ValueTotal = p.pStatus.ValueFinishedCount
	p.datasetInfo.ProcessedTime = t.Unix()
	p.datasetInfo.Status = status
	p.datasetInfo.StatusInfo = statusInfo

	// 删除缓存
	keys := []string{
		CacheKey.GetDatasetInfo(int(p.datasetInfo.DatasetId)), // 删除数据集缓存
		CacheKey.GetStopFlag(int(p.datasetInfo.DatasetId)),    // 停止 flag
	}
	if isStopped || p.isFinished { // 如果是最后阶段则删除状态
		keys = append(keys, CacheKey.GetCacheDatasetProcessStatus(int(p.datasetInfo.DatasetId)))
	}
	if p.isFinished { // 如果是已完成则删除 ChunkMeta
		keys = append(keys, CacheKey.GetChunkMeta(int(p.datasetInfo.DatasetId)))
	}
	rdb, err := db.GetRedis()
	if err == nil {
		err = rdb.Del(p.ctx, keys...).Err()
	}
	if err != nil && err != redis.Nil {
		log.Error(p.ctx, "stopSideEffect Del cacheKey fail.", zap.Error(err))
		// return // 这里不影响主流程
	}

	hInfo := &handler.Info{
		T:       t,
		Dataset: p.datasetInfo,
	}
	if p.isFinished {
		handler.Trigger(p.ctx, handler.DatasetProcessFinished, hInfo)
	} else if isStopped {
		handler.Trigger(p.ctx, handler.AfterDatasetProcessStopped, hInfo)
	} else {
		handler.Trigger(p.ctx, handler.DatasetProcessRunFailureExit, hInfo)
	}
}
