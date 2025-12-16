package module

import (
	"context"
	"io"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/bytedance/sonic"
	"github.com/zly-app/zapp/component/gpool"
	"github.com/zly-app/zapp/log"
	"github.com/zlyuancn/dataset/client/db"
	"github.com/zlyuancn/dataset/conf"
	"github.com/zlyuancn/dataset/dao/dataset"
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
func (processorCli) CreateProcessor(ctx context.Context, datasetInfo *dataset.Model) {
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

	de          *pb.DatasetExtend
	datasetInfo *dataset.Model
	pStatus     *model.CacheDatasetProcessStatus // 处理状态

	lockKeyUnlock redis_tool.KeyUnlock   // 任务处理锁解锁方法
	lockKeyRenew  redis_tool.KeyTtlRenew // 任务处理锁续期方法

	stopChan         chan struct{} // 停止信号
	renewKeyStopChan chan struct{} // key自动续期具有独立的停止信号
	onceStop         int32         // 只调用一次stop

	ds datasource.DataSource

	gotStopFlag model.StopFlag // 停止标记
	statusInfo  string         // 停止时的状态信息传递
}

func newProcessorLauncher(datasetInfo *dataset.Model,
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

	// 构造数据源
	p.ds, err = datasource.NewDataSource(p.ctx, de.GetDataProcess())
	if err != nil {
		log.Error(p.ctx, "newProcessorLauncher call NewDataSource fail", zap.Any("dataset", datasetInfo), zap.Error(err))
		return nil, err
	}

	// todo 构造持久化工具

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
	// 重新开始
	restart := true

	// 尝试断点续传
	if p.pStatus.ResumePointOffset > 0 {
		resume, err := p.ds.SetBreakpoint(p.pStatus.ResumePointOffset)
		if err != nil {
			log.Error(p.ctx, "try ResumePointOffset fail.", zap.Error(err))
		}
		restart = !resume
	}

	// 重新开始处理
	if restart {
		dataStreamLen := p.ds.GetDataStreamLen()
		chunkSizeLimit := conf.Conf.ChunkSizeLimit
		p.pStatus = &model.CacheDatasetProcessStatus{
			DataStreamLen:  dataStreamLen,
			ChunkSizeLimit: chunkSizeLimit,
			ChunkTotal:     0,
		}
		if dataStreamLen > 0 && chunkSizeLimit > 0 {
			p.pStatus.ChunkTotal = int32((dataStreamLen-1)/int64(chunkSizeLimit) + 1)
		}
	}
}

func (p *processorLauncher) flushChunkHandler(args *splitter.FlushChunkArgs) {
	// todo chunkSn和dataSn都有偏移
	// todo 如何并行上传? 是否考虑一个元数据缓存区, 记录所有已完成上传的元数据的chunkSn和nextDataSn
	// todo 并行上传的逻辑放到 chunkPersistence 的模块中, 不是由持久化类型实现, 而是一个外壳实现, 且该外壳还能将按顺序输出已完成的chunkSn
	// todo 这个 chunkPersistence 模块还接管了 chunkSn 偏移和 dataSn 偏移
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

	// 获取读取器
	rd, err := p.ds.GetReader()
	if err != nil {
		p.submitStopFlag("GetReader fail. err=" + err.Error())
		log.Error(p.ctx, "GetReader fail", zap.Error(err))
		return
	}

	// 开始读取
	sp := splitter.NewSplitter(splitter.Conf{
		Delim:                 []byte(p.de.GetValueProcess().GetDelim()),
		ChunkSizeLimit:        int(p.pStatus.ChunkSizeLimit),
		FlushChunkHandler:     p.flushChunkHandler,
		ValueMaxScanSizeLimit: conf.Conf.ValueMaxScanSizeLimit,
		ValueFilter:           value_filter.NewValueFilter(p.de.GetValueProcess()),
	})

	// 开始处理
	splitterIsFinished, err := p.runSplitter(sp, rd)
	if err != nil {
		p.submitStopFlag("RunSplit fail. err=" + err.Error())
		log.Error(p.ctx, "RunSplit fail", zap.Error(err))
		return
	}

	// 如果不是分隔完成就返回, 必然是停止运行
	if !splitterIsFinished {
		return
	}

	// todo 已完成
	// todo splitter 处理完成并不代表 chunk 持久化完成. 这里要等待chunk持久化完成

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

	// todo
}
