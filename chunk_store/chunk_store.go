package chunk_store

import (
	"bytes"
	"context"
	"errors"
	"hash/crc32"
	"sync/atomic"

	"github.com/zly-app/zapp/log"
	"go.uber.org/zap"

	"github.com/zlyuancn/dataset/conf"
	"github.com/zlyuancn/dataset/model"
	"github.com/zlyuancn/dataset/pb"
)

var ErrStop = errors.New("stop")

// 持久化进度回调, 不会每次完成 chunkSn 都回调
type FlushedLastedCallback func(meta *model.OneChunkMeta)

// 持久化回调, 每次完成 chunkSn 都会回调, 无序
type FlushedCallback func(meta *model.OneChunkMeta)

type ChunkStore interface {
	// 初始化
	Init(resumePoint *ResumePoint)
	// 持久化某个chunk
	FlushChunk(ctx context.Context, chunkData *model.ChunkData)
	// 等待flushChunk完成或者停止
	Wait(ctx context.Context, chunkSn int32) error
	// 与 Wait 相同, 但是入参没有计算断点
	WaitNoChunkSnOffset(ctx context.Context, chunkSn int32) error
	// 加载chunk
	LoadChunk(ctx context.Context, oneChunkMeta *model.OneChunkMeta) ([]byte, error)
	// 关闭
	Close()
}

// 断点
type ResumePoint struct {
	ChunkFinishedCount int32 // chunk 完成数
	ValueFinishedCount int64 // 当前已完成处理的数据总数
	ResumePointOffset  int64 // 断点续传偏移量, 表示已完成的chunk扫描了多少字节
}

type flushResult struct {
	meta *model.OneChunkMeta
	err  error // 如果需要错误处理可扩展
}

type chunkStore struct {
	datasetId uint
	de        *pb.DatasetExtend
	fcb       FlushedLastedCallback
	lcb       FlushedLastedCallback

	csp ChunkStorePersist

	threadLock chan struct{}     // 线程锁
	doneCh     chan *flushResult // 所有 FlushChunk 完成后，将结果发到这个 channel

	stopChan chan struct{} // 停止信号
	onceStop int32         // 只调用一次stop

	flushedChunkSn int32 // 已完成的chunkSn
	waitChunkSn    int32 // 等待chunkSn
	waitErr        error
	resumePoint    *ResumePoint
}

func NewChunkStore(ctx context.Context, datasetId uint, de *pb.DatasetExtend,
	fcb FlushedLastedCallback, lcb FlushedLastedCallback) (ChunkStore, error) {

	csp, err := NewChunkStorePersist(ctx, datasetId, de)
	if err != nil {
		log.Error(ctx, "NewChunkStore call NewChunkStorePersist fail.", zap.Error(err))
		return nil, err
	}

	c := &chunkStore{
		datasetId: datasetId,
		de:        de,
		fcb:       fcb,
		lcb:       lcb,

		csp: csp,

		threadLock: make(chan struct{}, conf.Conf.ChunkStoreThreadCount),
		doneCh:     make(chan *flushResult, max(conf.Conf.ChunkStoreThreadCount, 16)),

		stopChan: make(chan struct{}),

		flushedChunkSn: -1,
		waitChunkSn:    -1,
	}
	return c, nil
}

func (c *chunkStore) Init(resumePoint *ResumePoint) {
	c.resumePoint = resumePoint
	c.flushedChunkSn = resumePoint.ChunkFinishedCount - 1
	go c.callbackDispatcher()
}

func (c *chunkStore) FlushChunk(ctx context.Context, chunkData *model.ChunkData) {
	// 已停止则拒绝处理
	if atomic.LoadInt32(&c.onceStop) != 0 {
		return
	}

	chunkData = &model.ChunkData{
		ChunkSn:      chunkData.ChunkSn + c.resumePoint.ChunkFinishedCount,
		StartValueSn: chunkData.StartValueSn + c.resumePoint.ValueFinishedCount,
		EndValueSn:   chunkData.EndValueSn + c.resumePoint.ValueFinishedCount,
		ChunkData:    chunkData.ChunkData,
		ScanByteNum:  chunkData.ScanByteNum + c.resumePoint.ResumePointOffset,
	}

	// 占用一个线程
	select {
	case c.threadLock <- struct{}{}:
	case <-c.stopChan:
		return
	}

	go func(chunkData *model.ChunkData) {
		// 退出前释放线程
		defer func() {
			<-c.threadLock
		}()

		// 处理 Utf8Bom
		if c.de.GetDataProcess().GetTrimUtf8Bom() && chunkData.ChunkSn == 0 {
			chunkData.ChunkData = bytes.TrimPrefix(chunkData.ChunkData, []byte{0xEF, 0xBB, 0xBF})
		}

		// 获取校验和. 数据校验和是原始数据, 不是压缩后的数据
		checkSum := crc32.ChecksumIEEE(chunkData.ChunkData)

		// 压缩
		compressed, isCompressed, err := Compress(c.de.GetChunkProcess().GetCompressType(), chunkData.ChunkData)
		if err != nil {
			log.Error(ctx, "Compress fail.", zap.Error(err))
		}
		if isCompressed {
			chunkData.ChunkData = compressed
		}

		// 处理
		fr := &flushResult{meta: &model.OneChunkMeta{
			ChunkSn:      chunkData.ChunkSn,
			StartValueSn: chunkData.StartValueSn,
			EndValueSn:   chunkData.EndValueSn,
			Checksum:     checkSum,
			IsCompressed: isCompressed,
			ScanByteNum:  chunkData.ScanByteNum,
		}}
		fr.err = c.csp.FlushChunk(ctx, chunkData.ChunkSn, chunkData.ChunkData)
		if fr.err != nil {
			log.Error(ctx, "FlushChunk call csp.FlushChunk fail.", zap.Error(fr.err))
		}

		// 处理完成后将结果输出
		select {
		case c.doneCh <- fr:
		case <-c.stopChan:
			return
		}
	}(chunkData)
}

// 等待至少 ChunkSn 处理完成
func (c *chunkStore) Wait(ctx context.Context, chunkSn int32) error {
	if chunkSn < 0 {
		return errors.New("chunkSn must >= 0")
	}

	// 尝试写入要等待的 sn
	if !atomic.CompareAndSwapInt32(&c.waitChunkSn, -1, chunkSn) {
		return errors.New("Repeat the Wait operation.")
	}

	// 已经完成了
	if atomic.LoadInt32(&c.flushedChunkSn) >= chunkSn {
		return nil
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.stopChan:
	}

	if c.waitErr != nil {
		return c.waitErr
	}

	// 已经完成了
	if atomic.LoadInt32(&c.flushedChunkSn) >= chunkSn {
		return nil
	}

	return ErrStop
}

// 与 Wait 相同, 但是入参没有计算断点
func (c *chunkStore) WaitNoChunkSnOffset(ctx context.Context, chunkSn int32) error {
	return c.Wait(ctx, chunkSn+c.resumePoint.ChunkFinishedCount)
}

func (c *chunkStore) LoadChunk(ctx context.Context, oneChunkMeta *model.OneChunkMeta) ([]byte, error) {
	bs, err := c.csp.LoadChunk(ctx, oneChunkMeta.ChunkSn)
	if err != nil {
		log.Error(ctx, "LoadChunk call csp.LoadChunk fail.", zap.Error(err))
		return nil, err
	}

	// 解压缩
	if oneChunkMeta.IsCompressed {
		bs, err = UnCompress(c.de.GetChunkProcess().GetCompressType(), bs)
		if err != nil {
			log.Error(ctx, "LoadChunk call UnCompress fail.", zap.Error(err))
			return nil, err
		}
	}

	// 检查校验和
	checkSum := crc32.ChecksumIEEE(bs)
	if checkSum != oneChunkMeta.Checksum {
		err = errors.New("check checksum fail.")
		log.Error(ctx, "LoadChunk fail.", zap.Error(err))
		return nil, err
	}

	return bs, nil
}

func (c *chunkStore) Close() {
	if atomic.AddInt32(&c.onceStop, 1) == 1 {
		close(c.stopChan)
	}
}

func (c *chunkStore) callbackDispatcher() {
	pending := make(map[int32]*flushResult, 16)
	var fr *flushResult

	expectedSn := c.resumePoint.ChunkFinishedCount

	for {
		select {
		case <-c.stopChan: // 已停止
			return
		case fr = <-c.doneCh:
		}

		// 检查错误
		if fr.err != nil {
			c.waitErr = fr.err
			c.Close()
			return
		}

		pending[fr.meta.ChunkSn] = fr
		// 处理回调
		c.fcb(fr.meta)

		// 尝试按序处理
		var lastedFinishedFr *flushResult // 用于回调的 fr
		for {
			fr, ok := pending[expectedSn]
			if !ok {
				break
			}

			lastedFinishedFr = fr
			delete(pending, expectedSn)
			expectedSn++
		}

		// 检查是否有新的已完成的sn
		if lastedFinishedFr != nil {
			// 处理回调
			c.lcb(lastedFinishedFr.meta)

			// 更新已完成的
			atomic.StoreInt32(&c.flushedChunkSn, expectedSn-1)
			// 检查wait
			waitSn := atomic.LoadInt32(&c.waitChunkSn)
			if waitSn >= 0 && expectedSn > waitSn { // 一旦满足则终止
				c.Close()
				return
			}
		}
	}
}
