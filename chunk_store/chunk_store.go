package chunk_store

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/zlyuancn/dataset/conf"
	"github.com/zlyuancn/dataset/pb"
	"github.com/zlyuancn/splitter"
)

var ErrStop = errors.New("stop")

// 持久化进度回调, 不会每次完成 chunkSn 都回调
type FlushedLastedCallback func(args *splitter.FlushChunkArgs)

// 持久化回调, 每次完成 chunkSn 都会回调, 无序
type FlushedCallback func(args *splitter.FlushChunkArgs)

type ChunkStore interface {
	// 初始化
	Init(resumePoint *ResumePoint)
	// 持久化某个chunk
	FlushChunk(ctx context.Context, args *splitter.FlushChunkArgs)
	// 等待flushChunk完成或者停止
	Wait(ctx context.Context, chunkSn int32) error
	// 与 Wait 相同, 但是入参没有计算断点
	WaitNoChunkSnOffset(ctx context.Context, chunkSn int32) error

	Close()
}

// 断点
type ResumePoint struct {
	ChunkFinishedCount int32 // chunk 完成数
	ValueFinishedCount int64 // 当前已完成处理的数据总数
	ResumePointOffset  int64 // 断点续传偏移量, 表示已完成的chunk扫描了多少字节
}

type flushResult struct {
	args *splitter.FlushChunkArgs
	err  error // 如果需要错误处理可扩展
}

type chunkStore struct {
	cp  *pb.ChunkProcess
	fcb FlushedLastedCallback
	lcb FlushedLastedCallback

	threadLock chan struct{}     // 线程锁
	doneCh     chan *flushResult // 所有 FlushChunk 完成后，将结果发到这个 channel

	stopChan chan struct{} // 停止信号
	onceStop int32         // 只调用一次stop

	flushedChunkSn int32 // 已完成的chunkSn
	waitChunkSn    int32 // 等待chunkSn
	waitErr        error
	resumePoint    *ResumePoint
}

func NewChunkStore(ctx context.Context, cp *pb.ChunkProcess,
	fcb FlushedLastedCallback, lcb FlushedLastedCallback) (ChunkStore, error) {
	c := &chunkStore{
		cp:  cp,
		fcb: fcb,
		lcb: lcb,

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

func (c *chunkStore) FlushChunk(ctx context.Context, args *splitter.FlushChunkArgs) {
	// 已停止则拒绝处理
	if atomic.LoadInt32(&c.onceStop) != 0 {
		return
	}

	args = &splitter.FlushChunkArgs{
		ChunkSn:      args.ChunkSn + int(c.resumePoint.ChunkFinishedCount),
		StartValueSn: args.StartValueSn + c.resumePoint.ValueFinishedCount,
		EndValueSn:   args.EndValueSn + c.resumePoint.ValueFinishedCount,
		ChunkData:    args.ChunkData,
		ScanByteNum:  args.ScanByteNum + c.resumePoint.ResumePointOffset,
	}

	// 占用一个线程
	select {
	case c.threadLock <- struct{}{}:
	case <-c.stopChan:
		return
	}

	go func(args *splitter.FlushChunkArgs) {
		// 退出前释放线程
		defer func() {
			<-c.threadLock
		}()

		// todo 处理
		fr := &flushResult{args: args}

		// 处理完成后将结果输出
		select {
		case c.doneCh <- fr:
		case <-c.stopChan:
			return
		}
	}(args)
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

		pending[int32(fr.args.ChunkSn)] = fr
		// 处理回调
		c.fcb(fr.args)

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
			c.lcb(lastedFinishedFr.args)

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
