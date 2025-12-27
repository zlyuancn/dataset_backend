package datasource

import (
	"context"
	"fmt"
	"io"
	rawHttp "net/http"
	"strings"

	"github.com/spf13/cast"
	"github.com/zly-app/component/http"
	"github.com/zly-app/zapp/filter"
	"github.com/zly-app/zapp/log"
	"go.uber.org/zap"

	"github.com/zlyuancn/dataset/pb"
)

type uriFileDataSource struct {
	ctx    context.Context
	uf     *pb.DataSourceUriFile
	reader io.ReadCloser
}

// 尝试断点续传
func (u *uriFileDataSource) SetBreakpoint(ctx context.Context, offset int64) (bool, error) {
	header := make(http.Header, len(u.uf.GetHeaders())+1)
	for _, kv := range u.uf.GetHeaders() {
		header.Set(kv.GetK(), kv.GetV())
	}
	header.Set("Range", fmt.Sprintf("bytes=%d-", offset))

	method := strings.ToUpper(u.uf.GetMethod())
	if method == "" {
		method = rawHttp.MethodGet
	}
	r := &http.Request{
		Method:             method,
		Path:               u.uf.GetUri(),
		InsecureSkipVerify: u.uf.GetInsecureSkipVerify(),
		Header:             header,
		OutIsStream:        true, // 响应是一个流
		Proxy:              u.uf.GetProxy(),
	}

	ctx = filter.WithoutFilterName(ctx, "base.timeout", "base.gpool")
	sp, err := http.NewClient("uriFileDataSource").Do(ctx, r)
	if err != nil {
		log.Error(ctx, "SetBreakpoint fail.", zap.Error(err))
		return false, err
	}
	u.reader = sp.BodyStream

	// 检查是否支持断点
	if sp.StatusCode != rawHttp.StatusPartialContent {
		log.Warn(ctx, "SetBreakpoint fail. StatusCode is %d", sp.StatusCode)
		return false, nil
	}
	return true, nil
}

// 尝试数据长度
func (u *uriFileDataSource) GetDataStreamLen(ctx context.Context) int64 {
	header := make(http.Header, len(u.uf.GetHeaders()))
	for _, kv := range u.uf.GetHeaders() {
		header.Set(kv.GetK(), kv.GetV())
	}
	r := &http.Request{
		Method:             "HEAD",
		Path:               u.uf.GetUri(),
		InsecureSkipVerify: u.uf.GetInsecureSkipVerify(),
		Header:             header,
		OutIsStream:        true, // 响应是一个流
		Proxy:              u.uf.GetProxy(),
	}

	ctx = filter.WithoutFilterName(ctx, "base.gpool")
	sp, err := http.NewClient("uriFileDataSource").Do(ctx, r)
	if err != nil {
		log.Error(ctx, "GetDataStreamLen fail.", zap.Error(err))
		return 0
	}
	_ = sp.BodyStream.Close()
	return cast.ToInt64(sp.Header.Get("Content-Length"))
}

func (u *uriFileDataSource) GetReader(ctx context.Context) (io.Reader, error) {
	if u.reader != nil {
		return u.reader, nil
	}

	method := strings.ToUpper(u.uf.GetMethod())
	if method == "" {
		method = rawHttp.MethodGet
	}
	header := make(http.Header, len(u.uf.GetHeaders()))
	for _, kv := range u.uf.GetHeaders() {
		header.Set(kv.GetK(), kv.GetV())
	}
	r := &http.Request{
		Method:             method,
		Path:               u.uf.GetUri(),
		InsecureSkipVerify: u.uf.GetInsecureSkipVerify(),
		Header:             header,
		OutIsStream:        true, // 响应是一个流
		Proxy:              u.uf.GetProxy(),
	}

	ctx = filter.WithoutFilterName(ctx, "base.timeout", "base.gpool")
	sp, err := http.NewClient("uriFileDataSource").Do(ctx, r)
	if err != nil {
		return nil, err
	}
	if sp.StatusCode != rawHttp.StatusOK {
		err = fmt.Errorf("StatusCode not %d is %d", rawHttp.StatusOK, sp.StatusCode)
		return nil, err
	}

	u.reader = sp.BodyStream
	return sp.BodyStream, nil
}

func (u *uriFileDataSource) Close() {
	if u.reader != nil {
		_ = u.reader.Close()
	}
	return
}

func newUriFileDataSource(ctx context.Context, dp *pb.DataProcess) (DataSource, error) {
	u := &uriFileDataSource{
		uf: dp.GetUriFile(),
	}
	return u, nil
}
