package main

import (
	"context"

	"github.com/zly-app/grpc"
	"github.com/zly-app/service/cron"
	"github.com/zly-app/uapp"
	"github.com/zly-app/zapp/config"
	"github.com/zly-app/zapp/log"
	"github.com/zlyuancn/redis_tool"

	"github.com/zlyuancn/dataset_backend/module"
	"github.com/zlyuancn/dataset_backend/pb"
	"github.com/zlyuancn/dataset_backend/syslog"

	"github.com/zlyuancn/dataset_backend/conf"
	"github.com/zlyuancn/dataset_backend/logic"
)

func main() {
	redis_tool.SetManualInit()
	config.RegistryApolloNeedParseNamespace(conf.ConfigKey)

	app := uapp.NewApp("dataset",
		grpc.WithService(),        // 启用 grpc 服务
		grpc.WithGatewayService(), // 启用网关服务
		cron.WithService(),        // 启用定时服务
	)
	defer app.Exit()

	err := conf.Init()
	if err != nil {
		log.Error("Init config fail. err=", err)
		return
	}

	redis_tool.RedisClientName = conf.Conf.RedisName
	redis_tool.ManualInit()

	module.InitQuery()

	// rpc服务
	pb.RegisterDatasetServiceServer(grpc.Server("dataset"), logic.NewServer())

	// rpc网关
	client := pb.NewDatasetServiceClient(grpc.GetGatewayClientConn("dataset"))
	_ = pb.RegisterDatasetServiceHandlerClient(context.Background(), grpc.GetGatewayMux(), client)

	// 定时器
	cron.RegistryHandler("recover", "@every 10m", true, func(ctx cron.IContext) error {
		return nil
	})

	// syslog
	syslog.Init()

	app.Run()
}
