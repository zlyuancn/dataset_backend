# dataset_backend

数据集后端服务，提供 gRPC 和 HTTP Gateway 接口。

## Docker Compose 部署

### 前置条件

- Docker
- Docker Compose

### 快速启动

```bash
docker-compose up -d
```

这会启动以下服务：
- `dataset-backend` — 主应用服务（gRPC: 3300, Gateway: 8080）
- `dataset-mysql` — MySQL 数据库（端口 3306）
- `dataset-redis` — Redis 缓存（端口 6379）

首次启动时会自动执行 `assets/db_table/dataset.sql` 初始化数据库表。

### 配置文件

配置文件位于 `configs/default.yaml`，启动后可修改该文件并重启服务：

```bash
# 修改配置
vim configs/default.yaml

# 重启服务
docker-compose restart dataset-backend
```

主要配置项：
- `components.sqlx.dataset.Source` — MySQL 连接串
- `components.redis.dataset.Address` — Redis 地址
- `services.grpc.dataset.Bind` — gRPC 监听端口
- `services.grpc-gateway.Bind` — HTTP Gateway 监听端口
- `dataset` — 业务配置（可选，未配置时使用代码默认值）

### 查看日志

```bash
# 查看所有服务日志
docker-compose logs -f

# 仅查看应用日志
docker-compose logs -f dataset-backend
```

### 停止服务

```bash
docker-compose down
```

如需同时删除数据卷：

```bash
docker-compose down -v
```

### 仅构建镜像

```bash
docker-compose build
```

## 本地开发

```bash
go run ./main.go -c configs/default.yaml
```

本地开发时需确保 MySQL 和 Redis 已启动，并修改 `configs/default.yaml` 中的连接地址为 `localhost`。

## 端口说明

| 服务 | 端口 | 说明 |
|------|------|------|
| gRPC | 3300 | gRPC 服务端口 |
| Gateway | 8080 | HTTP Gateway 端口 |
