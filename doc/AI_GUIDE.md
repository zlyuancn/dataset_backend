# Dataset Backend - AI 集成指南

## 项目概述

**dataset_backend** 是一个高性能的数据集处理后端服务，采用 Go 语言开发，提供 gRPC 和 HTTP Gateway 双接口。

### 核心功能

1. **数据集管理**：创建、更新、删除数据集
2. **数据处理**：从远程 URL 下载数据，按分隔符切分，支持断点续传
3. **数据查询**：通过序列号快速查询已处理的数据值
4. **状态管理**：支持运行、停止、恢复等操作
5. **Chunk 存储**：数据分块存储，支持压缩（LZ4）

### 技术架构

- **接口层**：gRPC + GRPC Gateway（HTTP/JSON）
- **存储层**：
  - MySQL：持久化数据集元数据、操作历史、系统日志
  - Redis：缓存、分布式锁、处理状态、Chunk 元数据
- **处理层**：
  - 数据源：支持 HTTP/HTTPS 远程文件下载
  - 分隔器：按指定分隔符切分数据流
  - 值过滤器：支持修剪、前缀/后缀过滤
  - Chunk 存储：支持压缩和断点续传

## 数据模型

### 数据集状态机

```
Created (0) → Running (2) → Finished (3)
    ↓            ↓
    ↓        Stopping (4) → Stopped (5)
    ↓            ↓
Deleting (7) → ChunkDeleted (8)
```

### 核心数据结构

**DatasetExtend**（数据集扩展配置）：
- `DataProcess`：数据源配置（URL、Headers、代理等）
- `ChunkProcess`：Chunk 处理配置（存储类型、压缩类型）
- `ValueProcess`：值处理配置（分隔符、修剪规则、过滤规则）

## API 接口

### 端口说明

| 协议 | 端口 | 说明 |
|------|------|------|
| gRPC | 3300 | gRPC 服务 |
| HTTP | 8080 | HTTP Gateway |

### 管理接口

#### 1. AdminAddDataset - 创建数据集

**路径**：`POST /Dataset/AdminAddDataset`

**请求**：
```json
{
  "datasetName": "数据集名称",
  "remark": "备注信息",
  "datasetExtend": {
    "dataProcess": {
      "dataSource": 1,
      "uriFile": {
        "uri": "http://example.com/data.txt",
        "headers": [{"k": "Authorization", "v": "Bearer xxx"}],
        "method": "GET"
      },
      "trimUtf8Bom": true,
      "rateLimit": 0
    },
    "chunkProcess": {
      "storeType": 1,
      "compressType": 1
    },
    "valueProcess": {
      "delim": "\n",
      "trimSpace": true,
      "trimPrefix": ["\""],
      "trimSuffix": ["\""],
      "filterSubString": [],
      "filterPrefix": ["#"],
      "filterSuffix": []
    }
  },
  "op": {
    "opSource": "ai-assistant",
    "opUserid": "ai-001",
    "opUserName": "AI Assistant",
    "opRemark": "AI创建的数据集"
  },
  "startProcessNow": true
}
```

**响应**：
```json
{
  "datasetId": 12345
}
```

#### 2. AdminUpdateDataset - 更新数据集

**路径**：`POST /Dataset/AdminUpdateDataset`

**说明**：只有在 `Created` 状态才能更新 `datasetExtend`

**请求**：
```json
{
  "datasetId": 12345,
  "datasetName": "新名称",
  "remark": "新备注",
  "datasetExtend": { "...": "仅Created状态可更新" },
  "op": { "...": "操作信息" }
}
```

#### 3. AdminDelDataset - 删除数据集

**路径**：`POST /Dataset/AdminDelDataset`

**说明**：只有在 `Created`、`Finished`、`Stopped` 状态可删除

**请求**：
```json
{
  "datasetId": 12345,
  "op": { "...": "操作信息" }
}
```

#### 4. AdminRunProcessDataset - 启动处理

**路径**：`POST /Dataset/AdminRunProcessDataset`

**说明**：`Created` 或 `Stopped` 状态可启动

**请求**：
```json
{
  "datasetId": 12345,
  "op": { "...": "操作信息" }
}
```

#### 5. AdminStopProcessDataset - 停止处理

**路径**：`POST /Dataset/AdminStopProcessDataset`

**说明**：只有 `Running` 状态可停止

**请求**：
```json
{
  "datasetId": 12345,
  "op": { "...": "操作信息" }
}
```

### 查询接口

#### 6. SearchDatasetName - 搜索数据集名称

**路径**：`POST /Dataset/SearchDatasetName`

**请求**：
```json
{
  "pageSize": 10,
  "datasetName": "测试",
  "datasetId": 0
}
```

**响应**：
```json
{
  "lines": [
    {"datasetId": 12345, "datasetName": "测试数据集1"},
    {"datasetId": 12346, "datasetName": "测试数据集2"}
  ]
}
```

#### 7. QueryDatasetList - 查询数据集列表

**路径**：`POST /Dataset/QueryDatasetList`

**请求**：
```json
{
  "page": 1,
  "pageSize": 20,
  "datasetId": 0,
  "status": [0, 2, 3],
  "startTime": 1700000000,
  "endTime": 1710000000,
  "opUser": "admin"
}
```

**响应**：
```json
{
  "total": 100,
  "lines": [
    {
      "datasetId": 12345,
      "datasetName": "测试数据集",
      "remark": "备注",
      "delim": "\n",
      "valueTotal": 10000,
      "createTime": 1700000000,
      "processed_time": 1700000100,
      "op": {
        "opSource": "ai-assistant",
        "opUserid": "ai-001",
        "opUserName": "AI Assistant",
        "opRemark": "AI创建",
        "opTime": 1700000000
      },
      "status": 3,
      "statusInfo": "finished"
    }
  ]
}
```

#### 8. QueryDatasetInfo - 查询数据集详情

**路径**：`POST /Dataset/QueryDatasetInfo`

**请求**：
```json
{
  "datasetId": 12345
}
```

**响应**：
```json
{
  "line": {
    "datasetId": 12345,
    "datasetName": "测试数据集",
    "remark": "备注",
    "datasetExtend": { "...": "完整配置" },
    "valueTotal": 10000,
    "createTime": 1700000000,
    "processed_time": 1700000100,
    "op": { "...": "操作信息" },
    "status": 3,
    "statusInfo": "finished",
    "activate_time": 1700000000
  }
}
```

#### 9. QueryDatasetStatusInfo - 查询处理状态

**路径**：`POST /Dataset/QueryDatasetStatusInfo`

**说明**：用于轮询获取处理中数据集的实时进度

**请求**：
```json
{
  "datasetIds": [12345, 12346]
}
```

**响应**：
```json
{
  "datasetStateInfos": [
    {
      "datasetId": 12345,
      "chunkTotal": 100,
      "chunkProcessedCount": 50,
      "valueTotal": 50000,
      "status": 2,
      "statusInfo": "running",
      "op": { "...": "操作信息" },
      "processed_time": 0
    }
  ]
}
```

#### 10. QueryDatasetData - 查询数据

**路径**：`POST /Dataset/QueryDatasetData`

**说明**：通过 datasetId 和 valueSn 查询具体数据值

**请求**：
```json
{
  "datasetId": 12345,
  "valueSn": 100
}
```

**响应**：
```json
{
  "datasetId": 12345,
  "chunkSn": 1,
  "valueSn": 100,
  "value": "这是第100条数据的内容"
}
```

#### 11. QuerySyslog - 查询系统日志

**路径**：`POST /Dataset/QuerySyslog`

**请求**：
```json
{
  "datasetId": 12345,
  "nextCursor": 0,
  "pageSize": 20,
  "startTime": 1700000000000,
  "endTime": 1710000000000,
  "logType": [0, 1, 2],
  "asc": true
}
```

**响应**：
```json
{
  "nextCursor": 1700000100000,
  "pageSize": 20,
  "lines": [
    {
      "remark": "处理完成",
      "extend": "{}",
      "logType": 0,
      "createTime": 1700000100000
    }
  ]
}
```

## 接入流程

### 1. 服务连接

**gRPC 连接**：
```go
conn, _ := grpc.Dial("localhost:3300", grpc.WithInsecure())
client := pb.NewDatasetServiceClient(conn)
```

**HTTP 连接**：
```bash
curl -X POST http://localhost:8080/Dataset/AdminAddDataset \
  -H "Content-Type: application/json" \
  -d '{...}'
```

### 2. 典型工作流程

#### 创建并处理数据集

```
1. 调用 AdminAddDataset (startProcessNow=true)
   ↓
2. 轮询 QueryDatasetStatusInfo 获取进度
   ↓
3. 状态变为 Finished (3) 后，可查询数据
   ↓
4. 调用 QueryDatasetData 获取具体数据
```

#### 分步处理

```
1. 调用 AdminAddDataset (startProcessNow=false)
   ↓
2. 调用 AdminRunProcessDataset 启动处理
   ↓
3. 需要时调用 AdminStopProcessDataset 停止
   ↓
4. 再次调用 AdminRunProcessDataset 恢复
```

### 3. 配置说明

**必要配置**（`configs/default.yaml`）：

```yaml
components:
  sqlx:
    dataset:
      Driver: mysql
      Source: 'root:password@tcp(mysql:3306)/dataset?charset=utf8mb4&parseTime=True&loc=Local'
  redis:
    dataset:
      Address: redis:6379
      Password: ""

services:
  grpc:
    dataset:
      Bind: :3300
  grpc-gateway:
    Bind: :8080
```

**业务配置**（可选，有默认值）：

```yaml
dataset:
  ChunkSizeLimit: 4194304        # Chunk 大小限制 4MB
  ValueMaxScanSizeLimit: 1048576 # 单值最大长度 1MB
  ChunkStoreThreadCount: 5       # 持久化线程数
```

### 4. 数据库初始化

首次部署需执行 `assets/db_table/dataset.sql` 创建以下表：

- `dataset_list` - 数据集主表
- `dataset_history` - 操作历史表
- `dataset_log` - 系统日志表

## 使用示例

### 完整示例：AI 创建并查询数据集

```python
import requests
import time

BASE_URL = "http://localhost:8080"

# 1. 创建数据集
create_resp = requests.post(f"{BASE_URL}/Dataset/AdminAddDataset", json={
    "datasetName": "AI测试数据集",
    "remark": "AI自动创建",
    "datasetExtend": {
        "dataProcess": {
            "dataSource": 1,
            "uriFile": {
                "uri": "https://example.com/data.txt",
                "method": "GET"
            },
            "trimUtf8Bom": True
        },
        "chunkProcess": {
            "storeType": 1,
            "compressType": 1  # LZ4 压缩
        },
        "valueProcess": {
            "delim": "\n",
            "trimSpace": True
        }
    },
    "op": {
        "opSource": "ai-assistant",
        "opUserid": "ai-001",
        "opUserName": "AI Assistant",
        "opRemark": "AI创建用于测试的数据集"
    },
    "startProcessNow": True
})

dataset_id = create_resp.json()["datasetId"]
print(f"创建成功，datasetId: {dataset_id}")

# 2. 轮询进度
while True:
    status_resp = requests.post(f"{BASE_URL}/Dataset/QueryDatasetStatusInfo", json={
        "datasetIds": [dataset_id]
    })
    
    status_info = status_resp.json()["datasetStateInfos"][0]
    status = status_info["status"]
    
    if status == 2:  # Running
        progress = status_info["chunkProcessedCount"] / status_info["chunkTotal"] * 100
        print(f"处理中: {progress:.1f}%")
        time.sleep(5)
    elif status == 3:  # Finished
        print(f"处理完成，共 {status_info['valueTotal']} 条数据")
        break
    else:
        print(f"状态异常: {status}")
        break

# 3. 查询数据
data_resp = requests.post(f"{BASE_URL}/Dataset/QueryDatasetData", json={
    "datasetId": dataset_id,
    "valueSn": 0  # 第一条数据
})

print(f"第一条数据: {data_resp.json()['value']}")
```

## 注意事项

1. **valueSn 从 0 开始**：查询数据时，valueSn 从 0 开始递增
2. **状态检查**：只有 `Finished` 状态的数据集才能查询数据
3. **操作锁**：管理接口有分布式锁保护，同一数据集的操作会串行化
4. **断点续传**：处理过程中断后会自动从断点恢复
5. **预加载**：查询数据时会自动预加载下一个 chunk，提高连续查询性能
6. **日志时间**：系统日志时间单位为毫秒，其他时间单位为秒

## 错误处理

常见错误码：

- `Dataset status is running` - 数据集正在处理中，无法执行该操作
- `Dataset status is unknown` - 数据集状态异常
- `dataset not finished` - 数据集未完成处理，无法查询数据
- `Failed to locate the index of value` - valueSn 超出范围

## 部署方式

### Docker Compose

```bash
docker-compose up -d
```

### 本地开发

```bash
go run ./main.go -c configs/default.yaml
```

## 总结

这是一个专为高效处理大规模文本数据集而设计的后端服务。作为 AI 集成方，你只需要：

1. 通过 HTTP/gRPC 接口创建数据集
2. 轮询获取处理进度
3. 处理完成后通过 valueSn 查询数据

所有复杂的处理逻辑（下载、切分、过滤、压缩、存储、断点续传）都由后端自动完成。
