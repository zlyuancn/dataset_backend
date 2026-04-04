FROM golang:1.25-alpine as builder

WORKDIR /app

# 下载并安装依赖
COPY go.mod .
COPY go.sum .
RUN go mod download

# 复制源代码
COPY . .

# 编译应用
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o main .

# 使用 Ubuntu 作为最终的运行时基础镜像
FROM ubuntu:20.04

# 更新包索引并安装必要的证书
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /root/

# 从builder阶段复制编译好的可执行文件和默认配置文件
COPY --from=builder /app/main .
COPY --from=builder /app/configs ./configs

# 暴露端口（gRPC 3300, Gateway 8080）
EXPOSE 3300 8080

# 运行可执行文件
CMD ["./main", "-c", "/root/configs/default.yaml"]
