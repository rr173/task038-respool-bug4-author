# task038-respool

这是一个纯 Go 的加权资源池租约库。调用方可以按权重申请带 TTL 的租约，主动归还或续期租约，并在容量不足时按优先级排队；过期租约可由显式回收或后台回收器释放。项目不依赖数据库、网络服务或第三方包。

## 标准命令

以下命令均在 `env/` 目录执行：

```bash
go build ./...
go test ./...
go vet ./...
go run . --smoke-test
```

`--smoke-test` 会执行申请/归还、超限申请、过期回收唤醒、严格 FIFO 和缩容取消等待者等检查，然后自行退出。

## Benzhi 容器

`build_benzhi_docker.sh` 使用固定的 `benzhi.Dockerfile` 构建评测镜像，参数依次是镜像名和平台，默认值为 `my-project` 与 `linux/amd64`：

```bash
bash build_benzhi_docker.sh respool-benzhi linux/amd64
docker run --rm -it respool-benzhi:latest
```

容器启动后进入 shell；构建阶段执行 `go build ./...`，不依赖外部业务服务。
