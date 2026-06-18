# go-cache
**声明**:基于[geektutu](https://github.com/geektutu/7days-golang)做出改变  
一个基于 Go 实现的分布式缓存小项目，支持：

- 本地缓存
- LRU 淘汰
- `singleflight` 防止缓存击穿
- 一致性哈希选择远程节点
- `HTTP + protobuf` 节点间通信
- 独立的服务端和客户端入口
- 基础健康检查与统计接口
- 动态新增和删除 key

项目现在的通信方式是：

- 传输层：HTTP
- 消息编码：protobuf



## 目录结构

```text
go-cache/
├─ cmd/
│  ├─ client/        # 客户端入口
│  └─ server/        # 服务端入口
├─ consistenthash/   # 一致性哈希
├─ lru/              # LRU 缓存实现
├─ proto/            # protobuf 定义与生成代码
├─ byteview.go       # 只读缓存值封装
├─ cache.go          # cache 封装
├─ group.go          # 核心缓存逻辑
├─ http_pool.go      # 节点通信与 HTTP 服务
├─ peer.go           # peer 抽象接口
└─ README.md
```

## 核心流程

### 1. 客户端发起请求

客户端通过下面的格式请求某个服务节点：

```text
GET /go-cache/{group}/{key}
```

例如：

```text
GET /go-cache/scores/123
```

### 2. 服务端接收请求

服务端收到请求后，会：

1. 根据 `group` 找到对应的缓存组
2. 先查本地缓存
3. 如果本地没有，使用一致性哈希选择应该负责该 key 的节点
4. 如果目标节点是自己，就本地加载数据
5. 如果目标节点是其他机器，就通过 HTTP 请求远端节点

### 3. 远程节点返回 protobuf 数据

节点间通信时，返回的不是普通字符串，而是 protobuf 序列化后的 `Response`：

```proto
message Response {
  bytes value = 1;
}
```

服务端会把缓存值编码为 protobuf 二进制：

```text
Content-Type: application/x-protobuf
```

客户端收到后再进行反序列化，拿到真正的值。

## protobuf 在这个项目中的作用

`proto/go-cache.proto` 定义了通信消息格式：

```proto
syntax = "proto3";

package proto;

option go_package = "go-cache/proto;proto";

message Request {
  string group = 1;
  string key = 2;
}

message Response {
  bytes value = 1;
}
```

当前项目里：

- `Request` 主要用于节点间抽象接口
- `Response` 用于 HTTP 响应体的 protobuf 编码

所以你可以把这个项目理解为：

- 外层是 HTTP 路由
- 内层的数据格式是 protobuf

## 本地数据源

当前服务端内置了一份模拟数据库：

```go
var db = map[string]string{
    "kk":  "630",
    "acd": "589",
    "123": "567",
}
```

这表示：

- key=`kk`，value=`630`
- key=`acd`，value=`589`
- key=`123`，value=`567`

注意：

- 你查的是 key，不是 value
- 比如想拿到 `567`，应该请求 key=`123`

现在服务端的数据源已经支持动态写入和删除

## 如何启动

### 环境要求

- Go 1.25.6 或兼容版本

安装依赖：

```bash
go mod tidy
```

### 启动单节点服务

```bash
go run ./cmd/server -addr localhost:8001 -peers localhost:8001
```

这时只有 `8001` 一个节点在运行。

### 启动三节点服务

分别打开三个终端，执行：

```bash
go run ./cmd/server -addr localhost:8001 -peers localhost:8001,localhost:8002,localhost:8003
```

```bash
go run ./cmd/server -addr localhost:8002 -peers localhost:8001,localhost:8002,localhost:8003
```

```bash
go run ./cmd/server -addr localhost:8003 -peers localhost:8001,localhost:8002,localhost:8003
```

说明：

- `-addr` 表示当前节点监听的地址
- `-peers` 表示整个集群有哪些节点
- `-peers` 不会自动帮你启动其他节点，它只是配置集群拓扑

所以如果你只启动了 `8001`，那你就只能访问 `8001`。

## 如何请求客户端

客户端入口在：

```bash
go run ./cmd/client -server localhost:8001 -group scores -key 123
```

参数说明：

- `-server`：目标服务端地址
- `-group`：缓存组名称，当前默认是 `scores`
- `-key`：要查询的 key
- `-timeout`：请求超时时间，默认 `2s`

例如：

```bash
go run ./cmd/client -server localhost:8001 -group scores -key kk
```

输出：

```text
630
```

再比如：

```bash
go run ./cmd/client -server localhost:8001 -group scores -key 123
```

输出：

```text
567
```

## 如何动态新增和删除 key

### 新增 key-value

接口：

```text
POST /api/cache
```

请求体示例：

```json
{
  "group": "scores",
  "key": "tom",
  "value": "999"
}
```

命令示例：

```bash
curl -X POST http://localhost:8001/api/cache \
  -H "Content-Type: application/json" \
  -d "{\"group\":\"scores\",\"key\":\"tom\",\"value\":\"999\"}"
```

返回示例：

```json
{
  "group": "scores",
  "key": "tom",
  "message": "ok",
  "value": "999"
}
```

新增后可以直接查询：

```bash
go run ./cmd/client -server localhost:8001 -group scores -key tom
```

输出：

```text
999
```

### 删除 key

接口：

```text
DELETE /api/cache/{group}/{key}
```

命令示例：

```bash
curl -X DELETE http://localhost:8001/api/cache/scores/tom
```

返回示例：

```json
{
  "group": "scores",
  "key": "tom",
  "message": "ok"
}
```

删除后再次查询会返回找不到：

```bash
go run ./cmd/client -server localhost:8001 -group scores -key tom
```

### 这两个接口在多节点下的行为

- 你可以把请求发到任意一个节点
- 服务端会根据一致性哈希判断这个 key 应该归哪个节点负责
- 如果目标节点不是当前节点 会自动转发到正确节点
- 所以新增和删除并不是只在当前端口生效

## 健康检查和统计接口

### 健康检查

```text
GET /healthz
```

例如：

```text
http://localhost:8001/healthz
```

返回示例：

```json
{
  "self": "http://localhost:8001",
  "status": "ok"
}
```

### 统计信息

```text
GET /stats
```

例如：

```text
http://localhost:8001/stats
```

返回示例：

```json
{
  "self": "http://localhost:8001",
  "groups": [
    {
      "name": "scores",
      "cache_hits": 1,
      "peer_loads": 0,
      "peer_load_errors": 0,
      "local_loads": 1,
      "local_load_errors": 0
    }
  ]
}
```

字段说明：

- `cache_hits`：命中本地缓存次数
- `peer_loads`：成功从远端节点获取数据次数
- `peer_load_errors`：从远端节点获取失败次数
- `local_loads`：本地回源加载成功次数
- `local_load_errors`：本地回源加载失败次数

## 常见问题

### 1. 为什么报 `key not found`

如果你看到类似错误：

```text
request failed: 404 Not Found, body=key not found: 567
```

说明你请求的 key 不存在。

当前可用 key 是：

- `kk`
- `acd`
- `123`

注意不要把 value 当成 key。

例如：

- `123` 是 key
- `567` 是 value

所以想拿到 `567`，应该查：

```bash
go run ./cmd/client -server localhost:8001 -group scores -key 123
```

### 2. 为什么我只能访问 8001

因为你大概率只启动了 `8001` 这个服务。

`-peers` 只表示“集群里有哪些节点”，并不会自动启动 `8002` 和 `8003`。

如果想访问 `8002`、`8003`，需要分别把它们也运行起来。

### 3. 为什么不是 gRPC

项目里已经生成了 gRPC 相关代码，但当前实际使用的是：

- HTTP 负责传输
- protobuf 负责编码

所以现在是 `HTTP + protobuf`，不是完整的 `gRPC + protobuf`。

### 4. 新增和删除为什么可以发给任意节点

因为服务端收到写请求后，也会根据一致性哈希判断 key 属于哪个节点。

如果目标节点不是当前节点，就会把写入或删除请求转发过去。

## 已做的优化

当前版本已经补过这些优化：

- peer 地址统一规范化，避免一致性哈希与 getter 映射不一致
- HTTP 请求增加超时控制
- `ErrKeyNotFound` 单独分类，避免把业务错误误报成 500
- 增加 `/healthz` 和 `/stats`
- 增加动态写入和删除接口
- 增加基础测试
- 使用 `RWMutex` 优化读多写少场景
- 防止重复创建同名 group

## 测试

运行测试：

```bash
go test ./...
```

## 后续可继续扩展的方向

- 增加浏览器友好的查询接口，例如 `/api?group=scores&key=123`
- 增加一键启动 3 个节点的脚本
- 补更多集成测试
- 将当前 `HTTP + protobuf` 升级为真正的 `gRPC + protobuf`
- 将当前内存数据源替换为真实数据源
