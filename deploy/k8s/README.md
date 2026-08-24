# Meteron Kubernetes 部署

生产级高可用部署清单，采用 **Kustomize base + overlay** 架构，全组件资源可配置。

## 目录结构

```
deploy/k8s/
├── base/                           # 默认配置（中等规格）
│   ├── kustomization.yaml          # Kustomize 入口
│   ├── namespace.yaml
│   ├── secrets.yaml                # 密钥（生产必须替换）
│   ├── postgres.yaml               # PG HA + ConfigMap 可调参数
│   ├── redis.yaml                  # Redis HA + ConfigMap 可调参数
│   ├── clickhouse.yaml             # CH HA + ConfigMap 可调参数
│   ├── kafka.yaml                  # Kafka HA + ConfigMap 可调参数
│   └── meteron.yaml                # Meteron 多副本 + HPA + ConfigMap
├── overlays/
│   ├── dev/                        # 开发环境（小规格）
│   │   ├── kustomization.yaml
│   │   └── patches/
│   │       ├── postgres-patch.yaml
│   │       ├── redis-patch.yaml
│   │       ├── clickhouse-patch.yaml
│   │       ├── kafka-patch.yaml
│   │       └── meteron-patch.yaml
│   └── prod/                       # 生产环境（高配）
│       ├── kustomization.yaml
│       └── patches/
│           ├── postgres-patch.yaml
│           ├── redis-patch.yaml
│           ├── clickhouse-patch.yaml
│           ├── kafka-patch.yaml
│           └── meteron-patch.yaml
├── apply.sh                        # 部署辅助脚本
└── README.md
```

## 快速部署

```bash
# 方式一：使用辅助脚本
./deploy/k8s/apply.sh dev      # 部署 dev 配置
./deploy/k8s/apply.sh prod     # 部署 prod 配置
./deploy/k8s/apply.sh dry dev  # 预览 dev 渲染结果（不部署）

# 方式二：直接 kubectl
kubectl apply -k deploy/k8s/overlays/dev    # dev
kubectl apply -k deploy/k8s/overlays/prod   # prod
kubectl apply -k deploy/k8s/base            # base 默认
```

## 高可用拓扑

| 组件 | 副本数 | HA 方式 | PDB |
|------|--------|---------|-----|
| PostgreSQL | 3 | 流式复制（1 主 + 2 热备） | minAvailable: 2 |
| Redis | 3 | 主从复制（1 主 + 2 从） | minAvailable: 2 |
| ClickHouse | 3 | ReplicatedMergeTree + 内嵌 Keeper | minAvailable: 2 |
| Kafka | 3 | KRaft 3 节点 quorum | minAvailable: 2 |
| Meteron | 2~20 | 无状态 + HPA 自动扩缩 | minAvailable: 1 |

## 三套资源规格对比

### CPU / 内存

| 组件 | dev | base（默认） | prod |
|------|-----|-------------|------|
| **PostgreSQL** | 0.1~1 core / 256Mi~1Gi | 0.25~2 core / 512Mi~4Gi | 1~4 core / 2Gi~8Gi |
| **Redis** | 0.05~0.5 core / 64Mi~512Mi | 0.1~1 core / 256Mi~2Gi | 0.25~2 core / 1Gi~4Gi |
| **ClickHouse** | 0.25~2 core / 512Mi~4Gi | 0.5~4 core / 2Gi~16Gi | 2~8 core / 8Gi~32Gi |
| **Kafka** | 0.25~2 core / 512Mi~4Gi | 0.5~4 core / 1Gi~8Gi | 1~6 core / 4Gi~16Gi |
| **Meteron** | 0.1~1 core / 128Mi~1Gi | 0.25~2 core / 256Mi~4Gi | 1~4 core / 1Gi~8Gi |

### 磁盘（PVC）

| 组件 | dev | base | prod |
|------|-----|------|------|
| PostgreSQL | 10Gi | 20Gi | 100Gi |
| Redis | 2Gi | 5Gi | 20Gi |
| ClickHouse 数据 | 20Gi | 50Gi | 500Gi |
| ClickHouse Keeper | 5Gi | 5Gi | 10Gi |
| Kafka | 15Gi | 30Gi | 200Gi |

### 连接数 / 并发

| 参数 | dev | base | prod |
|------|-----|------|------|
| PG max_connections | 100 | 200 | 500 |
| PG shared_buffers | 128MB | 512MB | 2GB |
| PG max_wal_senders | 5 | 10 | 15 |
| Redis maxmemory | 256mb | 1gb | 2gb |
| CH max_concurrent_queries | 200 | 200 | 200 |
| CH max_memory_usage | 10GB | 10GB | 10GB |
| CH max_threads | 8 | 8 | 8 |
| Kafka num_io_threads | 4 | 8 | 16 |
| Kafka num_network_threads | 2 | 3 | 8 |
| Kafka log_retention_hours | 72 | 168 | 720 |
| Meteron GOMEMLIMIT | 1GiB | 4GiB | 7GiB |
| Meteron GOMAXPROCS | 2 | 4 | 4 |
| Meteron customer RPM | 60 | 60 | 200 |
| Meteron customer TPM | 100K | 100K | 500K |
| Meteron HPA min/max | 1/3 | 2/10 | 3/20 |

## 全部可配置参数

### PostgreSQL（ConfigMap: postgres-config）

| 参数 | 说明 | base 默认值 |
|------|------|-------------|
| PG_MAX_CONNECTIONS | 最大连接数 | 200 |
| PG_SHARED_BUFFERS | 共享缓冲区 | 512MB |
| PG_EFFECTIVE_CACHE_SIZE | 有效缓存大小 | 2GB |
| PG_WORK_MEM | 单查询工作内存 | 16MB |
| PG_MAINTENANCE_WORK_MEM | 维护操作内存 | 256MB |
| PG_WAL_BUFFERS | WAL 缓冲区 | 16MB |
| PG_CHECKPOINT_COMPLETION_TARGET | 检查点完成目标 | 0.9 |
| PG_RANDOM_PAGE_COST | 随机页成本 | 1.1 |
| PG_MAX_WAL_SENDERS | 最大 WAL 发送者 | 10 |
| PG_MAX_REPLICATION_SLOTS | 最大复制槽 | 10 |
| PG_WAL_KEEP_SIZE | WAL 保留大小 | 1024 |
| PG_STATEMENT_TIMEOUT | 语句超时 | 0（禁用） |
| PG_IDLE_IN_TRANSACTION_SESSION_TIMEOUT | 空闲事务超时 | 300s |
| PG_LOG_MIN_DURATION_STATEMENT | 慢查询记录阈值 | 1000ms |

### Redis（ConfigMap: redis-config）

| 参数 | 说明 | base 默认值 |
|------|------|-------------|
| REDIS_MAXMEMORY | 最大内存 | 1gb |
| REDIS_MAXMEMORY_POLICY | 淘汰策略 | allkeys-lru |
| REDIS_MAXMEMORY_SAMPLES | 采样数 | 5 |
| REDIS_TIMEOUT | 客户端超时 | 300 |
| REDIS_TCP_KEEPALIVE | TCP 保活 | 60 |
| REDIS_TCP_BACKLOG | TCP backlog | 511 |
| REDIS_DATABASES | 逻辑库数 | 16 |
| REDIS_APPENDONLY | AOF 持久化 | yes |
| REDIS_APPENDFSYNC | AOF 同步策略 | everysec |
| REDIS_SAVE | RDB 快照策略 | 60 1000 |
| REDIS_REPL_BACKLOG_SIZE | 复制 backlog | 64mb |
| REDIS_REPL_TIMEOUT | 复制超时 | 300 |
| REDIS_REPL_DISKLESS_SYNC | 无盘同步 | yes |
| REDIS_CLIENT_OUTPUT_BUFFER_LIMIT | 客户端输出缓冲限制 | replica 256mb 64mb 60 |

### ClickHouse（XML 配置注入）

| 参数 | 说明 | base 默认值 |
|------|------|-------------|
| max_connections | 最大连接数 | 4096 |
| max_concurrent_queries | 最大并发查询 | 200 |
| max_memory_usage | 单查询最大内存 | 10GB |
| max_memory_usage_for_user | 单用户最大内存 | 5GB |
| max_threads | 最大线程数 | 8 |
| max_block_size | 最大块大小 | 65536 |
| max_insert_block_size | 插入块大小 | 1048576 |
| max_query_size | 最大查询大小 | 262144 |
| uncompressed_cache_size | 未压缩缓存 | 8GB |
| mark_cache_size | 标记缓存 | 5GB |
| max_open_files | 最大打开文件数 | 262144 |
| max_replicated_merges_in_queue | 合并队列 | 16 |
| max_replicated_mutations_in_queue | 变更队列 | 8 |

### Kafka（ConfigMap: kafka-config）

| 参数 | 说明 | base 默认值 |
|------|------|-------------|
| KAFKA_NUM_PARTITIONS | 默认分区数 | 6 |
| KAFKA_DEFAULT_REPLICATION_FACTOR | 默认副本因子 | 3 |
| KAFKA_MIN_INSYNC_REPLICAS | 最小同步副本 | 2 |
| KAFKA_NUM_NETWORK_THREADS | 网络线程数 | 3 |
| KAFKA_NUM_IO_THREADS | IO 线程数 | 8 |
| KAFKA_SOCKET_SEND_BUFFER_BYTES | 发送缓冲区 | 102400 |
| KAFKA_SOCKET_RECEIVE_BUFFER_BYTES | 接收缓冲区 | 102400 |
| KAFKA_SOCKET_REQUEST_MAX_BYTES | 单请求最大字节 | 104857600 |
| KAFKA_QUEUED_MAX_REQUESTS | 队列最大请求 | 500 |
| KAFKA_LOG_RETENTION_HOURS | 日志保留小时 | 168 |
| KAFKA_LOG_RETENTION_BYTES | 日志保留字节 | -1（无限） |
| KAFKA_LOG_SEGMENT_BYTES | 日志段大小 | 1GB |
| KAFKA_MESSAGE_MAX_BYTES | 消息最大字节 | 1048588 |
| KAFKA_REPLICA_FETCH_MAX_BYTES | 副本拉取最大字节 | 1048576 |
| KAFKA_REPLICA_LAG_TIME_MAX_MS | 副本延迟最大时间 | 30000 |
| KAFKA_REQUEST_TIMEOUT_MS | 请求超时 | 30000 |

### Meteron（ConfigMap: meteron-config）

| 参数 | 说明 | base 默认值 |
|------|------|-------------|
| GOMEMLIMIT | Go 内存软上限 | 4GiB |
| GOMAXPROCS | Go 最大 CPU | 4 |
| GOGC | GC 触发比率 | 100 |
| METERON_BILLING_INTERVAL | 自动出账间隔 | 24h |
| METERON_RECON_LIGHT_INTERVAL | 轻量对账间隔 | 1h |
| METERON_RECON_DEEP_INTERVAL | 深度对账间隔 | 24h |
| METERON_RATE_LIMIT_CUSTOMER_RPM | 客户 RPM | 60 |
| METERON_RATE_LIMIT_CUSTOMER_TPM | 客户 TPM | 100000 |
| METERON_RATE_LIMIT_CUSTOMER_MAX_CONCURRENT | 客户最大并发 | 10 |
| METERON_RATE_LIMIT_PROVIDER_MAX_CONCURRENT | Provider 最大并发 | 20 |
| METERON_HEALTH_PROBE_INTERVAL | 健康探测间隔 | 60s |
| METERON_HEALTH_WINDOW | 健康统计窗口 | 5m |
| METERON_HEALTH_ERROR_RATE_THRESHOLD | 降级错误率阈值 | 0.5 |
| METERON_HEALTH_MIN_SAMPLES | 降级最小样本 | 10 |

## 自定义配置

### 方式一：创建自定义 overlay

```bash
mkdir -p deploy/k8s/overlays/custom/patches

# 创建 kustomization.yaml
cat > deploy/k8s/overlays/custom/kustomization.yaml <<EOF
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: meteron
resources:
  - ../../base
patchesStrategicMerge:
  - patches/postgres-patch.yaml
  - patches/redis-patch.yaml
  - patches/clickhouse-patch.yaml
  - patches/kafka-patch.yaml
  - patches/meteron-patch.yaml
EOF

# 复制 prod 补丁并按需修改
cp deploy/k8s/overlays/prod/patches/*.yaml deploy/k8s/overlays/custom/patches/

# 修改参数后部署
kubectl apply -k deploy/k8s/overlays/custom
```

### 方式二：运行时修改 ConfigMap

```bash
# 修改 PostgreSQL 连接数
kubectl -n meteron edit configmap postgres-config

# 修改 Kafka IO 线程
kubectl -n meteron patch configmap kafka-config \
  -p '{"data":{"KAFKA_NUM_IO_THREADS":"32"}}'

# 修改 Meteron 限流配置
kubectl -n meteron patch configmap meteron-config \
  -p '{"data":{"METERON_RATE_LIMIT_CUSTOMER_RPM":"500"}}'

# 修改后重启对应 Pod 使 ConfigMap 生效
kubectl -n meteron rollout restart statefulset postgres
kubectl -n meteron rollout restart statefulset kafka
kubectl -n meteron rollout restart deployment meteron
```

### 方式三：修改 PVC 存储

```bash
# 扩容 PostgreSQL PVC（StorageClass 需支持扩容）
kubectl -n meteron patch statefulset postgres \
  --type='json' \
  -p='[{"op":"replace","path":"/spec/volumeClaimTemplates/0/spec/resources/requests/storage","value":"200Gi"}]'
```

## 访问方式

三种 Service 同时部署，按需选用：

| Service 类型 | 名称 | 用途 | 端口 |
|---|---|---|---|
| ClusterIP | `meteron` | 集群内部 | 8080（UI）, 9080（API） |
| NodePort | `meteron-nodeport` | 节点端口 | 30080（UI）, 30090（API） |
| LoadBalancer | `meteron-loadbalancer` | 云负载均衡器 | 80（UI）, 9080（API） |

```bash
# 获取 NodePort
kubectl -n meteron get svc meteron-nodeport

# 获取 LoadBalancer IP
kubectl -n meteron get svc meteron-loadbalancer

# 本地端口映射
kubectl -n meteron port-forward svc/meteron 8080:8080 9080:9080
```

## 生产部署清单

1. **替换密钥**（secrets.yaml）：METERON_AES_KEY、POSTGRES_PASSWORD 等
2. **替换镜像**（meteron.yaml）：`image: meteron-meteron:latest` → 私有仓库地址
3. **确认存储**：StorageClass 支持 `volumeExpansion: true`
4. **资源调优**：根据实际负载调整 overlay 补丁
5. **Ingress**：按需添加 Ingress 资源暴露 LoadBalancer

## 清理

```bash
kubectl delete -k deploy/k8s/overlays/dev
# 或
kubectl delete namespace meteron
```
