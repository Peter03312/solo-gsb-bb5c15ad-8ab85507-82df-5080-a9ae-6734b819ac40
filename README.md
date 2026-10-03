# 匿名用户实验分流服务（experiment-traffic-split）

基于 **Python + FastAPI + PostgreSQL** 的匿名用户 A/B 实验分流服务。

- 每个互斥层固定 **10000 个桶** `[0, 10000)`
- 实验配置 **互不重叠的半开桶区间** `[start, end)`、**用户属性键值全匹配条件**、**组权重（总和 = 10000）**
- 发布时**原子校验**桶占用；并发发布不会产生重叠
- 分流按**层内固定盐**做稳定哈希落桶，命中且条件合格的实验内再按权重稳定分组
- 返回**实验版本、组别、分配时规则快照**；无命中明确返回未分配
- 同用户同层的**有效分配持久保持**：改桶/权重/条件只影响尚未分配的用户
- 结束实验后不再分新用户，旧分配可查，用户可进入同层另一实验
- 曝光凭 `assignment_id + event_no` 登记：同号同内容重试**幂等**，异内容**冲突**，无效/已结束分配**拒绝**
- 并发分流与结束不会给同用户留下两个有效分配

## 快速启动（Docker Compose 一键）

```bash
docker compose up --build
```

- API 地址： **http://localhost:8000** （交互式文档 http://localhost:8000/docs ）
- PostgreSQL： **localhost:5432** （用户/密码 `exp`/`exp`，库名 `experiments`）
- 数据库 schema 由 `db/init.sql` 在首次启动时自动初始化（含 `btree_gist` 扩展、表、排他约束、部分唯一索引）

停止并清空数据：`docker compose down -v`

## 配置

| 环境变量 | 默认值 | 说明 |
|---|---|---|
| `DATABASE_URL` | `postgresql://exp:exp@localhost:5432/experiments` | PostgreSQL 连接串（Compose 中自动指向 `db` 服务） |

端口：`8000`（HTTP API）、`5432`（PostgreSQL，均可在 `docker-compose.yml` 中改映射）。

## 本地开发（不用 Docker）

```bash
pip install -r requirements.txt
psql -d experiments -f db/init.sql          # 初始化 schema
DATABASE_URL=postgresql://exp:exp@localhost:5432/experiments \
  uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## 核心规则

1. **落桶**：`bucket = sha256("bucket" | layer_salt | user_key) mod 10000`，层盐在层创建时固定（可自带，缺省随机生成），同一用户在同层永远落同一桶。
2. **命中**：层内**活跃**实验中，桶区间包含该桶的那个（区间互不重叠，至多一个）。
3. **合格**：实验条件中的所有键值必须与用户属性完全一致（用户属性可有多余键）。
4. **分组**：`sha256("group" | salt | experiment_id | user_key) mod 10000` 落入按组权重累加的区间，权重总和恰为 10000。
5. **持久**：命中即写入分配（含规则快照）；之后无论配置如何变更，同一用户永远拿到同一条有效分配。未命中/不合格不产生分配。
6. **结束**：实验结束后不再分新用户；其全部分配转为非有效（仍可查询）；用户之后可进入同层其他实验。

## API 一览

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/health` | 健康检查 |
| POST | `/layers` | 创建互斥层（`key`，可选 `salt`） |
| GET | `/layers/{key}` | 查询层 |
| GET | `/layers/{key}/experiments` | 列出层内实验 |
| GET | `/layers/{key}/users/{user_key}/assignment` | 查询用户当前有效分配（无则 `assigned:false`） |
| POST | `/layers/{key}/experiments` | 创建实验（草稿）：半开区间 + 条件 + 组权重 |
| GET | `/experiments/{id}` | 查询实验 |
| PUT | `/experiments/{id}` | 修改草稿/活跃实验的区间、条件、权重（版本 +1，只影响未分配用户） |
| POST | `/experiments/{id}/publish` | 发布：原子校验桶占用后转活跃（幂等） |
| POST | `/experiments/{id}/end` | 结束：停止新分配，旧分配转非有效（幂等） |
| POST | `/assign` | 分流：输入层、用户键、属性，返回版本/组别/快照或未分配 |
| GET | `/assignments/{id}` | 查询分配（含已结束的旧分配） |
| POST | `/exposures` | 登记曝光：`assignment_id + event_no + content` |
| GET | `/assignments/{id}/exposures` | 查询某分配的曝光列表 |

## 请求示例（curl）

> 以下响应为真实运行输出，字段有删减。

**1. 创建层**

```bash
curl -X POST http://localhost:8000/layers \
  -H 'Content-Type: application/json' \
  -d '{"key": "checkout"}'
```
```json
{ "id": 15, "key": "checkout", "salt": "38ffd18c...", "created_at": "2026-10-03T06:56:07Z" }
```

**2. 创建实验（草稿）**：半开区间 `[0, 5000)`，条件 `country=CN` 全匹配，两组各 5000 权重

```bash
curl -X POST http://localhost:8000/layers/checkout/experiments \
  -H 'Content-Type: application/json' \
  -d '{
        "key": "button-color",
        "bucket_start": 0, "bucket_end": 5000,
        "conditions": {"country": "CN"},
        "groups": [
          {"name": "control",   "weight": 5000},
          {"name": "treatment", "weight": 5000}
        ]
      }'
```
```json
{ "id": 29, "key": "button-color", "version": 1, "status": "draft",
  "bucket_start": 0, "bucket_end": 5000, "interval": "[0, 5000)", ... }
```

**3. 发布（原子校验桶占用）**

```bash
curl -X POST http://localhost:8000/experiments/29/publish
```
```json
{ "id": 29, "status": "active", "version": 1, ... }
```
若与同层活跃实验区间重叠 → `409 {"detail": "bucket range [..) overlaps active experiment ..."}`。

**4. 分流（命中）**

```bash
curl -X POST http://localhost:8000/assign \
  -H 'Content-Type: application/json' \
  -d '{"layer_key": "checkout", "user_key": "anon-0003",
       "attributes": {"country": "CN", "lang": "zh"}}'
```
```json
{
  "assigned": true,
  "assignment_id": 12,
  "bucket": 424,
  "experiment_id": 29,
  "experiment_key": "button-color",
  "experiment_version": 1,
  "group": "control",
  "snapshot": {
    "experiment_version": 1,
    "bucket_start": 0, "bucket_end": 5000, "interval": "[0, 5000)",
    "conditions": {"country": "CN"},
    "groups": [{"name": "control", "weight": 5000}, {"name": "treatment", "weight": 5000}],
    "user_bucket": 424, "group": "control", "salt": "38ffd18c...", "layer_key": "checkout"
  },
  "assigned_at": "2026-10-03T06:56:07Z"
}
```
同一用户再次调用返回**同一条分配**（同一 `assignment_id` 与快照）。

**5. 分流（未命中，明确返回未分配）**

```bash
curl -X POST http://localhost:8000/assign \
  -H 'Content-Type: application/json' \
  -d '{"layer_key": "checkout", "user_key": "anon-9999", "attributes": {"country": "US"}}'
```
```json
{ "assigned": false, "reason": "conditions_not_met", "bucket": 2214,
  "candidate_experiment_id": 29, "candidate_experiment_key": "button-color" }
```
`reason` 取值：`no_experiment_covers_bucket`（无活跃实验覆盖该桶）/ `conditions_not_met`（条件不满足）。

**6. 登记曝光（幂等）**

```bash
curl -X POST http://localhost:8000/exposures \
  -H 'Content-Type: application/json' \
  -d '{"assignment_id": 12, "event_no": 1, "content": {"page": "home"}}'
```
- 首次：`201 {"exposure_id": 7, "deduplicated": false, ...}`
- 同号同内容重试：`200 {"exposure_id": 7, "deduplicated": true, ...}`（幂等，返回原记录）
- 同号异内容：`409 {"detail": "event_no 1 already registered with different content ..."}`
- 未知分配：`404`；已结束分配：`409`

**7. 结束实验**

```bash
curl -X POST http://localhost:8000/experiments/29/end
```
```json
{ "id": 29, "status": "ended", "already_ended": false, "deactivated_assignments": 1 }
```
之后：该实验不再分新用户；`GET /assignments/12` 仍可查（`effective: false, experiment_status: "ended"`）；对 `assignment_id=12` 的曝光返回 `409`；同用户可在同层新实验中重新分配。

## 并发与一致性设计

全部保证由单个 PostgreSQL 库内的锁 + 约束实现：

- **并发发布不产生桶重叠**：发布/修改活跃实验时先对层行 `SELECT ... FOR UPDATE` 串行化，再校验区间占用；另有**部分排他约束**兜底：
  `EXCLUDE USING gist (layer_id WITH =, int4range(start,end,'[)') WITH &&) WHERE status='active'`。
- **同用户同层至多一个有效分配**：部分唯一索引 `UNIQUE (layer_id, user_key) WHERE effective`；分流插入冲突时经保存点回滚后返回竞态胜出的分配。
- **分流与结束交错**：分流在插入前对候选实验行 `SELECT ... FOR UPDATE`，与结束操作（同一行锁 + 批量置 `effective=false`）串行化，因此不存在“结束后仍有效的分配”，也不会给同用户留下两个有效分配。
- **曝光幂等**：`UNIQUE (assignment_id, event_no)` + `ON CONFLICT DO NOTHING`；冲突时比较 `content IS NOT DISTINCT FROM`，一致则幂等返回，不一致报 409。

## 运行端到端测试

服务启动后：

```bash
python3 tests/test_e2e.py        # 或 BASE_URL=http://host:port python3 tests/test_e2e.py
```

覆盖：配置校验、并发发布唯一生效、条件匹配、分配持久、配置变更隔离、并发分流同号、曝光幂等/冲突/拒绝、结束语义、分流-结束竞态等 40 项检查。

## 项目结构

```
├── app/
│   ├── main.py       # FastAPI 路由
│   ├── services.py   # 核心业务逻辑（事务与并发控制）
│   ├── schemas.py    # 请求模型与校验（区间/权重和=10000）
│   ├── hashing.py    # 稳定哈希：落桶与分组
│   ├── db.py         # asyncpg 连接池（JSONB 编解码）
│   └── config.py     # 环境变量配置
├── db/init.sql       # schema：表、排他约束、部分唯一索引
├── tests/test_e2e.py # 端到端测试（含并发场景）
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```
