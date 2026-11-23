# 事件会话发布服务（Event Session Publisher）

FastAPI + SQLite 实现的事件流会话化与 watermark 发布服务，包含一个可交互的网页演示。

## 功能

- **分批事件导入**：事件含 `user_id` / `event_id` / UTC `event_time`；按 `(event_time, event_id)` 排序后，相邻事件间隔 **严格大于** 每用户空闲间隔（idle gap）时分会话，同刻事件按 `event_id` 排序。
- **无冲突会话键**：会话键为会话内**有序事件编号 JSON 数组**的 SHA-256 前 16 位。即使编号本身含逗号，单事件会话 `["a,b"]` 与双事件会话 `["a","b"]` 也各有独立键；事件明细以 JSON 数组存储，完整保留原顺序和原始编号，不再用逗号拼接。
- **Watermark 发布**：watermark 只能前进（后退/相等分别返回 409 / 空操作）；仅发布 `end_time < watermark − idle_gap` 的会话，其余保持 `pending`。
- **迟到事件修订**：迟到事件若改变已发布会话（落入其中或桥接两个会话），生成新修订，返回 `retract`（撤回）与 `add`（新增）差异；旧修订快照永久保留、可按修订号查询。
- **旧库直接打开与一次性键迁移**：升级前的 SQLite 库无需导数据即可启动（仅追加 `id_encoding` 列）；历史修订保留原修订号和原会话键。旧明细按逗号拆出的编号数与记录的事件数不符时，编号列表返回空并标 `ids_need_review=true`（待核对）。升级后首次重算遇到这类**已发布的旧键**时，每个旧键只撤回一次（撤回差异同样带待核对标记），再补发按旧规则会共用该键、当前仍满足发布条件的会话（使用新键）；其余未变会话沿用原键，不重复发布。
- **幂等导入**：同一 `(user_id, event_id)` 重复导入直接忽略，不产生修订；同 event_id 不同时间返回 409；批内重复返回 422。
- **失败保留旧修订与旧水位**：每个写请求在单事务内完成，任何异常整体回滚（含键迁移、发布差异与 watermark 推进），上一修订和原 watermark 仍为最新可见版本，可用完全相同的请求重试。
- **并发安全**：同用户写操作由进程内锁串行化。

## 启动（Docker Compose）

```bash
docker compose up --build
# 打开 http://localhost:8000
```

SQLite 数据保存在命名卷 `session-data`（容器内 `/app/data/sessions.db`）。
API 文档：http://localhost:8000/docs 。

## 本地开发（无 Docker 时）

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
SESSION_DB_URL=sqlite:///./data/dev.db uvicorn app.main:app --reload
pytest                      # 单元/接口测试（17 个）
python tests/smoke_e2e.py   # 对运行中的服务器做端到端冒烟（先启动 uvicorn）
```

## 网页演示

首页（`/`）提供：

1. 设置用户空闲间隔；
2. 粘贴 JSON 分批导入事件（内置“首批 / 迟到桥接 / 迟到落入”按钮）；
3. 推进 watermark 并发布，含“尝试后退（应失败）”按钮；
4. 查看当前会话状态、已导入事件、修订历史（点击修订行查看该修订的完整快照与差异；旧库中无法拆回的编号显示“待核对”且编号列表为空）；
5. **一键完整演示**：场景 A 迟到事件落入已发布会话（撤回+新增）；场景 B 迟到事件桥接两会话（撤回回退 pending → watermark 再前进重新发布），并验证重复导入幂等；
6. **逗号编号演示**：同一用户的 `["a,b"]` 单事件会话与 `["a","b"]` 双事件会话取得不同键，并随两次 watermark 分别发布。

## 主要 API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| PUT | `/api/users/{user_id}/config` | 设置 idle_gap（秒，>0） |
| POST | `/api/events/import` | 批量导入事件（同批须同一 user_id） |
| POST | `/api/watermark/advance` | 推进 watermark 并发布合格会话 |
| GET | `/api/users/{user_id}/sessions` | 最新修订下的会话（发布前无修订时实时推导 pending） |
| GET | `/api/users/{user_id}/revisions` | 修订列表（含每修订的差异摘要） |
| GET | `/api/users/{user_id}/revisions/{n}` | 某修订的完整会话快照 + 撤回/新增差异 |
| GET | `/api/users/{user_id}/events` | 已导入事件 |

### 示例

```bash
curl -X PUT localhost:8000/api/users/demo/config \
  -H 'Content-Type: application/json' -d '{"idle_gap_seconds":600}'

curl -X POST localhost:8000/api/events/import \
  -H 'Content-Type: application/json' \
  -d '{"events":[{"user_id":"demo","event_id":"e1","event_time":"2026-11-22T10:00:00Z"}]}'

curl -X POST localhost:8000/api/watermark/advance \
  -H 'Content-Type: application/json' \
  -d '{"user_id":"demo","watermark":"2026-11-22T10:20:00Z"}'
```

## 数据模型（SQLite）

- `user_config`：每用户 idle gap 与当前 watermark；
- `event`：原始事件，`(user_id, event_id)` 唯一约束保证幂等；
- `revision`：修订头（序号、原因 `publish`/`late_import`、当时 watermark 与 gap）；
- `session_snapshot`：每个修订下的完整不可变会话快照（published/pending）；
- `revision_change`：修订的撤回/新增差异。

会话与差异的 `event_ids` 列：新数据存 JSON 数组（`id_encoding='json'`），旧库行为逗号拼接（`id_encoding='legacy_csv'`，由启动时的增量迁移回填）。`session_snapshot` 与 `revision_change` 的输出均带 `ids_need_review`：旧行拆出的编号数与 `event_count` 不符时为 `true`，此时 `event_ids` 为空数组，需人工核对。

时间以固定宽度 UTC 文本存储（自定义 `TZDateTime` 类型），读回始终为带时区 UTC，规避 SQLite 时区丢失问题。

## 设计说明与边界

- 新会话键为有序事件编号 **JSON 数组**的 SHA-256 前 16 位，带结构边界，编号含逗号也不会跨会话撞键；旧库中内容未变的已发布会话在迁移修订里保留原旧样式键，避免无意义的撤回/重发。
- 首次导入（尚无任何发布）不创建修订，会话以 pending 实时推导；修订只在发布会话集合发生变化（或迁移产生撤回/补发）时产生。
- 旧库升级后的首次重算：待核对的已发布旧键每个只撤回一次；按旧规则共用该键、现在仍满足发布条件的会话以新键补发；同一 watermark 请求重试为空操作，不会二次撤回。
- 迟到事件不会把 watermark 拉回，只重算会话状态：不再合格的已发布会话被撤回并随合并结果回到 pending，待 watermark 再次前进后发布。
- 进程内锁覆盖单进程部署；多进程/多实例部署需换成数据库级锁或队列（当前 Compose 为单副本，满足演示要求）。
