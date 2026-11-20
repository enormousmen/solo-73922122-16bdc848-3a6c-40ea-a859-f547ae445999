# pkg-cache：单进程 HTTP 包缓存代理 + 可控本地源站

面向构建工程的依赖包缓存代理：固定本地源站、按路径 GET 完整包，减少重复
回源流量；源包更新后按路径清除缓存。代理由 **FastAPI + aiohttp** 实现，
单进程单 worker 运行。

## 快速开始

```bash
docker compose up --build
```

* 代理：http://localhost:8080 （下载 API + `/docs` 清除入口）
* 源站：http://localhost:8081 （控制 API + `/docs`，自带 32MiB 包 `pkg-32m.bin` 与 1MiB 包 `pkg-1m.bin`）

## 使用

```bash
# 下载（首次 MISS 回源，之后 HIT 命中磁盘缓存）
curl -OJ http://localhost:8080/packages/pkg-32m.bin

# 按路径清除（也可打开 http://localhost:8080/docs 在 Swagger UI 里执行）
curl -X POST http://localhost:8080/purge/pkg-32m.bin

# 查看代理内部状态
curl http://localhost:8080/cache/status

# 预热缓存：默认等整批处理完返回一个 JSON；加 "stream":true 则逐项输出 NDJSON
curl -X POST http://localhost:8080/cache/prewarm \
  -H 'Content-Type: application/json' \
  -d '{"paths":["pkg-32m.bin","pkg-1m.bin"]}'
# {"items":[{"path":"pkg-32m.bin","status":"ready","size":33554432,"sha256":"..."},
#           {"path":"pkg-1m.bin","status":"ready","size":1048576,"sha256":"..."}],
#  "ready":2}

# 流式：每项一处理完立即输出一行
curl -N -X POST http://localhost:8080/cache/prewarm \
  -H 'Content-Type: application/json' \
  -d '{"paths":["pkg-32m.bin","pkg-1m.bin"],"stream":true}'
# {"path":"pkg-32m.bin","status":"ready","size":33554432,"sha256":"..."}
# {"path":"pkg-1m.bin","status":"ready","size":1048576,"sha256":"..."}
```

`/cache/prewarm` 逐项处理、逐项报告（返回顺序与请求 `paths` 顺序一致）：

* `status` 取值：`ready`（**完整包已落盘入缓存**才报告）、`failed`（回源
  失败/404，不留半包）、`purged`（处理期间该路径被清除）。
* 预热自身发起的回源，任一时刻最多一笔未完成；多个并发批次也共用这一个
  名额。普通下载不占用、也不等待这个名额。
* 同一路径共用填充：若队列中的预热项发起前已有普通下载在回源，预热直接
  采用该次填充的结果，不再二次回源；普通下载也可以加入预热发起的填充，
  从字节 0 起逐块读取，无需等整包。
* 清除时，该路径尚未完成的预热项报告 `purged`：仍在排队的项不再回源；
  已开始的填充不再回源、也不再写回缓存（已加入的下载仍可读完旧代文件）。
* 请求体校验：`paths` 数量 1～16、不能重复、路径须通过安全检查；否则
  返回 422 / 400。

源站控制（换内容 / 延迟 / 断流 / 回源次数）：

```bash
# 查看回源次数与当前包 sha256
curl http://localhost:8081/control/stats

# 更换包内容（重新生成随机字节，sha256 改变；代理在清除前仍发旧包）
curl -X POST http://localhost:8081/control/content/pkg-32m.bin

# 每个 256KiB 块延迟 100ms（模拟慢源站）
curl -X POST http://localhost:8081/control/behavior \
  -H 'Content-Type: application/json' \
  -d '{"chunk_delay_ms":100,"truncate_after_bytes":null,"chunk_size":262144}'

# 发送 200000 字节后断流（模拟上游故障）
curl -X POST http://localhost:8081/control/behavior \
  -H 'Content-Type: application/json' \
  -d '{"chunk_delay_ms":0,"truncate_after_bytes":200000,"chunk_size":65536}'

# 清零回源计数
curl -X POST http://localhost:8081/control/reset
```

## 行为要点

* **单飞回源**：同路径并发未命中只回源一次；所有下载（包括中途加入的）
  都从字节 0 开始逐块收到原字节，首块在源站首块落盘后即可读出，不等整包。
* **慢客户端隔离**：填充只写磁盘，各读者按自己的节奏尾随 `.part-<代>` 文件；
  单客户端内存为一个读缓冲，不随包长增长（实测 32MiB 下载代理 RSS 增量约 4MiB）。
* **按路径清除**：清除返回后首个新下载重新回源，后续共用新填充/缓存；
  已开始的旧下载继续读完旧代文件；旧填充完成也不会重新登记；
  旧文件在最后一个读者结束后回收。
* **失败传播**：上游断流/非 200 使该代所有下载失败（客户端读到不完整
  响应），半包立即删除、绝不入缓存；下一个请求自动重试回源。
* **重启**：`proxy-cache` 卷保留完整包；启动时清理 `.part-*` 残件与孤儿
  meta。404 不做负缓存。
* **预热**：`POST /cache/prewarm` 见上文；`/cache/status` 中
  `prewarm_fill: true` 表示该代填充由预热发起、占着唯一预热回源名额。

## API 示例（curl）

```bash
# 0) 准备：两个慢速 2MiB 包（a.bin / b.bin / c.bin / d.bin），每个 64KiB 块延迟 100ms
for n in a b c d; do
  curl -X POST "localhost:8081/control/content/$n.bin?size=2097152"
done
curl -X POST localhost:8081/control/behavior \
  -H 'Content-Type: application/json' \
  -d '{"chunk_delay_ms":100,"truncate_after_bytes":null,"chunk_size":65536}'
curl -X POST localhost:8081/control/reset

# 1) 两个批次同时提交：后一批的第一项排在第一批后面，共享唯一的预热名额
curl -N -X POST localhost:8080/cache/prewarm -H 'Content-Type: application/json' \
  -d '{"paths":["a.bin","b.bin"],"stream":true}' &
curl -N -X POST localhost:8080/cache/prewarm -H 'Content-Type: application/json' \
  -d '{"paths":["c.bin"],"stream":true}'
# c.bin 与前一批的 b.bin 不会同时回源；源站 /control/stats 的 max_active 为 1

# 2) 预热排队时插入普通下载：不等预热名额；若同路径则加入其填充（不回源）
curl -OJ localhost:8080/packages/c.bin       # 与正在排队的预热项同路径：共用填充

# 3) 预热进行中/排队中清除同路径：对应预热项输出 purged
curl -X POST localhost:8080/purge/b.bin

# 4) 观察是否只有一个预热回源在跑（prewarm_fill）
watch -n0.2 "curl -s localhost:8080/cache/status | python3 -m json.tool"
```

## 本地自测（无 Docker 时）

```bash
pip install -r proxy/requirements.txt -r origin/requirements.txt
pip install httpx pytest
python3 -m pytest tests/ -v
```

测试以真实子进程启动源站与代理，覆盖：单飞、首块延迟、慢客户端隔离、
清除语义（含填充中清除）、断流不缓存半包、重启保留/清理、内存有界、
404 与路径穿越；预热专项（`tests/test_prewarm.py`）覆盖逐项报告、
单预热名额（源站 `max_active == 1`）、排队时插入下载并共享填充、
清除排队/进行中的预热项、失败项不入缓存。`tests/run_server.py` 是测试
专用启动器（本沙箱禁止进程间信号，用控制文件代替 SIGTERM 停服），交付
应用本身不含测试钩子。
