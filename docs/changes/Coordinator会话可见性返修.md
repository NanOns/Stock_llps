# Coordinator 会话可见性返修

本文件及 `src/stock_a/coordinator/` 是用户于 2026-10-28 明确要求的临时审查副本，供线上模型读取本轮 Coordinator 代码和真实验证结果。当前运行入口仍是 `G:/codex work/Stock A 交付/过程资料/协调器/coordinator.py`；项目内副本逐字对应运行实现与输出 schema，不是新的业务架构或数据库组件。

## 本轮实现

- Codex CLI 子进程不再使用 `--ephemeral`，可创建持久会话。
- Windows 子进程 stdout/stderr 并行流式读取；运行期间写入受凭据筛选的日志和 `activity.json`。
- 终端实时显示任务、角色、模型、thread ID 和安全阶段摘要，不打印命令正文或模型回复内容。
- `status` 命令附带每项任务最近会话和活动摘要。
- 保留任务执行/审计模型隔离、执行与审计权限、Windows Job 进程树管理和超时处理。

## 实际验证结果

- 当前 Coordinator 模拟回归共 18 项通过，覆盖连续推进、阶段审计门、返修重审、故障恢复、隔离、权限和敏感内容处理。
- 真实 Codex CLI 在隔离合成仓执行 1 个只读审计任务，模型为 `gpt-6.1-terra/low`，Coordinator 核验结果为 `PASS`，没有改动 Stock A 业务仓或生产数据。
- 任务未结束时，Coordinator 已实时输出 `thread.started`、`item.started`、`item.completed`、`turn.completed` 和 thread ID。
- Codex `read_thread` 与 `navigate_to_codex_page` 对该 thread ID 均成功；当前会话 ID：`01a18043-9e2e-7843-9c76-f522baa7cf49`。
- Codex 侧栏 `list_threads` 未列出 CLI 子进程会话；会话可按 ID 打开，但尚未证明会自动登记到侧栏。
- 普通沙箱子进程首次因无法访问用户配置目录的 `state_5.sqlite` 失败；在隔离仓、只读任务、受控提升权限下重跑后成功。任务沙箱未关闭。
- 18 项回归、真实烟测状态、thread 可读取/导航记录保存在 `G:/codex work/Stock A 交付/过程资料/协调器/验证/c76625d74c14d21d42151db43057c159684c2a669fe17b7e0ca3529848bb98ef/`。

## 未完成与准入状态

本轮只验证会话和进度可见性。Codex 侧栏自动登记仍未实现；R00–R08 真实业务写入未重新验收，Coordinator 的 `live_enabled` 保持关闭，旧验收摘要因代码变化而失效；任务29队列未启动或修改。此临时副本提交仅供线上审计，不表示完整自动化接管完成。
