# Stock A

A股市场结构研究系统的新项目，唯一业务开发主线。旧V4只作为只读设计/故障参考，不迁入旧代码树、数据库或运行历史。

先读[AGENTS](AGENTS.md)、[完整规格](PROJECT_SPEC.md)、[目录与Git准入](docs/design/目录与Git准入.md)，正式需求变化见[需求变更](docs/changes/需求变更.md)。唯一默认路径配置为[config/runtime.defaults.json](config/runtime.defaults.json)。

| 用途 | 路径 |
|---|---|
| 唯一源码 | G:/codex work/Stock A |
| 交付/过程材料/发布包 | G:/codex work/Stock A 交付 |
| 临时/缓存 | G:/codex_tmp |
| 测试源/脚本/fixture | G:/codex_tmp/test_temp/Stock A |
| 后续PG数据 | G:/Postgres/data |

Git只接收业务运行必需内容、需求、设计、正式需求变更和最小根控制文件。全部测试文件/脚本、阶段任务文档/报告/回执、证据、运行产物、数据、构建包与凭据禁止入Git，不论大小。运行记录和历史参考在外部交付目录留存。

正式业务CLI属于src业务包；不建堆放阶段脚本的scripts目录。测试在外部执行并绑定源码版本，业务不能导入外部测试/过程工具。空目录不放.gitkeep，实际业务文件出现时再纳入Git。

当前放置的是根规范、目录约束与默认配置，没有业务实现或可启动服务。PG默认配置尚未应用到当前Windows服务；E盘旧实例保留可读，不自动迁移、初始化覆盖或重启。新项目实际写库前必须核验服务与PG数据路径已符合G盘要求。

本地Git准入钩子保存在.git/hooks，不进入Git或运行包。新环境必须恢复本地准入；提交前仍逐文件审查用途。远端仓库按用户指定配置，不沿用旧V4远端。
