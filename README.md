# Stock A

A股市场结构研究系统的新项目，唯一业务开发主线。采用旧代码分模块复用与改造，保留符合合同的算法和产品组件；旧V4冻结，只读参考。

## 文档入口

**整体需求与设计的主入口是[PROJECT_SPEC.md](PROJECT_SPEC.md)；136项产品需求正文在[产品需求.md](docs/requirements/产品需求.md)。** 主规格通过稳定S编号链接到算法、数据闭环、工程与存储等现行正文，合起来构成唯一完整基线；不另建一份会与分域正文漂移的“完整汇总副本”。AGENTS是工作约束，外部审计报告和实施方案是过程材料，均不能代替产品需求。

主线领域业务规则和产品/API契约已按CHG-016冻结；FEP首批来源级条件统计可作描述性研究，模型能力及分钟研究仍独立延后，设计、功能和研究效果分开。逐域实现边界见[实现详细设计第13节](docs/design/实现详细设计.md)。各领域合同独立升版，R1/R2等编号较小不等于过期；历史报告/参考原件按各自范围和时间读取，不能代替现行规范或作为当前验收结论。

| 阅读顺序 | 文件 | 职责 |
|---|---|---|
| 1 | [AGENTS](AGENTS.md) | 工作约束、复用原则、目录与Git边界 |
| 2 | [PROJECT_SPEC](PROJECT_SPEC.md) | 产品/业务主规格及稳定S编号索引 |
| 3 | [产品需求](docs/requirements/产品需求.md) | 136项现行需求的唯一完整目录 |
| 4 | [工程与存储规范](docs/design/工程与存储规范.md) | 单schema、typed存储、API、安装、验收与外部文件引用/材料边界 |
| 5 | [算法合同](docs/design/算法合同与升级规则.md) | 字段口径、已知接口冲突和明确新规则选择 |
| 6 | [完整业务路径](docs/design/数据业务完整闭环.md) | Raw来源、Core/Profile、研究消费者和页面读取 |
| 6.1 | [实现详细设计](docs/design/实现详细设计.md) | 现役CLI/API、画像键、研究入口与实现边界 |
| 7 | [代码复用与模块边界](docs/design/代码复用与模块边界.md) | 具体旧代码候选、目标职责、改造要求及禁用边界 |
| 8 | [目录与Git准入](docs/design/目录与Git准入.md)／[需求变更](docs/changes/需求变更.md) | 文件归属与正式规则变化 |

现役价格画像字段以[price-profile.contract.json入口清单](config/contracts/price-profile.contract.json)声明，并由`stock_a.contracts.load_price_profile_contract`装配来源、Core和Profile字段合同。唯一默认路径配置为[runtime.defaults.json](config/runtime.defaults.json)。业务文件配置只从本项目config登记目录加载，私密配置如需只在config/private并忽略Git；测试配置与测试文档不得混入正式目录。阶段实施方案和交接文档在外部交付根过程资料中，不提交Git。

| 用途 | 路径 |
|---|---|
| 唯一源码 | G:/codex work/Stock A |
| 交付/过程材料/发布包 | G:/codex work/Stock A 交付 |
| 临时/缓存 | G:/codex_tmp |
| 测试源/脚本/fixture | G:/codex_tmp/test_temp/Stock A |
| 后续PG数据 | G:/Postgres/data |

Git只接收业务运行必需内容、需求、设计、正式需求变更和最小根控制文件。测试文件/脚本、阶段任务/报告/回执、证据、产物、数据、包、缓存和凭据全部外置，不论大小。AGENTS保留在根，保证新开发会话自动读取规则；不为归档移动它。

正式CLI归入src/stock_a业务包；测试不能成为业务运行依赖。按实际移植需要创建模块，不铺空实现。前端保留已接通的查询和研究交互，按真实消费者扩展市场→板块→个股画像、变化识别与持续跟踪；不引入新闻、外部事件催化或题材映射。

来源级历史观察可用 `python -m stock_a source-market-observation --help` 查看参数。调用时显式提供连续官方会话、研究截止、精确来源包、画像修订、行业/GN成员快照、D1事件窗及一个或多个后继会话数；可按来源代码、信号类别或板块筛选，并对观察明细分页。命令先计算完整来源池与观察组，再筛选输出；每页关联的同股同日摘要保留不同信号和D1锚点，比较结果分别给出事件计权与同日股票去重的价格分布。事件逐会话显示已知状态、UNKNOWN原因及候选线触及；信号转FALSE、候选线触及和正式D2退出是不同含义。指定板块和Rotation窗口可读取该板块的Native、LOO及后验轮动事实。普通观察只读PG；当前成员回看和未复权原始价格后继均为NON_PIT描述，不代表正式事件、可交易收益或持久Focus。

同一命令的 `--view fep` 是只读即时研究预览：按日期、规则和期限输出完整来源池的 MATCH／NO_MATCH／UNKNOWN、同日端点与路径质量、组差和市场宽度条件。它不写FEP业务事实，也不是产品页面的数据源。FEP正式描述性Baseline由`source-fep-baseline`显式离线生产，复用`source_fep_baseline.py`的同一最近秩和按日期平权数学，从PG已存T0登记、完整三态来源池、Forward与合法市场宽度事实读取输入；Reader/API和Focus/Cohort页面只消费已完成typed行。举例：

```powershell
python -m stock_a source-fep-baseline --research-cutoff $researchCutoff --source-package-id $sourcePackageId --rule-id D1_T0 --horizon 1 --horizon 3 --horizon 5 --lane ENTRY --lane DAILY_LANDMARK --persist
```

ENTRY和DAILY_LANDMARK独立计算；REENTRY及FIRST_OBSERVED_LEFT_CENSORED不计入ENTRY。RAW_CLOSE_RETURN分位采用离散最近秩`ceil(n*p)`，偶数样本p50不取两项平均；日期间对比按可评价的独立T0日期等权。MFE、MAE和MDD_CLOSE在现存Forward路径值与冻结字段合同完成逐值一致性核验前标记`NOT_COMPUTED`；本批不把未闭合路径算法混入Baseline。未到期只由官方日历判定，不读取截止外行情。所有结果标记`NON_PIT_EXPLORATORY / DESCRIPTIVE_ONLY`，不足独立日期、质量未知、到期未计算与未来未到期分别披露，不输出预测概率或模型资格。

来源规则登记的跨日状态由`source-observation-segments`离线生产：必须提供研究截止、数字来源包并显式指定`--persist`；可重复指定`--rule-id`，省略时只处理截止日前已有的ENTRY、左截断及REENTRY根登记。DAILY_LANDMARK复用为根段的持续事实，不创建每日新段。生产器只读已存PG登记、规则事实和相邻上下文，以单个序列化事务幂等保存typed逐会话状态；Focus观察段API只读已完成结果。未知事实不推断为FALSE；截止后状态仅以官方日历标记`NOT_DUE`，不读取截止外行情。

单日 `--view cohort` 可计算完整来源池的来源规则和后验路径；显式指定 `--persist-research-facts --persist-research-events` 时，将规则、已到期共享路径和独立D1锚放在同一PG事务中保存，同键异值拒绝。`--view persisted --rule-id 规则 --horizon 期限` 直接读取已存结果；画像修订应使用写入结果中的即时画像修订。`--view persisted-changes` 用 `--before-session` 和 `--before-event-window-start` 比较两个已存T0及当前行业/GN成员快照，非相邻日仅为不同日对照。指定 `--symbol` 可观察冻结线及逐锚结果，附加 `--risk-readiness` 查看可证风险分量和资格缺项。来源级诊断与正式D2、Priority或交易权限相互独立。

`source-research-day --rps-pulse-groups-from-saved --persist` 是单日离线派生入口：必须显式提供T0、研究截止和来源包；只读取PG内相邻Seed、Raw来源目录、已存T0/成员/PULSE/登记事实，不采集行情。它幂等保存每条最新登记×股票×板块关系的RPS5/RPS20并列名次变化和独立质量状态。Focus/Cohort选择冻结规则及板类后，直接聚合该T0已存Forward并展示完整关系分母、正/非正、中位数、覆盖、失败原因与例子；普通GET不遍历跨日全池。成员回看为`CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT`、价格为`RAW_UNADJUSTED`；T+5独立日期不足时显示`INSUFFICIENT_INDEPENDENT_DATES`。

`source-research-day --formal-admission-from-saved --persist` 从明确日期范围内PG已存且闭合的LAUNCH D0分区离线生产正式准入typed事实，不重算Raw/D0、不采集来源、不处理无D0事实的日期。示例使用调用者给定范围与数字来源包：

```powershell
stock-a source-research-day --formal-admission-from-saved --persist `
  --start-session $firstSavedSession --target-session $lastSavedSession `
  --research-cutoff $researchCutoff --source-package-id $sourcePackageId
```

每个目标会话逐行保留`READY`、`BLOCKED`、`NOT_READY`或`NOT_REQUIRED`及公共门原因；同键异内容拒绝回滚。仅READY保存T0严格前20会话PHC20冻结线，下一官方会话生效。Focus与个股GET只读已完成结果；原始单股D0诊断仍为`DIAGNOSTIC`。结果固定标记`SOURCE_SYMBOL_ONLY`、`NON_PIT_EXPLORATORY`和`RAW_UNADJUSTED`，不代表稳定证券身份、严格历史PIT或交易资格。其它D0场景、完整D2、Risk与Priority未实现。

使用 `--view summary` 可在完整来源池计算后查看有已证变化的板块和去重股票，分别最多展示15个板块会话和30只股票，并返回未展示数量。摘要包含原自动观察事件及D0已证转FALSE，后者不改变自动观察组和同源对照。省略 `--horizon` 时不计算后继比较，状态为 `NOT_REQUESTED`；显式指定后仍按原研究路径计算。同一组来源参数下可使用 `--symbol`，或使用 `--board-kind`、`--board-id`、`--rotation-start` 和 `--relative-position` 下钻。展示顺序仅用于描述，并非交易排序；首个请求内会话缺前态，不能推断上升沿。

同时指定精确板块、Rotation窗口与 `--relative-position` 可读取市场和板块跨日原语，以及所选板块内股票的收益、全池RPS、成员中位数和名次变化；加 `--symbol` 可查看未产生观察事件的成员。等值采用中秩，字段分别披露已知与未知成员分母；板块的单日变化与共同成员双端重算分别展示，不构成新阶段或正式资格。后继价格复用本次只读结果，不能反向选择T0。

业务开发路线是先抽取成熟内核，完成来源事实→PG→画像→API→页面，再按实际研究依赖扩展市场、板块、个股、变化识别与跟踪；每批具体验收和当前进度只维护在外部过程资料。首次源码移植不能直接导入旧仓，必须验证目标模块自身依赖闭合。

PG默认G盘配置尚不代表Windows服务已经应用。旧E盘数据不自动迁移、复制、覆盖或切换；物理位置独立核验与业务数据库身份分别记录，用户确认但未独立核验的状态不得冒充实测通过。当前具备分质量日线解码、字段/窗口摘要和基础画像纯求值入口；开发服务启动方式见下文。纯函数调用显式注入单位/规则，规则JSON数字用Decimal解码，不在导入期读配置；模块导入根为src/，不依赖旧仓。

本机Git钩子启动器位于`.git/hooks`；唯一守卫源码为`devtools/stock_a_git_guard.py`并受版本控制，换机后须安装本机启动器并确认`core.hooksPath`。守卫不进入运行包。远端按用户指定配置，不沿用旧仓远端。

来源事实、画像和实际研究结果按类型字段存PG；不生成逐日业务文件，不把源包/二进制/整对象JSON塞进PG。清空隔离测试临时目录不能破坏已入库历史查询；用户原始TDX来源仍保持只读。

新项目数据库/schema身份为stock_a.stock，使用现有PG实例5432；PG只按任务授权核查或迁移，不因设计文档自动切换。安装模式保留唯一config槽位，运行包不导入开发源码。

## 安装最小业务包

安装包提供唯一正式中文CLI，`stock-a`与`python -m stock_a`使用同一调度入口。帮助不需要配置：

```powershell
stock-a --help
stock-a doctor
```

`doctor`读取唯一登记的`config/runtime.defaults.json`，输出包/配置/来源根/数据库身份的结构诊断，不连接PG或来源。配置值不证明TDX来源当前可读或PG实际连接/部署就绪；业务能力以只读诊断的实际结果为准，doctor输出不授权业务启动。

当前可安装包只包含 `src/stock_a` 业务模块；运行依赖为`psycopg[binary]`，并在Windows上安装`tzdata`以提供`Asia/Shanghai`时区数据库。构建使用固定版本`flit_core==4.1.0`。支持Python 3.10及以上，当前已核验的解释器版本以外不据此声称逐版本实测。

在Windows PowerShell中构建wheel并安装到独立环境。构建输出、缓存、虚拟环境和临时文件都放在G盘临时根：

```powershell
$env:TEMP = 'G:\codex_tmp'
$env:TMP = 'G:\codex_tmp'
$env:TMPDIR = 'G:\codex_tmp'
$env:PIP_CACHE_DIR = 'G:\codex_tmp\stock-a\pip-cache'
python -m pip wheel --no-deps --wheel-dir 'G:\codex_tmp\stock-a\wheel' 'G:\codex work\Stock A'
python -m venv 'G:\codex_tmp\stock-a\venv'
& 'G:\codex_tmp\stock-a\venv\Scripts\python.exe' -m pip install 'G:\codex_tmp\stock-a\wheel\stock_a-0.1.0-py3-none-any.whl'
```

安装包使用自身的程序与合同资源，并共用唯一的`config`槽位；正式股票价格Reader从PG读取BaoStock类型化事实。低层研究内核不是行情采集入口，也不代表全域数值、数据库或页面验收完成。

## 当前来源与产品入口

股票价格只使用BaoStock已保存的PG捕获：RAW未复权用于当日价格与量额，QFQ前复权用于跨日Core/Profile结构。TDX只提供行业/GN概念目录与成员关系；本项目没有本地TDX股票日线、VIPDATA日K或`.tnf`名称读取入口。现行命令以`stock-a --help`为准；数据初始化和数据日更只通过产品运营菜单显式启动。

远端行业/GN目录与成员来源经`source-tdx-sector-members`管理；目录、成员和研究页面只读取已保存的PG结果。GET请求不采集来源、不启动全市场计算或写PG。未计算、数据不完整、无存储事实与未知质量按接口返回的状态分别展示。

## 本机市场与个股研究

在开发环境中，产品研究服务仅监听`127.0.0.1:28766`，页面从固定`stock_a.stock`读取Raw和画像；单股查询按日期和来源版本读取PG历史并调用现役Core/Profile。服务不写PG、不扫描来源ZIP、不联网、不保存结果；结果按实际数据范围和NON_PIT质量展示，不表示官方上市总体或正式选股。

页面按明确研究日期、BaoStock来源捕获ID、价格坐标和代码读取Raw与画像结果；没有已存结果时显示未计算、无存储事实或对应质量原因。

```powershell
$researchCutoff = Read-Host '输入研究截止日（YYYY-MM-DD）'
stock-a product-serve --host 127.0.0.1 --port 28766 --research-cutoff $researchCutoff
```

按`Ctrl+C`安全停止。服务拒绝其它主机、端口28765或已被占用的28766；不会接管或终止现有进程。
