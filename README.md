# Stock A

A股市场结构研究系统的新项目，唯一业务开发主线。采用旧代码分模块复用与改造，保留符合合同的算法和产品组件；旧V4冻结，只读参考。

## 文档入口

**整体需求与设计的主入口是[PROJECT_SPEC.md](PROJECT_SPEC.md)；136项产品需求正文在[产品需求.md](docs/requirements/产品需求.md)。** 主规格通过稳定S编号链接到算法、数据闭环、工程与存储等现行正文，合起来构成唯一完整基线；不另建一份会与分域正文漂移的“完整汇总副本”。AGENTS是工作约束，外部审计报告和实施方案是过程材料，均不能代替产品需求。

主线领域业务规则和产品/API契约已按CHG-016冻结；FEP/分钟独立延后，设计/功能/研究效果三层分开。逐域实现边界见[实现详细设计第13节](docs/design/实现详细设计.md)。各领域合同独立升版，R1/R2等编号较小不等于过期；历史报告/参考原件按各自范围和时间读取，不能代替现行规范或作为当前验收结论。

| 阅读顺序 | 文件 | 职责 |
|---|---|---|
| 1 | [AGENTS](AGENTS.md) | 工作约束、复用原则、目录与Git边界 |
| 2 | [PROJECT_SPEC](PROJECT_SPEC.md) | 产品/业务主规格及稳定S编号索引 |
| 3 | [产品需求](docs/requirements/产品需求.md) | 136项现行需求的唯一完整目录 |
| 4 | [工程与存储规范](docs/design/工程与存储规范.md) | 单schema、typed存储、API、安装、验收与外部文件引用/材料边界 |
| 5 | [算法合同](docs/design/算法合同与升级规则.md) | 字段口径、已知接口冲突和明确新规则选择 |
| 6 | [完整业务链](docs/design/数据业务完整闭环.md) | P00–P25输入、产物、消费者、发布/跟踪/前端与恢复 |
| 6.1 | [实现详细设计](docs/design/实现详细设计.md) | 目标接口、字段/类型、关键伪代码、基础窄图、D1状态转换、PG/发布/读取实现边界；明确已冻规则与实际实现的边界 |
| 7 | [代码复用与模块边界](docs/design/代码复用与模块边界.md) | 具体旧代码候选、目标职责、改造要求及禁用边界 |
| 8 | [目录与Git准入](docs/design/目录与Git准入.md)／[需求变更](docs/changes/需求变更.md) | 文件归属与正式规则变化 |

机器依赖以[day-chain.contract.json入口清单](config/contracts/day-chain.contract.json)声明，并由`stock_a.contracts.load_day_chain_contract`按清单装配[day-chain模块](config/contracts/day-chain/)；子模块清单固定，不扫描目录或动态加载未知文件。唯一默认路径配置为[runtime.defaults.json](config/runtime.defaults.json)。业务文件配置只从本项目config登记目录加载，私密配置如需只在config/private并忽略Git；测试配置与测试文档不得混入正式目录。阶段实施方案和交接文档在外部交付根过程资料中，不提交Git。

| 用途 | 路径 |
|---|---|
| 唯一源码 | G:/codex work/Stock A |
| 交付/过程材料/发布包 | G:/codex work/Stock A 交付 |
| 临时/缓存 | G:/codex_tmp |
| 测试源/脚本/fixture | G:/codex_tmp/test_temp/Stock A |
| 后续PG数据 | G:/Postgres/data |

Git只接收业务运行必需内容、需求、设计、正式需求变更和最小根控制文件。测试文件/脚本、阶段任务/报告/回执、证据、产物、数据、包、缓存和凭据全部外置，不论大小。AGENTS保留在根，保证新开发会话自动读取规则；不为归档移动它。

正式业务CLI在实际实现时归入src/stock_a业务包；测试不能成为业务运行依赖。按实际移植需要创建模块，不铺空实现。前端优先抽取旧产品组件，统一六入口、中文及context语义；不引入新闻、外部事件催化或题材映射。

业务开发路线是先抽取成熟内核，完成真实基础数据→PG→API→页面，再按完整图扩展研究域；每批具体验收和当前进度只维护在外部实施方案/交接中。首次源码移植不能直接导入旧仓，必须验证目标模块自身依赖闭合。

PG默认G盘配置尚不代表Windows服务已经应用。旧E盘数据不自动迁移、复制、覆盖或切换；新项目写库前必须实际核验G盘路径。当前具备分质量日线解码、字段/窗口摘要和基础画像纯求值入口；不提供尚不存在的服务启动命令。纯函数调用显式注入单位/规则，规则JSON数字用Decimal解码，不在导入期读配置；模块导入根为src/，不依赖旧仓。

本地Git准入钩子保存在.git/hooks，不进入Git或运行包；换机须恢复准入。远端按用户指定配置，不沿用旧仓远端。

正常日更业务结果、计划、恢复游标、Owner/索引/回执全部存PG必要类型字段和引用；不生成逐日业务文件，不把源包/二进制/整对象JSON塞进PG。清空隔离测试临时目录不能破坏已入库历史查询和恢复；用户原始TDX来源仍保持只读。

新项目数据库/schema设计身份为stock_a，仍使用唯一PG实例5432；物理G盘、项目/实例/迁移/角色须共同核验，本轮未初始化或切换。FOUNDATION已在现行机器合同绑定为同一执行器的具名范围；生产实现/真实PG/API/UI仍未完成。测试持久资产位于交付根过程资料/测试资产，临时根是执行副本。安装模式保留唯一config槽位，运行包不导入开发源码。

## 安装最小业务包

安装包提供唯一正式中文CLI，`stock-a`与`python -m stock_a`使用同一调度入口。帮助不需要配置：

```powershell
stock-a --help
stock-a doctor
```

`doctor`读取唯一登记的`config/runtime.defaults.json`，输出包/配置/来源根/数据库身份的结构诊断，不连接PG或来源。当前配置登记了`D:/new_tdx`来源根，且`deployment_applied`标记为`true`；这些配置值不证明来源阶段0或PG实际部署就绪。业务能力以只读诊断的实际结果为准，doctor输出不授权业务启动。

单文件检查必须显式指定绝对路径、单位与有限预算。金额倍率示例使用0.5元/原始单位，数量倍率示例使用100股/原始单位；调用者须根据真实合同确定数值：

```powershell
stock-a source-inspect 'G:\已绑定来源根\日线文件.day' `
  --unit-version SOURCE-UNIT-R1 --amount-to-cny 0.5 --volume-to-shares 100 `
  --max-bytes 33554432 --max-records 1048576 --chunk-records 256 --max-seconds 30
```

来源根只从已验证运行配置读取，不能以命令行临时根替代。当前配置登记了`D:/new_tdx`；实际文件可读性、来源权威及数据能力须由阶段0核验，配置存在不代表来源准入。成功摘要采用JSON写stdout，错误JSON写stderr；退出码0为命令完成、2为参数/合同错误、3为配置或来源不可用、4为系统拒绝访问、5为工程错误。所有消息为中文，稳定错误码保留原生标识。

通达信官网盘后完整ZIP只由显式来源命令触发，其他诊断和研究命令不隐式联网或下载：

```powershell
stock-a source-vipdata --target-session YYYY-MM-DD --mode inspect
stock-a source-vipdata --target-session YYYY-MM-DD --mode download-verify
stock-a source-vipdata --target-session YYYY-MM-DD --mode download-verify `
  --consume-symbol SH.六位代码 --unit-version SOURCE-UNIT-R1 `
  --amount-to-cny 0.5 --volume-to-shares 100
```

`inspect`只核对官网实际更新日期和ZIP链接；`download-verify`仅在页面日期及URL通过检查后下载一次完整包到`G:/codex_tmp`，并核验SHA-256、ZIP CRC、entry范围与安全预算。`--consume-symbol`为可选的单证券NON_PIT研究预览，必须显式给出单位合同；它复用现有TDX读取、Canonical选择和Core，不写PG或持久业务文件。网页未公布可核验日期/下载对象时会返回具体不可用状态，不推测下载地址。

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

安装后的模块入口不读取项目配置、行情文件或数据库。调用示例显式提供日线单位、画像输入和规则：

```python
from collections.abc import Mapping

from stock_a.canonical.identity import semantic_digest
from stock_a.domain import FieldValue
from stock_a.research.profile import profile
from stock_a.sources.tdx.day import UnitContract, decode_day_record

def evaluate(raw32: bytes, units: UnitContract, field_id: str,
             inputs: Mapping[str, FieldValue], rules: Mapping):
    day = decode_day_record(raw32, units)
    digest = semantic_digest((day.session_date, day.prices))
    result = profile(field_id, inputs, rules)
    return day, digest, result
```

调用方为 `evaluate` 提供原始32字节记录、单位合同、字段ID、带质量的输入和已登记规则；画像规则需符合字段合同。接口展示纯内核调用，不代表已完成真实来源采集、全域数值验收、数据库写入、服务或页面运行。

## TDX日线有界读取

TDX日线文件入口需要调用方显式提供绝对文件路径、非空保护根、单位合同和读取预算；不会从当前目录、旧项目或默认盘符寻找来源。生产根只应由本项目唯一`config`中的已绑定配置解析后传入。下面示例中的变量由调用方提供，不能用测试/交付目录代替生产配置：

```python
from stock_a.sources.tdx.day_source import open_day_file
from stock_a.sources.tdx.day_stream import ReadLimits

limits = ReadLimits(
    max_bytes=declared_source_byte_budget,
    max_records=declared_source_record_budget,
    chunk_records=256,
    max_seconds=declared_read_seconds,
)

with open_day_file(source_path, (bound_source_root,), units, limits) as scan:
    for located in scan:
        consume_in_memory(located.record_index, located.byte_offset, located.decoded)
    summary = scan.summary  # 仅在EOF、顺序/预算和最终句柄检查全部通过后可读
```

`iter_day_records(readonly_stream, units, limits)`复用同一扫描内核，适用于调用方管理的非seekable二进制流，不关闭调用方流。文件入口使用只读Windows句柄，并核验句柄最终路径、身份及扫描前后大小/修改时点。超限、坏日期、重复/冲突、路径越界、扫描中变化及提前退出都不能获得完整摘要；已yield前缀不能视为完整输入。摘要中的原始字节SHA仅描述本次读取字节，不代表来源捕获、PIT时钟、来源封口或业务接受版本。

## TDX真实数据研究查询

研究查询从`runtime.defaults.json`中的TDX只读来源根定位日线文件，按显式单位合同与预算完成整文件扫描，再复用既有Core。年度日历资源按文件年份动态装载；当前含经沪深交易所年度公告交叉核验的2025、2026年会话，资源记录版本、覆盖、来源URL和摘要。跨年查询只连接目标年前连续覆盖的年度后缀，缺失中间年度会明确报告日历缺口，不会把两侧会话拼成连续历史。目标日期必须显式指定且为已核验交易会话；若目标年不足70个已知会话且缺少连续前置年度，Core依赖足够历史的字段标记`CALENDAR_LOOKBACK_NOT_COVERED`，独立可验证的目标日原始行情仍会返回。缺bar的正式会话标为`UNKNOWN`，不会据此推断停牌或上市前；源记录落在已覆盖年度的休市日时拒绝计算。

来源代码仅是研究序列定位键，不提供稳定`security_id`证明。输出区分`source_last_session`、`target_session`与本机`read_time`；金额/数量的解码质量和单位可信度分列，倍率仍是调用方假设。结果始终标记`NON_PIT_RETROSPECTIVE`和原始未复权坐标；不得当作当前行情、严格PIT、公司行为已调整或生产结论。命令不连接PostgreSQL、不写数据库或持久业务文件。

```powershell
stock-a research-stock SH.600000 --target-session YYYY-MM-DD `
  --unit-version TDX_RAW_RESEARCH_R1 --amount-to-cny 1 --volume-to-shares 1 `
  --max-bytes 16777216 --max-records 200000 --max-seconds 30

stock-a research-stock SH.600000 --target-session YYYY-MM-DD `
  --unit-version TDX_RAW_RESEARCH_R1 --amount-to-cny 1 --volume-to-shares 1 `
  --format summary --include-profile-preview

stock-a research-stock SH.600000 --target-session YYYY-MM-DD `
  --unit-version TDX_RAW_RESEARCH_R1 --amount-to-cny 1 --volume-to-shares 1 `
  --format summary --include-baostock-evidence

stock-a research-compare SH.600000 SZ.000001 SZ.300001 --target-session YYYY-MM-DD `
  --unit-version TDX_RAW_RESEARCH_R1 --amount-to-cny 1 --volume-to-shares 1 `
  --max-bytes 16777216 --max-records 200000 --max-seconds 30 `
  --max-total-bytes 335544320 --max-total-records 4000000 --max-total-seconds 120

stock-a research-universe-preview --target-session YYYY-MM-DD `
  --unit-version TDX_RAW_RESEARCH_R1 --amount-to-cny 1 --volume-to-shares 1 `
  --offset 0 --limit 20 --market ALL --board ALL

stock-a research-universe-preview --target-session YYYY-MM-DD `
  --unit-version TDX_RAW_RESEARCH_R1 --amount-to-cny 1 --volume-to-shares 1 `
  --offset 0 --limit 20 --format summary --include-profile-preview
```

`research-stock`和`research-compare`输出同一既有Core的盘后基础观察摘要，标记`RESEARCH_PREVIEW_NON_PIT`，包含原始收盘、MA5/20/60、ATR20、ret5/20、金额/数量质量和均线位置；每个值保留质量与未知原因。`research-stock --include-profile-preview`在同一次Raw/Core读取上追加11项基础画像组件清单，逐项给出结果或缺因、质量、输入来源/窗口和规则引用。该预览把目标日`raw.close`局部映射到规则的价格输入名，只用于`RAW_UNADJUSTED`描述；不表示已执行P06复权、身份/PIT验证或可信FOUNDATION选择。周/月闭合周期和横截面相对能力按实际缺失输入显示`NOT_IMPLEMENTED`或`CONTRACT_UNBOUND`，不会填成正常状态。正式`profile()`仍拒绝缺少可信FOUNDATION选择器的调用。画像预览不生成Seed、Priority、评分、排序或交易信号。`--amount-to-cny`和`--volume-to-shares`是调用方声明的研究单位假设，不能替代经独立来源合同验收的单位权威。`research-compare`最多接受20个不同证券，并在每只证券的读取上限外共享`--max-total-bytes`、`--max-total-records`、`--max-total-seconds`。超限后保留全部请求分母；`available`、`failed`、`partial_unknown`和`not_evaluated`构成精确分类分区，后续证券不会被静默省略。耗时上限在每个读取块和证券边界检查，文件系统调用本身无法被同步扫描中断时，单个正在执行的系统调用可能延后返回。对比不对证券做身份推断或静默去重。

`research-stock --include-baostock-evidence`是显式联网的单证券、单日、未复权补充查询；默认命令、`research-compare`和全市场分页均不加载BaoStock SDK、不请求该来源。需安装可选依赖`stock-a[baostock]`。请求固定使用`frequency=d`、`adjustflag=3`，包含完整OHLC字段，单socket超时5秒、最多8行、零重试，并报告实际SDK响应字段与UTC请求时钟。补充结果保留Bao原始字段及逐字段质量，并逐项对照原始未复权开、高、低、收；Bao文档中的单位展示为尚未独立验收的单位假设。金额比较保留精确差，并按调用方声明的TDX金额倍率单独模拟IEEE754 float32：只有Bao金额经该坐标编码后精确等于可还原的TDX float32值时，才标记为`FLOAT32_QUANTIZATION_EQUIVALENT_UNDER_DECLARED_UNITS`，同时报告ULP和相对差；该解释不证明单位权威，也不使用固定容差。成交量使用整数独立核对，任意差值均原样保留并标记未解释，不归因于金额float32舍入或复权。上述结果不据此替换TDX或判定TDX错误。Bao换手`turn`按百分比除以100输出fraction；历史换手分位需要已绑定身份及完整前60个有效观察，本单日查询不计算。`tradestatus`、`isST`仅为Bao来源证据；缺行、缺字段或未知枚举均不推断停牌、退市或正常状态。此输出仍为`SOURCE_SYMBOL_ONLY`、`RAW_UNADJUSTED`、`NON_PIT_RETROSPECTIVE`，且不解除正式`profile()`准入阻断。

`research-universe-preview`只读枚举已配置来源根内的`vipdoc/sh/lday`和`vipdoc/sz/lday`，按标准化来源代码排序，使用`_normalize_symbol`的范围规则保留沪深主板/创业板候选；不依据ST名称过滤。文件名只代表`TDX_SOURCE_CANDIDATES_NON_PIT`来源候选，不证明证券实体、上市状态、当日可交易或行情更新。默认每页20只、最多100只，支持offset/limit及市场/板别过滤；目录项和发现耗时有独立上限，目录缺失、越界链接、重复代码及超预算明确失败且不报告完整总数。单页跨20只子批查询时仍共享一次字节、记录和耗时预算。默认以短摘要显示总候选分母、本页、剩余和未评价数量；`--format json`提供完整逐对象详情。`raw_availability`与`core_availability`独立列出观察数、未知数和原因；兼容字段`status`/`classification`不意味着Core全部已知。页内每行生成的单股详情查询引用携带同一目标日、单位和单文件预算，并显式请求画像组件预览；该摘要不连接PG、不写业务状态。

JSON和短摘要另提供`coverage_summary`中的当前页实际目标Bar、目标缺Bar、读取失败、未评价对象、按市场/板别拆分及本页已完成读取文件的末条日期直方图/最大值。末条日期只代表本页完整读到EOF并核验句柄的来源文件，不是来源可得时钟，也不自动证明完整过滤池；多页总体只能在调用方确认计划页全部实际执行后合并。`candidate_counts_by_board`是来源文件名候选分布，主板/创业板研究候选与被研究范围排除的文件分开；均不等同官方有效证券总体。

可选`--include-profile-preview`在当前请求页复用一次固定day-chain合同，并在原有Raw/Core对象计算中附加11项Profile组件；不额外扫描来源，也不逐股重载合同。分页JSON在每个已处理对象中保留完整组件输入来源、质量、原因和规则身份，并另提供紧凑`summary[].profile_preview`与`profile_preview_denominators`。页级组件分母只统计实际返回画像的对象；`evaluated`组件包含质量为`UNKNOWN_DATA`的已执行规则，分别统计`observed`、`unknown_data`、`not_implemented`和`contract_unbound`。失败和预算未评价对象没有伪造组件。中文摘要逐行显示11个组件的结果/质量或未接原因，并显示请求、已评价、失败和未评价分母。未带开关时保留现有分页JSON及摘要行为。预览仍是RAW_UNADJUSTED、NON_PIT、SOURCE_SYMBOL_ONLY，不代表正式Profile、全市场官方证券总体或RPS；正式`profile()`生产门继续阻断。

## GBBQ字节解析

`decode_gbbq_bytes`只解析调用方已经取得的有限字节。调用方必须提供协议版本、类别1单位版本、来源引用及字节/记录预算；它不打开来源文件或读取配置。此示例展示纯解析调用，`source_ref`用于追溯输入，不是证券稳定身份：

```python
from stock_a.sources.tdx.actions import (
    GBBQ_PROTOCOL_VERSION,
    GBBQ_UNIT_VERSION,
    decode_gbbq_bytes,
)

parsed = decode_gbbq_bytes(
    content,
    source_ref="SYNTHETIC:gbbq-example",
    protocol_version=GBBQ_PROTOCOL_VERSION,
    unit_version=GBBQ_UNIT_VERSION,
    max_bytes=content_byte_budget,
    max_records=record_budget,
)
for record in parsed.records:
    if record.semantic_status == "SUPPORTED_CATEGORY_1" and record.data_quality == "VALID":
        fields = record.xrxd_fields()  # Fraction精确值；单位由版本合同固定
```

解码会保留输入顺序、重复记录、行质量、来源位置及未知类别；类别2–14的经济语义尚未接入类别1解释。市场/代码产生的`source_symbol`不等于稳定`security_id`，解析结果也不证明真实来源、PIT或公司行为准入。协议和类别1单位合同见[实现详细设计](docs/design/实现详细设计.md#32-gbbq协议与类别1解释)。

## 精确仿射坐标运输

`stock_a.canonical.adjustment`提供无IO的精确仿射系数、价格/逆变换、OHLC与差值运输。每次调用必须显式提供稳定实体、源/目标basis、目标会话、带时区的知识截止及正tick；公司行为的有效会话和可得时钟由上游经过核验后注入。示例只用于合成研究，不能替代真实来源或PIT证明。

```python
from datetime import date, datetime, timezone
from fractions import Fraction

from stock_a.canonical.adjustment import (
    AdjustmentContext,
    XrxdEvent,
    affine_price,
    build_affine_factor,
    transport_difference,
)

context = AdjustmentContext(
    security_id="SYNTHETIC:entity-1",
    source_basis="RAW:S1",
    target_basis="ADJ:S2",
    target_session=date(2026, 1, 2),
    source_cutoff=datetime(2026, 1, 3, tzinfo=timezone.utc),
    tick=Fraction(1, 100),
)
event = XrxdEvent(
    event_ref="SYNTHETIC:event-1",
    source_ref="SYNTHETIC:actions",
    security_id=context.security_id,
    effective_session=date(2026, 1, 2),
    available_at=datetime(2026, 1, 2, tzinfo=timezone.utc),
    cash_per_10=1,
    rights_price=0,
    bonus_per_10=0,
    rights_per_10=0,
)
factor = build_affine_factor([event], source_session=date(2026, 1, 1), context=context)
adjusted = affine_price(11, factor, tick=context.tick)  # Fraction(109, 10)，即10.90
atr = transport_difference(2, factor)  # 2；仅乘alpha，不加beta
```

同日多事件须传唯一`same_day_order`；缺序、重复引用、未知实体或不支持事件跨越均明确拒绝。正式`ADJ:`坐标在factor中绑定tick，省略或重复传入tick都按该绑定值计算；不同tick有不同目标坐标身份。需要精确未量化结果时，必须明确使用独立`RESEARCH:`坐标和`EXACT_RESEARCH`模式。正式价格只在最终目标坐标执行一次HALF_UP。完整合同见[实现详细设计§3.3](docs/design/实现详细设计.md#33-本地仿射)。
