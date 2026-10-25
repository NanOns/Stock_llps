# Stock A PROJECT_SPEC.md：完整产品与工程规格 R2

合同：`STOCK-A-PROJECT-SPEC/R2`。日期：2026-10-25，Asia/Shanghai。状态：`DESIGN_BASELINE_READY_FOR_REVIEW`。本文件给出新项目完整目标、数据模型、主链、算法、交互、运维与验收；不是旧V4继续开发卡，也不证明新代码已实现。R2统一完整业务链、算法命名/单位/时间角色及无新闻产品范围；Stock A源码/交付根和Git业务/规范白名单保持，测试、阶段材料、回执与产物全部外置。后续PG默认路径为G盘；当前E盘旧数据仅保留参考，不迁移、不覆盖、不因本轮配置修订切换服务。

## S01 项目目标、范围与完成定义

新项目是后续唯一主线。服务单用户盘后A股主观研究，覆盖市场→行业/概念→全市场个股→结构/状态→关注→独立Forward及可选条件统计。用户每天能够理解环境、变化、风险、候选为何入选/未入选、下一步观察及失效条件。研究辅助不连接自动交易，不承诺上涨或收益。

完整产品包含六入口、全市场Daily Profile、日周月K、市场四轴/压力变化、板块生命周期/扩散/轮动/重叠、个股Seed/PREWATCH/确认/结构、可解释Priority、Focus、独立Cohort与Forward、回放/比较、真实日更/恢复、Windows安装与托盘；BaoStock补充、FEP、分钟是独立能力支线。新闻、资讯抓取、外部事件催化及题材映射不在本项目范围，不设置对应菜单、API、表或占位能力；TDX行业/概念分类与成员关系保留。

首条交付是一个真实已收盘交易日的端到端基础研究链，随后至少两个真实不同交易日连续增量、重跑与故障恢复。首链不删除完整产品目标，基础通过不宣称全产品完成。新股不因缺两年历史而没有画像；缺字段以真实质量和原因保留。

完成维度独立：SOURCE_CAPTURED、DATA_VALIDATED、RESEARCH_COMPUTED、EOD_CONSUMABLE、PIT_PROVEN、PRODUCTION_RELEASED、STABILITY_OBSERVED、MODEL_SUPPORTED。普通后验研究无需伪造严格PIT；模型/真实前瞻能力必须满足其时间证据。每批预先冻结声明能力和全市场分母，运行后不能删失败域换全通过。

## S02 单一实现、项目结构与权威

只使用一个批准源码目录、一个PG实例/库和一个业务schema；环境参数从受验证启动配置读取，未变更前沿用AGENTS的G盘、28765/28766与5432约束。用户已指定本项目源码根`G:/codex work/Stock A`、交付根`G:/codex work/Stock A 交付`；临时根`G:/codex_tmp`、测试根`G:/codex_tmp/test_temp`保持不变。当前仅放置规范/配置与Git准入，不授权搬旧代码、迁移旧库或启用服务。

唯一运行路径与PG部署默认配置采用`config/runtime.defaults.json`：数据`G:/Postgres/data`，WAL`G:/Postgres/data/pg_wal`，日志`G:/Postgres/log`，主动导出/备份`G:/Postgres/backups`；PG排序/哈希等临时文件使用G盘默认表空间，子进程TMP/TEMP/TMPDIR为`G:/codex_tmp`。PostgreSQL程序二进制仍可位于现安装目录，程序所在盘不等于数据所在盘。

现有`E:/Postgres/data`和当前5432服务保留必要只读可达，不自动复制、迁移、清空或initdb覆盖。后续新项目启用时核验/应用服务实际-D参数，不以PGDATA或文档中的路径冒充已生效；数据/日志/WAL/表空间最终解析路径必须在G盘。当前服务不因本轮默认配置修改被重启/切换；不得为保留旧数据再自动新增长期实例/端口。启用时目标目录非空或端口被占用，按明确任务核定，禁止覆盖和抢占。

源码结构：`src/`按source、canonical、factors、research、tracking、publication、storage、service、desktop分责任，正式CLI归入src；`web/`仅产品前端；`config/`仅无密业务运行配置/合同；`migrations/`仅正式业务DDL；`resources/`仅产品运行资源。测试源/fixture/测试脚本/配置在`G:/codex_tmp/test_temp/Stock A/`按域及integration/e2e组织，不提交Git。不存在承载阶段工具的根scripts目录，也不机械创建所有空模块。

只有一个受支持CLI/任务注册入口，子命令承担inspect/ingest/build/validate/publish-dev/serve/recover等明确职责；一个维护功能不再复制独立任务日期脚本。Git中根README/AGENTS/SPEC与需求/设计/正式需求变更各有唯一职责；CURRENT、ISSUES执行台账、阶段任务/报告/回执/截图全部在外部交付根的过程资料，不能提交Git。正式需求变更只写规则变化和理由，不写阶段状态/执行报告。

五层职责：S来源/捕获；C规范事实与身份；F纯计算/状态；P不可变manifest与受控可见指针；V只读API/轻量投影/页面。每域只有一个现行producer/reader。旧代码只按函数审查借鉴，不能导入旧workspace、历史收据路径或兼容层作为日常依赖。

## S03 来源合同、阶段0与原始数据

每数据集注册dataset_id/version、供应方/URL或配置根、字段/单位、时钟语义、适用市场/范围、包与解析版本、重复/冲突政策、请求预算/timeout/size/retry/circuit-breaker、保留与降级。登录成功、ZIP存在、HTTP200不等数据可用。

所有TDX配置源根只读，写入前解析绝对路径及链接/别名，覆盖子进程/GBBQ/解压/日志/缓存。缺根配置时拒绝写，输出源外同卷原子写。新项目阶段0核对输入能力/来源身份/坐标及保护边界，输出FULL_PASS/DEGRADED_PASS/BLOCKED和影响scope；不借用旧项目的FULL_PASS。

TDX本地日线/合法官方包为核心，GBBQ及本地仿射引擎生成复权；BaoStock承担有合同的换手/状态交叉核验等补充。包重复大小写成员、路径穿越、symlink/reparse、坏CRC/SHA、解压漂移、重复证券日期记录必须拒绝或精确隔离，不能字典后写覆盖。

来源包只留必要单份字节，PG存manifest/捕获事实/解析关系。source_content_id与capture_id分开，相同内容不同捕获时钟不合并事实；从同一内容解析相同行无需再次复制。原件缺失列精确SHA/范围与不可恢复影响，不能用派生输出反证输入存在。

请求ready时钟不早于完整下载、校验及必要多源最晚可用；旧缓存/包日期不能提前。基础来源缺失只阻断真实依赖，不因可选turnover失败停止Core。免费来源预算按实际ledger计含登录/查询/重试/退出，未验证能力不得无限探测。

## S04 身份、日历、成员与双时间

security_entity_id与当前symbol分离；源记录保留source_instrument_id。官方事件/稳定身份不足时隔离provisional/unmapped，不靠代码形状/行情连续自动合并实体。指纹只作候选发现，盲测少量成功不证明全市场误报率；合并/吸收后继与纯换码区分。

Data Universe、制度可交易Universe、Research Universe分别登记日期、成员、排除与摘要。全市场画像基于有效身份来源，未知身份原行保留在隔离分母；当前query_all_stock不得覆盖已绑定历史Lifecycle。

交易日历来自有版本的交易所事实；窗口索引越界明确失败，不将自然工作日当交易日、不使用负索引环绕。ST/停复牌/IPO特殊期/退市/板别有当日有效来源和known time，现行ST不证明过去ST，代码首次观测不证明IPO。

行业/概念/风格/未知类型分明，行业父子关系和成员有effective_from/to、observed_at、source_revision。主行业由分类关系决定，不按当日涨幅挑选；支持概念保存全部合法关系，算法支持板块另算。风格不进入主线席位，类型未知不假称行业。

最新TDX映射可在明确NON_PIT_RETROSPECTIVE变体中回看，保存snapshot_as_of、已映射/未映射与覆盖；不当历史真实成员。valid_time、available/known_time、captured_at、seal/cutoff分别验证，后来可得来源不回流T0。

## S05 数据模型、版本键与单schema

业务schema固定`v4`，不按模块/release/date/workspace/test建schema或整套表。以下为逻辑表族，不要求同义重复表；迁移只为本阶段实际写读能力新增对象。

| 表族 | 主要业务键/身份与作用 |
|---|---|
| contracts/parameters/migrations | contract_id/version、参数版本、生效范围、schema版本；源文件与实际采用内容SHA |
| source_contents/captures | content_sha唯一原字节；capture_id含真实时钟、供应方/覆盖、验证状态，关联内容 |
| securities/aliases/status/calendar | entity_id、symbol有效期、日期/状态revision；日历版本与市场角色 |
| sector_definitions/membership | sector_id/type、member_revision、effective/known时点及成员关系 |
| daily_bars/actions/price_coordinates | entity/date/source_revision、RAW单位、公司行为与目标坐标引用；一日一行，不存整证券长JSON |
| factor/profile/domain_results | entity/date/algorithm+parameter+input版本；可索引业务字段及字段质量，未变结果复用 |
| structure/episodes/state/events | event/anchor/episode稳定ID、前态、冻结参数/坐标与关联；源事实/修订追加 |
| focus/cohorts/observations | 跟踪操作与研究样本分开；观察key含T0/Lane/rule/entity/capture revision |
| schedules/settlements | observation/horizon/due_session；settlement_revision、evaluation/source identity；不让PENDING卡住DUE |
| manifests/heads/jobs/outbox | 不可变版本向量、受控CAS指针、run阶段/失败/幂等及事务outbox |
| serving indexes | 检索键、稳定排序/必要摘要、manifest引用；可再生，详情按需读取 |
| optional model | 实际功能实施时按typed键新增；不先建35张空表，不通用payload承载全部业务 |

PK/UK/FK/CHECK覆盖业务日期、entity、版本/namespace及内容一致性，迁移合并后同kind+fact_key不得覆盖。精确manifest跨表冻结版本向量，逻辑角色唯一；不能因追求一个Head而把来源/模型/结算所有修订压进单指针。

源/已接受历史/首冻/标签/预测事实不可原地改写；修正追加revision，first-observed与latest-validated分别可查。当前指针是受控可变对象，CAS与receipt/outbox同事务；失败全部回滚，历史不删。旧locator迁移保存原descriptor/SHA和新逻辑等价证明，不宣称换路径后原字节没变。

Reader/GET无DDL/DML，默认READ ONLY；请求内count/页/settlement一致快照用RR或经证实等价机制。写事务显式目标/范围，开发凭据可统一，生产最小实际权限另验；不输出口令，不绕过访问控制。

## S06 大表、去重、冷热与容量预算

禁止ZIP Base64→Forward JSON、万能kind/payload热库、每release全量历史复制、ReadIndex values+summary+detail+research_objects重复整对象。必要JSON仅存有界半结构附加字段，不替代可查询业务列/关系。

不变事实按内容/业务键复用，运行尝试与结果身份分离；相同输入重跑只追加短run记录，不新增第二套行情/画像/索引。publication引用变化域的新结果及未变域旧结果，兼容性由manifest验证。索引只持久必要查询字段，缓存有上限/TTL/明确失效。

容量按行数×字段/索引/TOAST/WAL、初始历史范围、每日增长、重算峰值、备份/恢复及G盘余量规划；原始源/不可再生证据、规范事实、派生、缓存、日志分级。业务历史保留不是无限保留全部中间态；无消费者候选按保留策略退休，实际删除不改审计记录。

控制回执目标≤64KiB，>1MiB报警逐对象核验；大量行证据压缩外置/存typed表，以摘要引用。Git禁止任何运行报告/证据/回执/测试/样例数据/快照/行情包，大小不构成豁免；业务必需的无密来源合同可入config或正式设计。测试反例及来源实测证据只在外部保存。物理关系大小/精确或估算行数/逻辑重复/可释放磁盘分开报告，不能将VACUUM FULL代替模型治理。

项目骨架就执行：新release不增加业务schema；GET不写库；同输入幂等无重复大payload；新增常驻文件/表有用途与生命周期；任务结束无自己创建的临时执行器残留。后续高级功能不能绕过这些门。

## S07 Canonical、复权、制度与周期

RAW OHLCV/amount按供应方原单位验证并规范，金额CNY、量的股/手换算有明确合同；盘后完整性/停牌与缺bar分开。RAW实际价格正且包络合法；终值退市0走独立terminal-value来源，不当普通负/零bar。合法仿射历史的允许域与可求收益的价格域分别定义，不仅验finite/布尔verified。

本地仿射转换`P_target=alpha*P_source+beta`，alpha/事件来源/可得时间可验证；同一观察字段的OHLC/Anchor/阈值必须同目标坐标，不能只改basis标签。公司行为截断不含cutoff后未来事实；外部复权只在明确验证范围作参考，不反向替换。

技术窗口用已验证actual bars，仅跳确认停牌；收益/RPS/相对市场固定市场会话端点；Forward用T+N会话。每窗口存start/end、calendar_span、actual_count、suspended_count、unknown_gap；未知缺日不能取最近N条凑齐。

日/周/月由Canonical Daily派生，closed与as-of分版本；未来行物理截断，跨周月/节假日/复牌/当前未闭合/日历超前均有反例。正式周趋势用已闭合MA5周，月趋势用MA3月；图表可显示未闭合bar但明确标记，不影响已闭合规则。

涨跌停引擎按当日交易所/板别/ST/上市特殊期/前收/除权参考及tick舍入；无有效制度与receipt时不可确定。不能由普通前收、现行ST或涨幅猜历史规则。一字/首板/连板只给日线能证明的可判定性，不造分钟封开板时序。

## S08 核心因子与完整字段合同

每字段登记producer、表达/AST、参数、单位/值域、窗口、t或t−1角色、所用Universe、坐标、源/输出摘要、必需/可选、消费者、未知/NA处理。结构校验后还需独立数值执行，不能把预计算字段引用当算法本身。

| 字段 | 确定口径与边界 |
|---|---|
| MA_N、HHV/LLV_N | 含当前N根合法actual；prior高低严格排除当前，不能索引差一 |
| TR/ATR_N | TR=max(H−L,abs(H−previous actual C),abs(L−previous actual C))；ATR=SMA(TR,N)，需N+1价格bar；ATR≤0归一化不可用 |
| retN/volN | 固定市场t−N→t；ret=Ct/Cstart−1；vol为N相邻log收益population std(ddof=0)，N+1点，停牌/未知不填0 |
| slope20/60 | (MA20−MA20前5actual)/ATR20；60前10actual；25/70actual窗口；与市场趋势5市场会话区分 |
| RPS5/20 | n≥2时100*(less+0.5*(equal−1))/(n−1)，同值同分，全同为50；n<2 UNKNOWN |
| RPS Δ1/Δ3 | 两端收益都在目标T0同仿射坐标，分别在各端点精确Universe重排；差单位百分点，可负/0；不要求两端成员相同 |
| rel_market_N | 股票retN−起始日精确历史可评估成员等权均值；保留全部known/unknown及覆盖；不是指数趋势参考或Forward篮子 |
| amount/volume_ratioN | 当前分子/严格前N根actual原值均值，分母不含当前；N=5/20，拒非法分母，金额不随价格复权 |
| CLV | (C−L)/(H−L)，H=L UNKNOWN；不改成另一符号尺度 |
| position/ratio | pos60=(C−LLV60)/(HHV60−LLV60)；bias=(C−MA20)/ATR20；dist=(prior_high20−C)/ATR20；range5/20、ATR5/20、vol5/20同口径 |
| core_price_damage | C<prior_low20−0.5ATR20 AND ret1<0，纯价格规则，不读Anchor/最终状态 |

全市场Profile先于筛选，必须有趋势、位置、均线结构、相对市场、压缩、量额、extension、数据质量；高级结构/板块/换手是可分层补充。不入选仍有画像/原因，不因首页cap丢底层对象。

趋势按以下first-true顺序计算，各分支只在必需字段已知时判定；高优先分支的UNKNOWN是否阻止后续，必须由采用的真值表表达，不能靠代码默认else：

| 顺序/状态 | 条件 |
|---|---|
| DOWNTREND_STRONG | C<MA20且slope20<−0.1且slope60<−0.1且LL_PROGRESS |
| UPTREND_STRONG | C>MA20且slope20>0.1且(C>MA60或slope60>0.1)且HH_PROGRESS且非damage |
| DOWNTREND | C<MA20且slope20<−0.1 |
| UPTREND | C>MA20且slope20>0.1且非damage |
| SIDEWAYS_WEAK/STRONG/SIDEWAYS | 前述均不成立时，分别C<MA20、C>MA20、C=MA20 |

HH_PROGRESS=max(H[t−4:t])>max(H[t−9:t−5])；LL_PROGRESS=min(L[t−4:t])<min(L[t−9:t−5])。区间两端包含，使用同坐标actual bars，不读未来高低。

位置按bias≥3先EXTENDED，再pos60的0.8/0.6/0.4/0.2分区；drawdown≥−5%为SHALLOW、≥−15%为MODERATE，等号边界不可丢。

均线BULL_ALIGNED为MA5>MA10>MA20且slope20>0，BEAR反向，TRANSITION用MA5/MA20及slope符号，其余MIXED。相对状态的Δ3统一指rps5_delta3_pp，按first-true：ACTIVE_EMERGENCE为Δ3≥10且(正在压缩或均线BULL_TRANSITION/BULL_ALIGNED)；PASSIVE_RESILIENCE为rel_market_1>0且非ACTIVE且Δ3≤0；LEADING_ACCELERATING为RPS20≥80且Δ3>0；LEADING_STABLE为RPS20≥80且Δ3≥−3；IMPROVING为Δ3>3；WEAKENING为Δ3<−3；LAGGING为RPS20<20；其余NEUTRAL。所需compression/MA状态未知不得落默认中性。

压缩先判断atr或vol ratio≥1.5为极端扩张、≥1.1为扩张，再按range≤0.35/atr≤0.7/vol≤0.7/amount≤0.8/流动性构成强压缩，较宽阈值0.6/0.9/0.9构成压缩；minimum_liquidity仅依赖前20金额均值≥20,000,000CNY，不能隐藏依赖当前amount_ratio20。

量额ratio20分界0.5/0.8/1.2/2；参与结果区分高参与反转、有效推进（ret1>0且CLV≥0.7）、低效、低参与涨/跌与正常，缺CLV的高优先分支不直接降成已知低效。extension以bias≥4 EXTREME，≥3或(ret5≥3*ATR20/C且amount_ratio20≥2且ret1≤0)为HIGH，≥2 MEDIUM，其余LOW，逐分支依赖。

这些阈值是继承的版本化研究规则，不是最优参数/上涨概率。新实现必须给每状态的完整有序表达、全部必需字段/正反边界及独立Oracle；摘要不得替代精确AST。更换规则须显式新算法版本，不静默改旧结果。

## S09 Seed、板块、LOO与轮动

本节Seed的Δ3统一指rps5_delta3_pp。Seed安全：Research Universe、actual bar、合法价格、前20流动性、无price damage/严重extension，按专属三态归约。路径S1为bias<3且压缩且Δ3≥3；S2为bias<3且Δ3≥10且早期均线转强。只读Core，不读板块最终状态、Focus或高级Anchor。

板块按日期真实成员M与可评估E计算覆盖，最低成员5、quote coverage≥0.8；RS_N为retN中位数，板块百分位在同日同类型/源/成员质量合格全集排名。dq5为RS5百分位跨3会话差；跨日宽度/MA变化在两端共同成员可评估集合分别重算，成员加入/退出单列。

Seed宽度记录k/n/未知与Wilson下界启发式(z=1.96)，其值为`(p+z²/(2n)−z*sqrt(p*(1−p)/n+z²/(4n²)))/(1+z²/n)`，p=k/n，n=0为UNKNOWN；仅作小样本惩罚，不称统计置信/上涨概率。强势成员RPS20≥80；留存分母冻结，未知成员不当退出，不删缺值后改分母。无适用前强势集合的成熟留存为NA，不应阻止有独立证据的早期Rotation。

板块PREWATCH：安全 AND dq5≥3 AND(breadth_delta3>0 OR ma20_delta3>0) AND adjusted_seed_width≥0.05。WARM/CONFIRMED按算法合同ALG-23的PURE_CORE_SECTOR_QUALIFICATION_R2明确设计，不继承Amount A硬依赖；这是新规则选择，实施前完成采用与独立验证，未采用/未实现不得报完整板块能力。

行业/概念底层独立，Overlap提供Jaccard/簇/unique share/集中度；不做1/N成员稀释。LOO排除目标后重算中位数、rank、Seed/板块原始资格及必要历史，不能只减计数。算法支持板块选择按LOO确认/成熟、emergence、Seed宽度、stable_id，不能提升个股硬资格。

Rotation有NONE/PULSE/IN/ACCEPTED/EXPANDING/REACCELERATING/OUT/FAILED/UNKNOWN；pulse使用dq5≥10且breadth_delta1≥0.05，冻结pulse前基准/当时成员/Seed和上涨集合。价格retention用固定篮子，不将每日中位收益拼成指数。

early与mature retention分开，前强势分母0只影响mature；单龙头高集中且无扩散不得通过接受/扩散。脉冲后至少1会话才IN、至少2才ACCEPTED，成熟留存/扩散才EXPANDING或REACCELERATING；失败/连续衰退/5会话未接受按冻结有序规则终止；未知暂停计数。新项目明确提出采用`ROTATION_RESEARCH_DEFAULTS_R1`，继承旧R5工程候选数值但重新登记采用状态：early_seed_retention≥2/3、early_breadth_retention≥0.6、breadth_delta≥0、early_top1_concentration≤1/3、mature_strong_retention≥0.75、diffusion_top1≤0.5。emergence HIGH阈值为dq5≥10、breadth_delta3≥0.05、adjusted_seed_width≥0.1。各字段分母、冻结pulse基准与未知规则按前述固定篮子/共同成员合同执行，不能仅有阈值而缺公式。旧配置formal_consumer_enabled=false，以上是新项目待采用默认，不是旧系统已获正式有效性证明。其余未定义规则须在实现前补齐同一合同，不能临场填阈值。

## S10 市场轴、三族风险与Priority

趋势使用动态参考目录中被配置为PRIMARY_MARKET角色的类型化市场序列，不在代码写指数证券号。当前publication绑定close、MA20、5市场会话前MA20及坐标，严格大于为STRONG、严格小于为WEAK，其余NEUTRAL，缺证UNKNOWN。

Breadth主式`mean(I(ret1_t>0)−I(ret1_t−3>0))`，零收益为0，精确比较日集合及两端收益可核验；SIGN差单列诊断，不替换。Core仅在日期/范围/精确身份集合与合法历史Universe相等时作为加速，不能只比数量。

Participation为合法amount_ratio20中位数；压力等级为同日合法限价覆盖下跌停数/可评估数，压力变化用前后同口径可比集合。缺当日限价只关压力，缺前日只关变化；共享身份/摘要损坏关其全部依赖，不能全局抹掉独立可算轴。

继承研究阈值：宽度±0.05，参与扩张≥1.2/稀薄<0.8，限价最低覆盖0.8，压力elevated≥0.01/high≥0.05。复合环境按版本化规则和hysteresis，不用投票补缺轴；真实可计算和获准用于Priority是不同状态。指数参考、横截面均值、Forward固定篮子不得互换。

股票/板块/市场三族风险分别物化总体/known/unknown/coverage/变化，first-observed与前态变化分开，不用Focus行数替代全市场分母。

Priority的枚举rank、方向和缺键处理见算法合同ALG-32，Δ3明确为rps5_delta3_pp。Priority保留研究序，不合神秘单总分；资格、完整性、桶、emergence、structure、risk、staleness与stable tie-break明确。股票按bucket→emergence→structure→Δ3降序→risk→改善年龄→prior20金额→stable_id；板块按emergence→rank_velocity→breadth/MA变化→调整Seed宽度→exhaustion→stable_id。风险枚举不按字符串排序，必需排序键未知归UNKNOWN且不进入RANKABLE；已知资格仍独立保存。可选展示键缺失可置其明确末位，不制造默认中性。

新项目若采用操作型分值变体，必须独立contract/version/全量Oracle，不与上述Priority V1同名混用。rps20可为完整性前提但不直接加分；合法TARGET_COORDINATE不得被展示丢弃。已知FALSE/必需UNKNOWN的支配按本规则表，全部原因与互斥主因分开。事件变化序、研究排序、显示cap及手动关注分开；NEW不自动第一名。

## S11 D0、D1、D2与状态：补齐设计而不补造旧事实

D0四场景的原语、全部谓词、冻结输入和主次顺序按算法合同ALG-27/28的D0_SCENARIO_FACTS_R2设计，替代旧未闭合legacy标志。STRONG_PULLBACK、TREND_CONTINUE依赖合法t−1 episode/LOO raw，不能从同日reducer/Focus反推。原始谓词、场景治理、正式聚合分列；每次运行预先固定FORMAL/DIAGNOSTIC角色，诊断UNKNOWN不污染正式集合。完整目标包括四场景，部分实施只能报具名范围。

新项目封口方案`SOURCE_SEAL_R1`：manifest列全部必需数据集、time_role、原内容/日期身份、实际available/captured时钟、规则/参数与来源验证。对真实盘后首捕，时间边界取官方日历下一常规交易会话open，严格在此前实际就绪/冻结才可申请LIVE/PIT分能力资格；迟到保留LATE/NON_PIT。开发封口只声明真实验证事实，不伪装第三方签发；布尔sealed不能代替证据。历史缺真实捕获不能补首冻。

D1覆盖突破、回踩、恢复、impulse、支撑/接受/失效、多Anchor与活跃选择器。锚在创建时冻结类型/价格区间/坐标/来源/窗口/算法；新锚不在同日自证支撑，使用t−1事件。突破示例继承C>prior_high20+0.1ATR20且CLV≥0.7，创建日tentative，至少两连续可评估守住才accepted。没有前置有效上涨/突破不能叫pullback，收复事件与真正守住分开。

**D2新项目设计`FROZEN_SCENARIO_INVALIDATION_R2`**按算法合同ALG-30执行，替代R1泛指PRIOR_HIGH的方案：LAUNCH冻结PHC20，RECOVERY冻结实际触发的MA20或MA5，PULLBACK冻结关联旧support lower，CONTINUE冻结关联旧support lower或明确T0 MA20事件线。输出failure_level/coordinate/source_window/source_event/effective_from，出生要求C>failure_level，最早下一市场会话生效；后继同坐标C<=冻结线只失效关联episode。不得用PHH20替代只突破收盘平台的事件，不读未来低点，不回填旧缺T0证据的episode。D1缓冲破坏与D2无缓冲事件失效分开，新设计不冒充旧批准合同或效果证明。

状态分maturity(NONE/SEED/PREWATCH/WARM/CONFIRMED)、health、validity、tracking、scenario。硬失效优先，必需未知保留last_known+STALE但不作今日资格；上升当日生效、下降按连续可评估会话及hysteresis；未知/停牌断连续，不增长expiry。旧某锚坏只影响关联episode，重入新episode含parent，旧到期工作继续。

Expiry继承10个可评估会话无升级/改善门槛才退出对应Seed/PREWATCH，市场年龄另显示；退出当天不新建重入。模型边界与普通状态变化分开，事件diff/Why Now/Waiting For/Invalid If/反证可追至谓词。

## S12 Focus、独立Cohort与Forward

Focus是研究跟踪，不是持仓，也不是验证样本全集。独立Cohort按冻结规则留全体MATCH/NO_MATCH/UNKNOWN及未入组分母，不能只留Top N/手工关注/后来赢家。

为消除排序空白，提出新项目`FOCUS_RECENCY_ORDER_R1`：默认按最后已验证跟踪事件交易日降序，再entity_id/episode_id稳定排序；风险/有效性为显式过滤/分组，手动置顶作为独立用户视图，不改研究Priority/Cohort。这个中性可解释方案是本次新设计提案，非旧合同追认；如用户目标需要确认强度排序，实施前另定版本，不临场改权重。

观察身份含T0、Lane H/L/late、rule/version、entity、capture revision、source digest；同revision异内容拒绝，合法变化追加，高revision查询不改first。hook走真实分Lane分组/发布路径，覆盖缺import、重复lane字段、混合实体与部分成功重试。

Schedule与outcome分层：PENDING是计划/观测状态，不能以同source+key永久挡住到期DUE结果。结算key=observation/horizon/settlement_revision与明确evaluation identity，不能简单加运行时间逃避幂等。仅用接受的后继来源追加，不改T0；T+1/3/5/10/20按交易会话，不到期/停牌/缺失/右删失各有原因。

**端点与路径质量独立**：T0/终点价格和坐标有效则绝对终点收益可算，内部缺价只限制MFE/MAE/MDD；不能整份清空。板块自身固定篮子生成合法聚合identity，日线仅输出MFE_CLOSE/MAE_CLOSE，不把篮子close冒充high/low。

固定T0成员/原权重，不因缺终值删成员后重权；已证停牌估值保留quote_age/换基且单列MARKED_ESTIMATE；真实缺失、身份未知、无终值退市不填0。实际端点、估值端点、部分不可估值和全路径质量分开。matched controls T0冻结，后来入选保留ITT，事后clean子集不作主要因果结论。

读取同一请求内count/页/成熟数/settlement用只读一致快照；水位方案必须覆盖低ordinal迟提交、高revision替换和并发读写。基础publication与Forward_read_revision组合，不强迫每次结算重发行情。真实LIVE首捕就绪即开始，不能等FEP/UI全写完错过窗口；成熟度等待真实未来，不用历史补跑凑天数。

## S13 FEP可选条件统计与模型

完整保留观察/feature/target/label版本、dataset分母、预测slot、模型/校准/OOD、支持、部署与撤销的设计；仅在实施对应能力时建实际最小存储，不先铺空平台。

ENTRY与DAILY_LANDMARK分开；所有候选今日排序使用当天feature/预测，不能长期挂入选日预测。dataset按fold/partition/FIT-TUNE-CALIBRATION-OUTER_TEST各自cutoff选择当时可见label revision，后修标签不回流早折。按市场日期组切分，episode/重叠标签/实际可得时间purge，预处理只fit训练内；不能随机CV、测试后改阈值或洗掉失败试验。

先确定条件统计baseline；base分区不跨实体类型/scope/target/horizon/variant/来源/质量混池；backoff事先固定，不选最好收益桶。桶内按日期平权，分位定义明确，支持看日期/blocks/entities/episodes/classes，不靠一天数千行制造独立样本。描述频率不称个股未来概率。

模型、校准、OOD/coherence和missingness分别输出；参数和实验预算在看外测前冻结。收益/风险同屏，MFE/MAE符号及跨horizon可比性先验明确；不静默裁剪改善分数。scope/target/horizon/feature/model/capability精确准入，描述、模型显示、Shadow、Priority分别验。FEP失败恢复Priority V1，不影响Core；不接自动交易。

## S14 产品范围排除与内部业务事件

新闻、资讯采集、外部事件催化、社会事件研究及其公司/题材映射全部排除，不建采集任务、数据库表、API、菜单或可开启占位模块。DA-EVENT研究规范及旧EVT-01至EVT-06不作为Stock A需求或依赖；历史参考原件保留只读，不重新执行。

保留行情研究中的行业/概念目录、成员关系与Overlap；保留算法内部突破、回踩、失效、状态变化、Focus跟踪事件及事务outbox。这些内部事件必须追到确定输入/规则，不生成新闻叙事或催化评分。第五入口固定为“市场研究”。FEP和分钟的独立可选边界按S13及来源合同执行。

## S15 日更、原子发布与恢复

候选PG封存后，开发验证通过显式候选token读取同一manifest，只有验证路径可访问；普通用户默认context只解析已接受Head。候选读回不使其成为正式版本。所有必要验证完成后受控CAS接受，再读回普通context与last_good，避免“先发布才能测试、未测试先全局可见”的循环。

运行按完整闭环合同P00–P25及机器依赖清单执行，覆盖来源、基础事实、全部研究域、Owner/Manifest、ReadIndex、候选API、CAS/outbox、接受后观察/到期任务/读回及前端确认。SOURCE_INGRESS、FOUNDATION_EOD、FULL_RESEARCH_EOD、FULL_CHAIN_DISPLAYED分别报告，基础范围不得代完整产品。后台全部就绪但实际前端未确认时明确“后台业务链已就绪，前端显示待确认”；只有全部必需域与后继任务具名完成、同context读回及当前前端显示ACK成立，通用day_chain_complete才为true。单次PG提交、某Owner或某ReadIndex成功永远不能代替。工程错误/未实现不能伪装真实缺源UNKNOWN或未来未到期。真实首错、未跑域、T0/source/parameter/producer及last_good持久化。

无修订只算新增日及必要历史读取，source/adjustment/algorithm/member变化先识别影响范围，普通APPEND拒绝RANGE_REPAIR_REQUIRED。没有自动修订引擎不无条件阻止经验证无修订日；有修订则不能吞掉异常发布。

G1同源同坐标/集合/参数的值、状态、事件语义与独立全量参考零不明差异；G2旧已接受身份/引用守恒；G3新身份/摘要/来源可重算且语义匹配唯一；G4当前候选真实PG、CAS竞争一胜、重复调用、写/提交前后/读回故障、进程中断恢复。全重播新ID不强求等于旧正式ID，不能因此跳过结构字段。

同key异请求幂等冲突；同输入retry逐行/来源/版本/日期/计数一致才复用seal，坏/重复/缺/未来行拒绝。CAS、publication receipt与outbox同事务，失败无半可见，recover区分PREPARED/CAS_DONE_READBACK_PENDING/COMMITTED。回滚回指已接受版本，不改历史。

一个workspace应用锁+一个有效worker；进度含阶段、总量/处理量、心跳、等待/计算/PG/发布、错误与恢复点。维护停止与用户取消分开，安全停机保留必要读回服务；没有后台残留/重复调度。

## S16 API、上下文、摘要与错误

bootstrap给context_token、manifest_id、snapshot_date、available_trade_dates和权限摘要；后续query给token/data_date/entity/filter/page。服务端解析/校验全部底层源/成员/坐标/代码/参数/版本，不要求客户端重传整套SHA，也不信自报身份。错误as_of/token拒绝，不fallback最新。

基础context挂Forward读revision、FEP模型/预测和补充source revision；各自变更明确，不跨页漂移。真正服务端分页/筛选/稳定排序，全集无漏重；GET只查现成结果，不做大历史计算/下载/训练/DDL/发布。

稳定业务digest包括值、质量、真实provenance、时间/合同/参数/producer，排除cache_hit/耗时/request_id/临时路径；规范编码拒NaN/Infinity。缓存键覆盖直接/传递源、adapter、参数、token/date/query；合法变化失效，坏SHA不能复用旧结果，缺源失败有界缓存。

原字节SHA与约定LF规范语义SHA分列，不能为换行差异误判内容破坏或放宽真正内容校验。当前repo HEAD与last_loaded_build/实际资源SHA/instance/workspace/PG分别显示；代码push不等服务加载。

value/quality、时间证据、使用权限、工程错误及结算状态正交。已知0/FALSE/NONE、不适用、缺来源、窗口不足、未实现、合同未定、错误、尚未到期分别用中文解释；不能捕获所有异常返回SOURCE_INCOMPLETE。保留完整分母/全部原因，互斥主状态用于统计。

## S17 六入口、图表与用户解释

| 入口 | 必须交付 |
|---|---|
| 今日总览 | 环境各轴/允许复合、今日轮动、板块与独立个股新变化、三族风险；先回答发生什么/为什么/待验证，不堆digest |
| 板块研究 | 全行业/概念目录、筛选分页、生命周期/健康/轮动时间线、扩散/留存/Overlap、全部成员/风险、来源覆盖 |
| 个股研究 | 任意有效代码/名称搜索、全市场Profile、入选/未入选/Near-Miss谓词、日周月K、MA/锚点、事实/规则解释/研究假设、板块/Focus |
| 关注跟踪 | episode、事件、Anchor/observation、退出/失效/重入、等待与失效条件，独立显示跟踪与研究排序 |
| 市场研究 | 类型化指数、宽度/参与/压力、涨跌停/梯队/连板可判定性、各轴风险；不造日线无法知道的分钟信息 |
| 数据与诊断 | 源/时钟/覆盖、合同/参数、任务阶段/首错、版本与PG/服务身份、恢复和积压；诊断折叠可查 |

公共：完整分页、服务端过滤排序、历史回放、跨日/跨对象比较、全站zh-CN、同context来源追踪。首页显示cap只控负荷，不控底层样本；独立股票去重并保留also-in-sectors，主题簇不改原板块事实。

同token PG→投影→API全集逐域对账；UI预先固定多类别/多日期代表操作，六入口/详情/图表/历史/搜索/排序/分页与中文实际浏览器验。截图、DOM、Node语法、HTTP200、进程内模块各自注明证据层，不互相代替。

## S18 Windows交付与运行

一个安装器、完整程序目录、主EXE、轻量托盘、现有本机网页。程序含所需运行时/依赖/资源/合同，不要求系统Python或日常管理员；不把行情库、Git、测试树与全部证据打进EXE。

资源根/持久工作区/源码/临时分离；托盘可启动/安全停引擎、重启、暂停/恢复日更、打开网页/日志、登录自启与退出。应用活性、调度、数据新鲜度、能力准入分别显示，错误包占端口不能当正常服务。

冻结任务白名单在UI/调度初始化前分派；sys.executable不是通用Python，multiprocessing如用则freeze_support。单例/worker锁、CAS后读回、安全退出/崩溃再启与last_good都在实际包上验。缺动态资源/源码原件显式失败，不fallback另一环境。

安装读回build/source/resource/instance/workspace/schema/DB身份；卸载默认保留数据/证据。最终包及28765正式启用须完整适用验收与用户明确指令；开发构建/28766测试不冒充最终发布。

## S19 测试、性能与资源验收

所有测试源文件、测试脚本/配置、fixture/Oracle与回归向量统一放`G:/codex_tmp/test_temp/Stock A/`，不提交Git；阶段测试结果与回执写交付根过程资料。测试按域组合：纯算法独立Oracle/边界；真实PG迁移/约束/并发；CLI干净子进程；日更真实数据；API全集；真实浏览器；实际安装/恢复。默认suite可收集，旧退役node显式登记，skip/依赖缺失/CI未跑不算PASS；不无限复制轮次fixture。

真实PG在合法单库固定表的限定测试版本/合成身份及有界TEMP中执行；读RO、一致快照RR，提交型故障保留精确范围和保护正式consumer。测试不得偷偷启动旧真实数据库恢复入口或写TDX；fixture先验证根/版本/任务范围。

每个复用旧缺陷有错误旧样本、正常样本、边界样本；生产函数不生成期待。AST结构+独立数值执行、参数等号、UNKNOWN/枚举、重复身份、价格域/单位、坐标/时间、首捕/到期、PENDING→DUE、迟提交/高revision、seal retry、bad SHA、错token、旧进程、schema恒定、体积复用与严格扫描都必须覆盖。

候选工程预算：单新增交易日纯计算/PG/发布≤15分钟，热列表P95≤2秒、真实worker中3并发读P95≤3秒、冷首页/图表≤8秒、首页payload≤512KiB。实施M0用实际硬件、全市场规模、独立冷样本数、错误/超时定义冻结；这些是设计预算，不是已有证据或市场规律。源等待和用户总耗时另报，不可排除后掩盖失败。

记录CPU/RAM峰值、PG数据/索引/TOAST/WAL/G盘增长、恢复耗时，区分定位IO与大对象解压/传输/解析。单次冷样本不估P95，超时/错误列失败率，不只算幸存请求。优化须同值/质量/身份对账，不以缩市场或缓存旧错误达标。

严格扫描以真实活动入口、动态import、当前static allowlist/UI资源、浏览器路由及可达测试建闭包；维护非活动另列，精确处理CSS/URL/日历/法规误报，不给目录/整SHA豁免。prepare任何写入前门未过不出可误认正式的包。

## S20 实施、关闭标准与不重复旧路

M0骨架：当前唯一实现/固定schema/typed存储/动态上下文/来源保护/日志容量/测试安全/严格入口/单CLI先成立；原件策略和初始规模可测。不得先建模型平台/巨型审计容器。

M1真实0→1：一个真实完整交易日的声明基础字段、全市场身份/质量/独立数值、PG/API/页面及一次失败last_good形成可复核样本；仅SourceIngress不算。

M2持续闭环：第二真实日正常APPEND、同输入幂等、修订拒绝或有界修复、G1–G4、恢复、真实worker读性能与空间不异常增长；旧身份/当前版本正确。

M3完整研究：按依赖完成Seed/板块/市场/Priority/D0/D1/D2/Focus/独立Forward与六入口；封口/D2/Focus排序提案在对应实施前完成明确采用裁定，新实现不带旧未定义坑。完整产品所有声明能力真实验，缺源/未来成熟仍精确披露。

M4交付与持续实证：实际安装包、运行身份、生产授权/切换、连续真实日稳定与Forward自然成熟；FEP按独立能力阶段接入。首捕在其源/规则就绪时先行，不等M4才开始积累。

外部过程资料中的ISSUES从V4全部问题台账导入可适用问题的ID/防线/回归/验收责任，执行台账和测试证据不提交Git；禁止只复制叙述不落实机器检查。结构问题必须在M0/M1关闭，算法/结算问题在对应模块准入前关闭；历史不可补证不能靠代码填造。文档完整不等于未来不再发现未知问题，任何新增问题按同一台账与影响域处理，不能再靠新建平行系统逃避。

## S21 配置与决策清单

启动配置必须明确：用户批准源码/产物/源根、PG逻辑身份/实际合法凭据加载、单schema、固定端口角色、日历/参考目录、当前数据集/算法/参数/成员/价格合同、请求及资源预算、保留/备份/恢复、已启用能力。缺少必需项拒绝对应操作，不默取旧V4路径/日期/证券。

PG默认G盘为用户确定约束，不属于候选资源预算；新写入/发布前连接验证`SHOW data_directory`、日志/WAL/表空间和服务参数。当前E盘实例可读只证明旧数据仍可访问，不计作新项目G盘存储通过；本轮没有声称已改变运行服务。

确定继承：三窗口、RPS中秩、amr分母排除当前、breadth主式、Core/Supplemental隔离、单PG/schema、完整安装路线与FEP方法。新项目提案：SOURCE_SEAL_R1、PURE_CORE_SECTOR_QUALIFICATION_R2、D0_SCENARIO_FACTS_R2、FROZEN_SCENARIO_INVALIDATION_R2、FOCUS_RECENCY_ORDER_R1、ROTATION_RESEARCH_DEFAULTS_R1及候选资源预算，须明确采用后实施，不伪装为旧合同已批准。

真实来源缺口、缺历史首冻、未来到期、外部访问能力无法由本规格制造；应在运行范围与完成谓词中如实反映。用户已授权在Stock A目录放置根规范和Git准入；业务编码、旧代码清理、旧库迁移/复用、服务停止/切换仍按实际后续任务，不能从文档放置推定已完成实施。

## S22 领域合同落地清单与变更边界

本规格是完整产品和工程主规格，领域级大量AST/状态向量应在同一现行合同库表达，不把旧数百份任务文档全部搬成新的活动规范。M0必须生成下列合同目录和schema，M1起只为实际实施字段填充；缺少一项必需表达就不得发布对应能力。

| 合同域 | 最低完整内容 | 对应实施前的关闭条件 |
|---|---|---|
| 字段与Profile | 字段DAG、公式/AST、窗口/坐标/时钟、全部枚举分支、等号、质量、单位与数值向量 | 不允许“已有规则”等未解析引用进入可执行注册表；独立解释器一致 |
| Seed/板块/Rotation | 资格真值表、WARM/CONFIRMED路径、membership版本、LOO、固定篮子、retention/hysteresis全部参数 | 每参数采用状态/默认/来源明确，空参数禁用其能力而非填猜测 |
| Priority/风险 | 各桶边界、有序键、字段必需性、FALSE/UNKNOWN支配、风险主因及全量原因 | 全集独立排序与未知向量；显示截断不改变结果 |
| D0/Anchor/状态 | 每场景原始谓词、来源封口、前态/锚选择、四场景适用性、失效与连续计数 | 采用封口与D2明确提案，缺冻结价不得生成可评估episode |
| Cohort/Forward/FEP | 采样总体、Lane/时钟、horizon、due/terminal/path、固定篮子/对照、标签修订与fold | 首捕/迟采/历史分离；PENDING→DUE及各horizon单独证据 |
| 服务/交付 | API字段/分页/错误、manifest token、缓存来源闭包、包资源与启动恢复 | 真PG→实际worker API→真实浏览器→实际包读回，禁止源码结果代验 |

继承参考中的阈值/候选不能仅凭旧文件标FROZEN_CANDIDATE就自动变正式。本轮补齐了方向、关键公式和防复发条件；仍需领域采用决策的参数、封口/D2/Focus新提案与真实来源证据均明确保留，不宣称已经完成实施或外部验收。完整规格应让未闭合项可识别和可执行关闭，而不是隐去它们。

## S23 全部136项现行产品需求的现行承接条款

本节保留136项现行产品需求；按用户明确指令撤销六项EVT新闻需求，不重编号其余稳定ID。历史142项仅是旧版参考，不是当前范围。按S01–S22落实；新项目明确方案替代旧项目路径/未决措辞，不承接旧运行状态或授权。旧范式中的Owner/ReadIndex是逻辑职责，不要求重建同名巨型容器。领域详细表达按唯一现行合同注册表执行，不直接执行旧任务卡。

| ID | 新项目要求 | 主要规格位置 |
|---|---|---|
| OBJ-01 | 支持全市场每日状态理解、变化发现、可解释研究、持续关注、历史回放和前瞻验证，形成“市场 → 板块 → 个股 → 结构/状态 → Focus → Forward”的完整流程。 | S01/S17/S20 |
| OBJ-02 | 任意当日有效证券均有 Daily Profile 和未入选解释；画像范围不由筛选结果、榜单、Focus 或少量样例决定。 | S01/S17/S20 |
| OBJ-03 | 事实、规则解释(rule_explanation)与研究假设分层；全部来源、数值计算与研究者判断可分别追溯，假设不得冒充已证事实。 | S01/S17/S20 |
| OBJ-04 | 已具备真实输入、工程准入与有效研究发布政策的能力可正式研究使用；长期样本尚未成熟只限制对应效果论证或模型能力。 | S01/S17/S20 |
| OBJ-05 | 研究优先级不等于买入优先级；不输出自动交易，不以历史重建、观察频率、研究评分或工程测试宣称个股上涨概率或收益保证。 | S01/S17/S20 |
| OBJ-06 | 所有能力按域、对象、时间、规则版本、证据来源及消费者管理；一个域缺源时，保留独立且可验证的其他域。 | S01/S17/S20 |
| OBJ-07 | 必须真正交付六入口、详情、筛选、分页、历史与比较；摘要区、HTTP 200、静态页面或夹具演示不能代表全产品交付。 | S01/S17/S20 |
| SRC-01 | 所有配置的 TDX 根目录只读，涵盖主进程、子进程、解压、日志、缓存、GBBQ、维护工具与路径别名；写入前校验解析后的最终路径，配置缺失时关闭写入。 | S03/S04/S07 |
| SRC-02 | 扫描器工作开始前完成阶段 0，明确 `FULL_PASS / DEGRADED_PASS / BLOCKED`、适用源、价格能力及限制；不能仅引用旧项目某次通过。 | S03/S04/S07 |
| SRC-03 | 本地 TDX 日线与合法公开历史包按版本化权威矩阵使用；不引入已排除的 TDX 量化接口，也不以外部复权结果替代本地复权内核。 | S03/S04/S07 |
| SRC-04 | 历史初始化以不少于原合同所需窗口的能力为目标；两年是初始化目标，不是自动删除较早原件的保留上限。缺历史按对象/窗口降级。 | S03/S04/S07 |
| SRC-05 | 每个包登记来源、原始字节摘要、大小、下载起止、完整下载后可用时间、观察时间、覆盖范围、解析器版本、重叠核验和接受结果。包标题日期不等于已可用时间。 | S03/S04/S07 |
| SRC-06 | 下载、重试、响应大小、并发、请求预算和等待均有界；先验证包、路径、内容和重叠，后进入 SourceIngress；缓存不得借用早于实际完整可用的时钟。 | S03/S04/S07 |
| SRC-07 | SourceIngress 只表示原始来源已写入并读回；回执必须单列 `completion_scope`、业务链各阶段和失败原因，不能报告为整日计算/发布完成。 | S03/S04/S07 |
| SRC-08 | BaoStock 只承担经合同核实的字段与身份/状态补充；`BOUND_STRICT / BOUND_SOFT / UNAVAILABLE` 分明，只有准入质量进入对应因子。失败不无条件阻塞独立 Pure Core。 | S03/S04/S07 |
| SRC-09 | 跨源 OHLCV、amount、单位及身份逐字段核对。金额差异独立立项，保留 TDX 原值与冲突证据，不平均、不静默替换、不以其它字段相等推定金额正确。 | S03/S04/S07 |
| SRC-10 | 行情、日历、身份、制度和成员等获准数据集须逐项具备来源合同、版本、能力门、可审计时钟及有界请求；可选补充失败只影响其真实消费者，不能堵独立Core。 | S03/S04/S07 |
| SRC-11 | 精确原始来源、冻结引用、历史身份和不可再生证据可恢复；来源缺失不能用其派生结果反向证明。所有产物在源根之外同卷原子落盘。 | S03/S04/S07 |
| ID-01 | 稳定证券身份与交易代码分离，支持沪深北、各有效板块、新股、代码变更、更名、退市及生命周期；交易代码首次出现不能直接解释为 IPO 日。 | S04/S16 |
| ID-02 | 每个交易日绑定有效 Data Universe、Research Universe、交易状态及排除理由；日期身份优先于宽泛供应方名录，不能被当前名单覆盖。 | S04/S16 |
| ID-03 | 没有足够身份来源的成员隔离为 `UNMAPPED_QUARANTINED`，保留原始记录；不按代码形状、名称猜测或后来上市事实倒填旧身份。 | S04/S16 |
| ID-04 | 日历、停复牌、ST、价格制度按生效时间及知识时间选择；官方交易日与自然日区分，日历超出来源范围时拒绝，不允许数组负索引环绕。 | S04/S16 |
| ID-05 | 板块目录、行业层级、概念及成员关系来自本次绑定来源。保存成员加入/退出、快照、覆盖、有效期和观察时间，历史比较显式定义共同集合。 | S04/S16 |
| ID-06 | 当前最新 TDX 映射可在明确声明的回溯研究模式使用，必须标记非 PIT、快照时间和 mapped/unmapped 数量；不能称完整历史成员关系。 | S04/S16 |
| ID-07 | PIT 同时验证 valid time、known/available time、实际消费版本和 cutoff；晚采集、历史重建、当前成员回溯分别标记，不由路径名或布尔标志升级。 | S04/S16 |
| ID-08 | 所有 API、计算、图表、维护脚本按传入交易日/接受上下文运行；历史查询不得返回最新一天的数据或趋势，缺少精确日期来源时关闭相应能力。 | S04/S16 |
| PX-01 | 原始 OHLCV/amount 与 adjusted daily 分层；本地 TDX 公司行为及仿射复权有完整合同、版本、来源与可重复数值证据。外部服务只可在已准许范围作验证参考。 | S07/S08 |
| PX-02 | 每个收益、均线、ATR、前高低、Anchor、冻结失效价及 Forward 路径注明 price basis、调整版本与目标坐标；跨日不能直接混算各自 QFQ 坐标。 | S07/S08 |
| PX-03 | 复权截断只消费当时可见公司行为；缺公司行为或换基证据时按能力降级，不借未来事件修正 T0 判断。 | S07/S08 |
| PX-04 | 日/周/月均从 Canonical Daily 派生；区分已闭合周期与 as-of 未闭合周期，显式 cutoff，截断未来行，覆盖跨周、跨月、停牌与日历超前反例。 | S07/S08 |
| PX-05 | 严格区分实际交易 bar、已证实停牌、真实缺口、历史不足、无当前 bar、身份不足。窗口同时提供日历跨度、有效观察数、起止与缺口证据。 | S07/S08 |
| PX-06 | `amount_ratio20` 等严格交易 bar 窗口必须证明当前 bar 与前序实际交易 bar 完整；末若干可用 bar 算术相等不能替代缺日状态证明，未知停牌不能当零或自动跳过。 | S07/S08 |
| PX-07 | 价格限制引擎按当日制度、板块、ST、新上市特殊期、前收及精度生成；缺制度/状态/receipt 不能出确定涨跌停。结果保存适用规则与可判定覆盖。 | S07/S08 |
| PX-08 | 只使用日线时不捏造分钟封板、开板顺序或成员同步盘中极值；成交量单位未核实时关闭相关展示。 | S07/S08 |
| ALG-01 | 每个字段登记 producer、合同版本、定义/AST、参数、单位、取值域、窗口、time role、来源、价格坐标、质量和全部实际消费者。不能只有说明性字段名。 | S08/S10/S11/S22 |
| ALG-02 | 核心原语覆盖收益、MA、ATR、波动、位置、压缩、RPS、跨日 RPS 变化、相对市场、量额参与、extension 与价格损伤；具体表达见S08与唯一现行字段注册表；继承原合同不得省略逐字段数值验证。 | S08/S10/S11/S22 |
| ALG-03 | Profile 覆盖全体当日有效身份，分 Core、Participation、Sector Context、Advanced Structure、Research Projection；筛选结果不反向决定画像存在与否。 | S08/S10/S11/S22 |
| ALG-04 | Pure Core、Supplemental、带板块上下文的 variant 分版本，换手补充不偷入核心硬资格/排序；未闭合的 Amount A 不进入正式 Core。 | S08/S10/S11/S22 |
| ALG-05 | RPS 每个端点的收益与排名集合须显式绑定；跨日 delta 在同一目标价格坐标下重算端点 rank，不能把不同日期各自坐标的排名直接相减。保存两端 universe 与同分规则。 | S08/S10/S11/S22 |
| ALG-06 | 相对市场收益绑定起始日有效成员及可评估收益，明确等权/固定篮子政策；不能拿目标日或宽泛名录替换起始集合。市场状态参考与 Forward 基准分开。 | S08/S10/S11/S22 |
| ALG-07 | 非有限值、bool 冒充数值、非正价格、坏质量、缺字段、错日期/坐标必须可解释拒绝。禁止 epsilon 遮蔽无效分母、缺价格回填成前低或将缺项默认中性。 | S08/S10/S11/S22 |
| ALG-08 | 三态 TRUE/FALSE/UNKNOWN、NONE、NOT_APPLICABLE、NOT_IMPLEMENTED、SOURCE_INCOMPLETE、NOT_AUTHORIZED 含义分开；每个 reducer 明确优先级，不能跨模块套用同一未知规则。 | S08/S10/S11/S22 |
| ALG-09 | 每个规则保留全部谓词、原始原因及互斥主原因。全体=入选+规则排除+输入/合同不可评估等互斥分类；理由全集允许重叠，汇总不能重复计数。 | S08/S10/S11/S22 |
| ALG-10 | 参数注册工程候选、批准、退役及生效范围；未经证据冻结的阈值不得包装成市场规律。经济含义变化必须升版，旧算法与结果保留。 | S08/S10/S11/S22 |
| DAG-01 | Seed 不读最终板块/个股资格、Radar、Focus；Rotation 不读当日最终成熟度；C 不回灌 B；UI、补充来源、Forward 标签与 FEP 结果不回灌当日资格。 | S02/S09/S11 |
| DAG-02 | 新 Anchor 当日只成为新事件，最早后继会话用于相应路径判断；LOO 按排除目标后的真实成员与聚合重算，禁止只删显示行或用最终结果反推。 | S02/S09/S11 |
| SEC-01 | 板块行业/概念类型、成员规模、报价覆盖、RS、宽度、Seed 扩散、强势/Seed 留存、参与代理与集中度分别可解释。未知成员结果不当作退出。 | S09/S11 |
| SEC-02 | 跨日宽度和变化使用合同指定共同成员及两端可评估集合；成员变更单列。少成员/低覆盖按门降级，小样本惩罚只作排序启发，不作上涨概率。 | S09/S11 |
| SEC-03 | 维护 PREWATCH/WARM/CONFIRMED 原始资格与生命周期、健康度、轮动三轴；NONE 表示已评估无匹配，UNKNOWN 表示证据不足。缺比较输入不能归 NONE。 | S09/S11 |
| SEC-04 | 轮动状态、pulse、retention、恢复/扩散/再加速及时间线使用冻结来源；板块重叠诊断、主题去重和展示分散保留，不以简单 1/N 稀释成员贡献。 | S09/S11 |
| SEC-05 | 个股主行业、支持概念及算法支持板块分别保存；历史关系不充分时显式降级，不让缺板块上下文的独立个股画像消失。 | S09/S11 |
| MKT-01 | 市场趋势、宽度、参与度、压力等级及压力变化分别保存数值、分母、覆盖、来源、质量与权限；四轴中压力含等级/变化两个分量。 | S10 |
| MKT-02 | 宽度主式承接最新裁定 `MEAN(I(r_t>0)-I(r_t_minus_3>0))`，零收益映射为 0；SIGN 差仅为诊断。两者量纲不同，不能按分类恰好相同认定数值等价。 | S10 |
| MKT-03 | 比较分母取精确日期及市场范围的绑定历史身份；Core 仅在身份集合完全相等时作加速，不能只比行数；缺历史日期集合时不得假定为当前全集。 | S10 |
| MKT-04 | 趋势使用带类型、角色、版本、price basis、源成员和 SHA 的参考目录，校验本次 publication 日期绑定；目录存在不等于参考路径已获当前使用资格。 | S10 |
| MKT-05 | 市场分轴降级：缺限价不妨碍独立宽度/参与度，缺前日限价只影响压力变化；共享核心身份/摘要损坏按相应依赖整体拒绝。复合环境另按有效发布政策判定。 | S10 |
| MKT-06 | 股票、板块、市场三族风险分别物化，有各自总体、已知、未知、覆盖和变化；首观测与从前态变化分开，不能用 Focus 行数代替全市场分母。 | S10 |
| PRI-01 | 研究排序与事件变化排序分开；股票和板块 Priority 分别登记资格、输入完整性、评分轴、风险、bucket、截断、同分 tie-break 和互斥排除。 | S10/S12 |
| PRI-02 | 必需输入必须存在且有限；例如 rps20 即使不直接加分仍可为完整性前提。保留已获准的精确价格坐标质量，不误判合法 TARGET_COORDINATE。 | S10/S12 |
| PRI-03 | 排序规则的 FALSE/UNKNOWN 支配关系由本规则合同决定；保留全部失败谓词。市场上下文未获使用资格时不能悄悄改变排序权重。 | S10/S12 |
| PRI-04 | Focus Priority 作为独立子能力，需要明确业务目标、字段、比较方向、时间语义与稳定排序；普通 Focus 可读不等于该排序已定义。 | S10/S12 |
| ST-01 | D0 场景逐项登记来源封口、真实 producer、available_at/cutoff、time role、权限、谓词及三态。`sealed=true` 不能代替真实绑定链。 | S11 |
| ST-02 | LAUNCH_CONFIRM、RECOVERY_TURN 与 STRONG_PULLBACK、TREND_CONTINUE 分别按现行合同的正式/诊断角色处理；诊断 UNKNOWN 不污染仅获准场景的正式聚合。 | S11 |
| ST-03 | D1 支持 breakout episode、pullback、recovery、bullish impulse、支持/接受与 retention；Anchor 类型、选择、多锚冲突、换基、失效、活跃选择器和冻结时点均须定义。 | S11 |
| ST-04 | D2 冻结失效价须先具备生成规则、适用 episode、所需窗口、参数、冻结时刻、生效日及价格坐标；比较器 `close <= frozen_level` 不构成生成规则。 | S11 |
| ST-05 | 不从当前低点、后来的最优锚或 Focus 结果反推历史入场冻结价；无 T0 证据的旧 episode 只能按明确非 PIT 重建方案研究，不能伪造历史首冻。 | S11 |
| ST-06 | maturity、health、validity、tracking、scenario 分轴；硬失效只作用关联 episode，优先于同日确认，不把某个旧坏锚解释为证券永久失效。 | S11 |
| ST-07 | 未知保留 last_known 及 stale 标记，不当今日资格；升降级、连续可评估会话、expiry、停牌/未知断点、退出/重入与 parent episode 都有确定规则。 | S11 |
| ST-08 | 事件差异、Radar 与 Near-Miss 可回到 AST 失败谓词；Why Now、Waiting For、Invalid If、冲突与反证为一等输出，不能只给标签或总分。 | S11 |
| FWD-01 | Focus 为用户研究跟踪；独立 Validation Cohort 按冻结全量规则建样本，不能只从 UI Top N、手工关注或后来表现好的对象入组。 | S12 |
| FWD-02 | Focus 保留 episode 延续、Anchor、observation、状态、退出/失效、重入、后继任务及历史队列；旧 episode 与新 episode 不按证券身份合并。 | S12 |
| FWD-03 | 历史 Lane H、实时 Lane L、late capture 分别标识；完整总体保留 MATCH/NO_MATCH/UNKNOWN，真实 capture clock 不能回写为事件日或下载包日期。 | S12 |
| FWD-04 | observation 身份包含 T0、Lane、规则版本、entity、capture revision、来源摘要；同版本异内容拒绝，合法修订追加；不同 Lane 可共存。 | S12 |
| FWD-05 | accepted Head 后的 observer 只消费该 Head 及绑定来源，后继到期/未决结算可重试追加，不改 T0。T+N 使用接受的交易会话，未到期 PENDING，截尾 RIGHT_CENSORED。 | S12 |
| FWD-06 | settlement 按 observation、horizon、revision 绑定；同观察最新合法修订读取，历史版本保留。不能跨对象取 latest、靠最大 ordinal 忽略迟提交低序号。 | S12 |
| FWD-07 | count、分页、成熟数及 settlement 在同一请求内使用一致的只读事务快照；缓存水位覆盖迟提交和高 revision 替换，实测并发读。 | S12 |
| FWD-08 | 绝对收益、MFE/MAE/MDD、结构首事件、相对市场/板块、matched controls 分别定义；固定 T0 篮子和原权重，不因缺终值删成员后重分权。 | S12 |
| FWD-09 | 确认停牌可按合同提供明确标识估值，保留 quote age 和换基证据；真实缺失、身份未知、退市无终值不填零。OBSERVED、MARKED_ESTIMATE、PARTIAL_UNVALUED 分开。 | S12 |
| FWD-10 | 日线板块路径只按可验证收盘形成 MFE_CLOSE/MAE_CLOSE，不累加各成员 high 冒充同步高点；未复权名义收益明确披露公司行为限制。 | S12 |
| FWD-11 | 对照 T0 冻结并保留后来转为信号的 ITT 样本；事后 clean-control 只作敏感性分析，不声称因果。历史成熟样本不替代 LIVE_OOS 首捕和真实未来观察。 | S12 |
| FEP-01 | 观察、feature、target、label revision、expected-target denominator、预测 slot、模型选择、实际预测和评分时间分别记录。ENTRY 与 DAILY_LANDMARK 分开，不用入选日预测冒充今日全池预测。 | S13 |
| FEP-02 | dataset 按 fold/partition/phase 截止选择当时可见 label revision；未来修订不可回流早折。FIT/TUNE/CALIBRATION/OUTER_TEST 按市场日期分组、实际标签结束及可得时间 purge。 | S13 |
| FEP-03 | 先确定条件统计 baseline；base partition 不跨实体类型、scope、target、horizon、variant、来源和质量政策混池；backoff 事先固定，不挑收益最好桶。 | S13 |
| FEP-04 | 桶内重新按日期平权，保留权重摘要；support 同时评估日期、blocks、实体、episode、正负/各类数量与缺失代表性，不能用一天数千行制造独立支持。 | S13 |
| FEP-05 | 模型、变换、校准、OOD reference 只 fit 合规训练段；失败试验留存，已用于调参的测试/Shadow 窗口不再当独立晋级证据。 | S13 |
| FEP-06 | support、calibration、OOD、coherence、missingness 分列，分位数与置信区间不混淆。未获相应合同和证据时不输出概率性宣称。 | S13 |
| FEP-07 | 权限精确到 scope/target/horizon/feature/model/capability；描述、模型展示、Shadow、Priority 各自准入。FEP 失效恢复 Priority V1，不影响 Core。 | S13 |
| FEP-08 | 模型 artifact 本地可信、校验摘要与依赖；部署 head 受控 CAS、幂等及撤销保留历史。API 只读，不在 GET 内训练、下载或重算。 | S13 |
| UI-01 | 今日总览 ／ 市场各轴与环境、轮动、板块/独立个股变化、三族风险、研究入口；优先回答当日变化、原因和待验证条件，不能用成员数/收盘价代替算法结果。 | S16/S17 |
| UI-02 | 板块研究 ／ 全行业/概念目录、筛选分页、生命周期、轮动与因子时间线、成员扩散/留存、Overlap、完整成员及风险；披露关系快照覆盖。 | S16/S17 |
| UI-03 | 个股研究 ／ 任意代码/名称搜索、全市场画像、入选/未入选/近失原因、日周月K线、结构锚点、事实/规则解释/研究假设、板块上下文、Focus 与历史来源。 | S16/S17 |
| UI-04 | 关注跟踪 ／ 列表、episode 生命周期、Anchor/Observation、事件、退出重入、待验证与失效；采用S12明确Focus默认排序方案并独立验证，不再留下无规则排序。 | S16/S17 |
| UI-05 | 市场研究 ／ 指数、宽度、参与、压力、涨跌停/梯队/连板可判定性与风险；标记实际数据能力，日线不造分钟信息。 | S16/S17 |
| UI-06 | 数据与诊断 ／ 来源/时钟/覆盖、字段合同、参数/版本、错误、任务阶段、旧模块归位、队列与消费者依赖追踪；诊断信息不挤占首页主要阅读。 | S16/S17 |
| UI-07 | 公共能力 ／ 服务端筛选、稳定排序、全量分页、详情、历史 PIT/非PIT回放、跨日/跨对象比较；页间无重复、遗漏，比较对象时间和坐标一致。 | S16/S17 |
| UI-08 | 状态解释 ／ 简体中文 zh-CN 默认；技术枚举配中文，空数据、未匹配、输入缺失、未实现、合同未定、未授权、未来待到期分别解释。 | S16/S17 |
| UI-09 | 上下文 ／ bootstrap 提供真实可用日期和 context token；切页/日期/对象不漂移，明确 snapshot date 与 data date，错 token/as_of 拒绝而非回落 latest。 | S16/S17 |
| UI-10 | 结果身份 ／ 可查 source/contract/producer、版本、data cutoff、token、质量、证据和运行包；页面显示的结果必须与同 token PG/API 一致。 | S16/S17 |
| UI-11 | 验收 ／ 六入口和详情在真实浏览器完成操作、图表及中文检查，覆盖原要求的桌面分辨率；静态脚本测试、DOM 抽查、像素截图各自注明范围。 | S16/S17 |
| DB-01 | 默认复用一个 PostgreSQL 实例、固定业务数据库；版本/日期作为显式业务维度，稳定逻辑 schema 及迁移边界，禁止为普通换日/换分支不断复制库或动态创建 schema。 | S05/S06/S16 |
| DB-02 | Owner、Publication、Head、ReadIndex、jobs、Forward/FEP 由登记职责写入；事实/修订追加保存，受控可变指针有明确 CAS。读取不执行 DDL、迁移、下载或业务重算。 | S05/S06/S16 |
| DB-03 | 精确 Owner 读取验证数据库身份、namespace/version、域、日期、token、行数、来源与完整 payload 摘要。不能因另一个空间日期相同而 fallback。 | S05/S06/S16 |
| DB-04 | ReadIndex 是派生投影，绑定 exact token+data date+对应 Owner；错日、坏摘要、旧/新版本混用拒绝。换 token 不沿用旧 token 的 PG/API/UI 通过。 | S05/S06/S16 |
| DB-05 | 查询使用索引范围和真正 SQL 分页，避免每页全量解压/重算历史；离线 Parquet SQL 路径/cutoff 参数绑定需真实引擎回归，不能只 mock 字符串。 | S05/S06/S16 |
| DB-06 | 稳定业务摘要包含值、质量、合同、真实来源/时点及传递依赖；排除 cache_hit、请求耗时/ID、临时绝对路径。规范编码拒绝 NaN/Infinity。 | S05/S06/S16 |
| DB-07 | 缓存键涵盖 token、日期、域、producer/adapter、参数、输入字节身份和传递消费者；坏 SHA 不可命中旧缓存，缺源失败可按同一精确身份缓存并及时失效。 | S05/S06/S16 |
| DB-08 | LF/CRLF 语义身份和原始字节身份分别保存；只按明确合同进行换行规范化，不能用规范化掩盖实际内容变化或取消原件摘要。 | S05/S06/S16 |
| DB-09 | 成功重试必须验证原不可变快照与全部入参内容相同才复用；坏行、重复、缺行、未来行、异内容同 revision 均拒绝。 | S05/S06/S16 |
| DB-10 | 使用现有安全连接配置，不入文档/日志凭据；读事务 READ ONLY，需一致快照时 REPEATABLE READ；开发写事务列明对象与范围，不绕过真实访问控制。 | S05/S06/S16 |
| RUN-01 | 日更目标从接受上下文、日历及来源事实解析；无修订 APPEND 只算新增会话及必要窗口，不每日重建全历史。按S25的P00–P25依赖图分阶段执行并派生聚合完成，包含真正接受后的观察/到期任务和显示确认，不能只串模块名。 | S15/S18 |
| RUN-02 | 明确 source-ready、derived、PG-staged、candidate-validated、published、consumer-verified；保存 accepted_before/after、真实来源上限、缺失域及 last_good，任一步失败不写“今日更新完成”。 | S15/S18 |
| RUN-03 | 四门独立：G1 业务值/状态等价；G2 旧 Owner/Anchor/Episode 身份守恒；G3 新身份/摘要/引用正确可重算；G4 PG/发布/CAS/失败恢复。重建新 ID 不强求等于旧正式 ID。 | S15/S18 |
| RUN-04 | 修订检测区分来源、复权、算法、成员/身份和参数变化；异常拒绝普通 APPEND，进入版本化有界 RANGE_REPAIR；未完成自动修订引擎不无条件阻塞经证实无修订的正常新增日。 | S15/S18 |
| RUN-05 | 前态、输入、参数与计算版本冻结后生成候选；Head CAS 与 receipt/outbox 事务一致，竞争只有一胜；失败无半成品可见，重试幂等，进程中断后可恢复。 | S15/S18 |
| RUN-06 | 只能一个有效调度器/生产者持有工作区与任务锁；启动前检查现有作业，不重复运行、擅自取消、清空或重置。暂停自动日更与停止引擎分开。 | S15/S18 |
| RUN-07 | 进度包含 task/operation ID、阶段、已处理/总量、来源等待、纯计算、PG、发布、错误与心跳；长任务不阻塞读页面；报耗时须区分下载等待和计算。 | S15/S18 |
| RUN-08 | 发布以确切候选字节及完整依赖生成包，真实运行源码/资源/工作区/PG身份可读回；Git 提交、合并、tag、打包、安装、Head 更新和业务验收分别记状态。 | S15/S18 |
| RUN-09 | 保留 last_good 包与 Head，实际验证安全停机、失败回滚及已接受事实不变；不得启动仍绑定归档路径的旧环境来代表当前版本。 | S15/S18 |
| RUN-10 | 研究运营可用、工程正确、数据真实、长期样本成熟、模型支持和交易权限分开；只因真实未来会话未到而等待，工程缺口不得包装成 WAIT_FUTURE_SESSION。 | S15/S18 |
| OPS-01 | 承接 Windows 完整安装包及轻量托盘设计：普通用户启动、打开工作台、状态、启动/安全停止/重启、暂停日更、日志和自启设置；不要求用户反复打开管理员命令行。 | S18/S19 |
| OPS-02 | 程序资源根、持久数据根、源码根、临时目录分开，运行不写安装资源或 Git 源码。打包依赖白名单含动态模块、资源、合同、必要原件与可达子任务。 | S18/S19 |
| OPS-03 | 冻结子任务在 UI/调度初始化前按登记白名单分派；禁止把打包 EXE 当通用 Python 执行任意脚本，必要 multiprocessing 正确处理；不递归启动托盘或第二个引擎。 | S18/S19 |
| OPS-04 | 应用活性、调度运行、数据新鲜度、能力准入分别显示；HTTP 200 或 READY 不能代替正确版本/PG/日期/业务质量。卸载默认保留研究证据与数据。 | S18/S19 |
| OPS-05 | 先冻结性能预算和样本方法，再验收。覆盖冷启动、冷/热 API P50/P95、首页载荷、全量分页、图表、三并发、真实日更负载、峰值内存/CPU、数据库增长和恢复耗时。 | S18/S19 |
| OPS-06 | 单个冷样本不估 P95，进程内 BFF 不冒充 HTTP/浏览器性能，热缓存基线不冒充新版本。错误样本保留，不选择性剔除失败计算。 | S18/S19 |
| OPS-07 | 大历史文件流式 hash/parse；逐页不反复全扫源码或压缩历史；只在集合/来源身份完全等价时使用加速。优化前后结果逐字段核对。 | S18/S19 |
| OPS-08 | 告警覆盖源新鲜度/预算、身份/成员缺口、候选数量、状态异常、发布失败、Focus/settlement 积压、磁盘/数据库增长和错误版本监听。日志默认中文，错误码可复制。 | S18/S19 |
| ENG-01 | 业务代码、正式脚本、运行配置、查询和页面默认均面向全市场、动态日期；不写正常个股、具体板块或单日专属分支。指数身份等版本化数据事实应由目录解析，不能散落默认选择。 | S02/S19/S20 |
| ENG-02 | 测试可用合成身份/固定日期，必须与生产输出隔离。法规生效日、日历事实、历史证据身份可保留但分类核验；固定历史路径若仍决定当日日更输入就是运行缺陷。 | S02/S19/S20 |
| ENG-03 | 扫描覆盖本项目实际入口、静态资源 allowlist、动态模块、可达测试与真实依赖；非活动旧模块/维护候选单列，不把全仓命中数当 当前运行缺陷数。未解析边/未裁定项不算通过。 | S02/S19/S20 |
| ENG-04 | 文档、页面、帮助、日志、报告及维护者 Git 文案默认简体中文 UTF-8；协议/API/代码标识/供应方枚举保持兼容并配用户解释。 | S02/S19/S20 |
| ENG-05 | 一次性脚本只在G:/codex_tmp执行后删除；所有测试文件/测试脚本/fixture/Oracle在G:/codex_tmp/test_temp/Stock A，不进Git。正式业务CLI归src；阶段工具、报告、回执和产物外置，不能改名混入业务目录。 | S02/S19/S20 |
| ENG-06 | 唯一源码G:/codex work/Stock A；交付G:/codex work/Stock A 交付；临时G:/codex_tmp、测试G:/codex_tmp/test_temp。Git仅允许业务代码/运行必需组成、需求/设计/正式变更及最小根控制文件；所有测试/任务文档/报告/回执/产物禁止提交。 | S02/S19/S20 |
| ENG-07 | 未有用户新指令前沿用生产28765、唯一开发28766、PG5432/market_research的环境限制；后续PG默认数据、WAL、日志、主动备份/导出均为G盘。旧E盘数据保留只读参考，不自动迁移/覆盖；新项目启用前实际核验G盘服务与存储，不抢占端口或另建实例。 | S02/S19/S20 |
| ENG-08 | 开发期限定 DDL/DML/回填/重算/开发 CAS 按最新用户权限及实际访问控制执行；旧任务逐项申请条款不覆盖最新授权；生产 Head、调度和 28765 切换仍独立。 | S02/S19/S20 |
| ENG-09 | 阶段前读最新适用合同；阶段记录输入、证据、验收范围、失败、下一步，证据外置且仅白名单业务代码/正式规范提交推送。跨模块问题在外部独立建账，不能借当前模块测试顺带关闭。 | S02/S19/S20 |

## S24 Stock A目录、Git准入与外部过程材料

本节按用户2026-10-25最新指令生效，替代旧交接中测试入Git、必要小证据提交、任务报告随阶段提交等冲突描述。

| 内容 | 位置 | Git |
|---|---|---|
| 根规则、完整规格、项目入口 | Stock A根AGENTS/PROJECT_SPEC/README | 允许 |
| 业务后端/正式业务CLI | src/ | 允许，必须是实际业务依赖 |
| 产品前端 | web/ | 允许，不含打包输出/node_modules/测试 |
| 无密运行默认、算法/来源合同、配置schema | config/ | 允许，不含数据行/捕获结果/回执 |
| 正式业务迁移 | migrations/ | 允许，不含测试SQL/一次性任务SQL |
| 运行资源 | resources/ | 允许，不含截图/测试样例/证据 |
| 需求、设计、正式需求变更 | docs/requirements、docs/design、docs/changes | 允许，执行记录不能冒充设计变更 |
| 业务依赖与锁定/打包清单 | 根白名单清单文件 | 允许，测试配置/测试依赖单列在外部测试区 |
| 测试源/脚本/fixture/Oracle/测试配置 | G:/codex_tmp/test_temp/Stock A | 禁止 |
| 临时执行器/构建中间物/缓存 | G:/codex_tmp项目子目录 | 禁止，用后清理 |
| 阶段任务/报告/回执/执行台账 | G:/codex work/Stock A 交付/过程资料 | 禁止，包括小摘要/JSON/截图 |
| 原设计/历史复盘参考 | G:/codex work/Stock A 交付/参考资料 | 禁止，不作为当前授权 |
| 安装包/发布文件 | G:/codex work/Stock A 交付/发布包 | 禁止 |
| 非PG运行数据/持久产物 | G:/codex work/Stock A 交付/运行数据 | 禁止，按生命周期管理 |
| 本机凭据/运行覆盖 | G:/codex work/Stock A 交付/运行配置 | 禁止，合法安全配置加载 |
| PG数据/WAL/日志/主动备份 | G:/Postgres下固定角色目录 | 禁止；旧E盘数据不迁移 |

Git以默认拒绝的.gitignore和本地提交/推送准入共同保护；目录、扩展名只是初筛，人工仍须确认用途。禁止force-add、关闭钩子、no-verify、重命名或改后缀绕过。克隆/换机不会自动获得本地钩子，首次提交前必须按本项目规则恢复准入；不因缺少远端而擅自使用旧仓远端或另建外部仓库。

新项目保留全部业务验收义务，测试仍在外部可重复执行，版本/命令/独立预期和结果摘要在外部留存并绑定源码。阶段记录不入Git不改变其真实验收要求。开发不允许依赖外部测试或过程工具生成正常业务结果。

## S25 唯一完整业务链与算法设计入口

详细算法口径及升级选择以[算法合同与升级规则](docs/design/算法合同与升级规则.md)为唯一现行设计；字段单位/窗口、39类算法、四场景D0、Pure Core板块资格、D2失效及Priority不得继续从不同代旧代码拼接。

端到端输入/产物/消费者、26阶段DAG、来源落点、Owner封存、无循环Manifest/ReadIndex/Publication身份、Head CAS、接受后observer/Forward、六入口与前端ACK、首日/增量/修订/恢复按[数据业务完整闭环](docs/design/数据业务完整闭环.md)及[业务依赖合同](config/contracts/day-chain.contract.json)执行。机器清单是待绑定实现的正式业务合同，不是现成调度程序或执行回执；缺实际生产者不得凭设计节点存在判通过。

前端日常显示ACK由产品自身提交，外部浏览器测试仅用于发布验收，不能成为生产运行导入依赖。API GET保持只读。Forward读取generation在追加事实的同一事务内按分区行锁递增，不以max sequence或行数替代。FEP尚未实施不阻断主研究scope，也不能被报成已实现；新闻功能在所有scope中排除。
