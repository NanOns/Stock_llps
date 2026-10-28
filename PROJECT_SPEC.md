# Stock A PROJECT_SPEC.md：完整产品与工程规格 R12

合同：`STOCK-A-PROJECT-SPEC/R12`。日期：2026-10-26，Asia/Shanghai。状态：`MAINLINE_BUSINESS_DESIGN_FROZEN_IMPLEMENTATION_AND_EFFECT_SEPARATE`。本文件给出新项目完整目标、数据模型、主链、算法、交互、运维与验收；不是旧V4继续开发卡，也不证明新代码已实现。R8补足阶段/字段断边、来源封口准入、局部Profile规则与历史依赖；R9将near_high20采用为0.5 ATR研究基线并补齐11项基础画像表达，修复Canonical分质量/细粒度身份；纯内核不证明完整流水线实现。R7补足新项目逻辑身份、测试资产恢复、安装配置边界和FOUNDATION机器范围；R6补足算法边界、并发恢复和生命周期约束；R10采用股票综合风险/D2及D0准入/结构类型R1执行合同，见算法§10；R11按CHG-016收尾主线领域业务设计及页面/API契约；R12按CHG-017增加数据库表/字段中文注释和项目中文表达要求；实际执行器/typed迁移/消费者仍须分别落实，不能称全产品已实现。R5已取代每日业务文件方案：日更事实/控制状态全部PG化，配置文件只允许新项目config且与测试/过程文档隔离；按责任域归档与旧代码优先复用路线保持。完整业务链、算法命名/单位/时间角色及无新闻范围保持。Stock A源码/交付根和Git业务/规范白名单保持，测试、阶段材料、回执与产物全部外置。后续PG默认路径为G盘；当前E盘旧数据仅保留参考，不迁移、不覆盖、不因本次规范修订切换服务。

## S01 项目目标、范围与完成定义

实现接口、逻辑类型、关键伪代码、基础范围窄图及详细状态转换见[实现详细设计](docs/design/实现详细设计.md)。该设计保留逐域未就绪清单；主线DES-01–08/10现按CHG-016、算法§11和详细设计§17完成设计决策；功能和研究有效性独立报告，不能以未接线重开已冻规则。

新项目是后续唯一主线。服务单用户盘后A股主观研究，覆盖市场→行业/概念→全市场个股→结构/状态→关注→独立Forward及可选条件统计。用户每天能够理解环境、变化、风险、候选为何入选/未入选、下一步观察及失效条件。研究辅助不连接自动交易，不承诺上涨或收益。

完整产品包含六入口、全市场Daily Profile、日周月K、市场四轴/压力变化、板块生命周期/扩散/轮动/重叠、个股Seed/PREWATCH/确认/结构、可解释Priority、Focus、独立Cohort与Forward、回放/比较、真实日更/恢复、Windows安装与托盘；BaoStock补充、FEP、分钟是独立能力支线。新闻、资讯抓取、外部事件催化及题材映射不在本项目范围，不设置对应菜单、API、表或占位能力；TDX行业/概念分类与成员关系保留。

首条交付是一个真实已收盘交易日的端到端基础研究链，随后至少两个真实不同交易日连续增量、重跑与故障恢复。首链不删除完整产品目标，基础通过不宣称全产品完成。新股不因缺两年历史而没有画像；缺字段以真实质量和原因保留。

完成维度独立：SOURCE_CAPTURED、DATA_VALIDATED、RESEARCH_COMPUTED、EOD_CONSUMABLE、PIT_PROVEN、PRODUCTION_RELEASED、STABILITY_OBSERVED、MODEL_SUPPORTED。普通后验研究无需伪造严格PIT；模型/真实前瞻能力必须满足其时间证据。每批预先冻结声明能力和全市场分母，运行后不能删失败域换全通过。

## S02 单一实现、项目结构与权威

本节完整正文已移动到[工程与存储规范](docs/design/工程与存储规范.md#s02)，所有原约束继续适用；本节仅作稳定定位入口。项目内适合中文表达的文档、说明性注释和面向用户文案使用中文；稳定标识、接口协议键、专业词汇及固定枚举值可保留原生语言，并提供必要的中文解释。

## S03 来源合同、阶段0与原始数据

每数据集注册dataset_id/version、供应方/URL或配置根、字段/单位、时钟语义、适用市场/范围、包与解析版本、重复/冲突政策、请求预算/timeout/size/retry/circuit-breaker、保留与降级。登录成功、ZIP存在、HTTP200不等数据可用。

所有TDX配置源根只读，写入前解析绝对路径及链接/别名，覆盖子进程/GBBQ/解压/日志/缓存。缺根配置时拒绝写，临时输出仅在G:/codex_tmp，源根永不写；规范业务事实落PG，不要求在来源盘落文件。新项目阶段0核对输入能力/来源身份/坐标及保护边界，输出FULL_PASS/DEGRADED_PASS/BLOCKED和影响scope；不借用旧项目的FULL_PASS。

TDX本地日线/合法官方包为核心，GBBQ及本地仿射引擎生成复权；BaoStock承担有合同的换手/状态交叉核验等补充。包重复大小写成员、路径穿越、symlink/reparse、坏CRC/SHA、解压漂移、重复证券日期记录必须拒绝或精确隔离，不能字典后写覆盖。

只读来源和必要原件按来源合同保护；新增下载默认临时/流式处理，不默认保存逐日副本。PG存必要规范事实、来源manifest/捕获时钟/解析版本，禁止存原包/响应体/文件二进制。source_content_id与capture_id分开，相同内容不同捕获时钟不合并事实；从同一内容解析相同行无需再次复制。原件缺失列精确SHA/范围与不可恢复影响，不能用派生输出反证输入存在。

请求ready时钟不早于完整下载、校验及必要多源最晚可用；旧缓存/包日期不能提前。基础来源缺失只阻断真实依赖，不因可选turnover失败停止Core。免费来源预算按实际ledger计含登录/查询/重试/退出，未验证能力不得无限探测。

## S04 身份、日历、成员与双时间

security_entity_id与当前symbol分离；源记录保留source_instrument_id。官方事件/稳定身份不足时隔离provisional/unmapped，不靠代码形状/行情连续自动合并实体。指纹只作候选发现，盲测少量成功不证明全市场误报率；合并/吸收后继与纯换码区分。

Data Universe、制度可交易Universe、Research Universe分别登记日期、成员、排除与摘要。全市场画像基于有效身份来源，未知身份原行保留在隔离分母；当前query_all_stock不得覆盖已绑定历史Lifecycle。

交易日历来自有版本的交易所事实；窗口索引越界明确失败，不将自然工作日当交易日、不使用负索引环绕。ST/停复牌/IPO特殊期/退市/板别有当日有效来源和known time，现行ST不证明过去ST，代码首次观测不证明IPO。

行业/概念/风格/未知类型分明，行业父子关系和成员有effective_from/to、observed_at、source_revision。主行业由分类关系决定，不按当日涨幅挑选；支持概念保存全部合法关系，算法支持板块另算。风格不进入主线席位，类型未知不假称行业。

最新TDX映射可在明确NON_PIT_RETROSPECTIVE变体中回看，保存snapshot_as_of、已映射/未映射与覆盖；不当历史真实成员。valid_time、available/known_time、captured_at、seal/cutoff分别验证，后来可得来源不回流T0。

## S05 数据模型、版本键与单schema

本节完整正文已移动到[工程与存储规范](docs/design/工程与存储规范.md#s05)，所有原约束继续适用；本节仅作稳定定位入口。PostgreSQL逻辑数据库固定为stock_a、唯一业务schema固定为stock，任何场景不得新建数据库或schema。每张正式数据库表及每个字段/列都必须有随迁移维护的数据库注释，注释以中文为主；专业术语和固定枚举可保留原生语言并以中文说明。

## S06 大表、去重、冷热与容量预算

本节完整正文已移动到[工程与存储规范](docs/design/工程与存储规范.md#s06)，所有原约束继续适用；本节仅作稳定定位入口。

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

位置按bias≥3先EXTENDED，再pos60的0.8/0.6/0.4/0.2分区。drawdown60、near_high20及closed周/月趋势的唯一精确规则正文见[算法合同§9.7](docs/design/算法合同与升级规则.md#97-foundation基础画像的确定表达)，机器expression_ref绑定具体rule_id；near_high20明确采用0.5 ATR研究基线，见CHG-014；11项机器规则与纯求值入口不等于实际行情、PG/API/UI已通过。历史窗口与context传递见该合同§9.8。

均线BULL_ALIGNED为MA5>MA10>MA20且slope20>0，BEAR反向，TRANSITION用MA5/MA20及slope符号，其余MIXED。相对状态的Δ3统一指rps5_delta3_pp，按first-true：ACTIVE_EMERGENCE为Δ3≥10且(正在压缩或均线BULL_TRANSITION/BULL_ALIGNED)；PASSIVE_RESILIENCE为rel_market_1>0且非ACTIVE且Δ3≤0；LEADING_ACCELERATING为RPS20≥80且Δ3>0；LEADING_STABLE为RPS20≥80且Δ3≥−3；IMPROVING为Δ3>3；WEAKENING为Δ3<−3；LAGGING为RPS20<20；其余NEUTRAL。所需compression/MA状态未知不得落默认中性。

压缩先判断atr或vol ratio≥1.5为极端扩张、≥1.1为扩张，再按range≤0.35/atr≤0.7/vol≤0.7/amount≤0.8/流动性构成强压缩，较宽阈值0.6/0.9/0.9构成压缩；minimum_liquidity仅依赖前20金额均值≥20,000,000CNY，不能隐藏依赖当前amount_ratio20。

量额ratio20分界0.5/0.8/1.2/2；参与结果区分高参与反转、有效推进（ret1>0且CLV≥0.7）、低效、低参与涨/跌与正常，缺CLV的高优先分支不直接降成已知低效。extension以bias≥4 EXTREME，≥3或(ret5≥3*ATR20/C且amount_ratio20≥2且ret1≤0)为HIGH，≥2 MEDIUM，其余LOW，逐分支依赖。

这些阈值是继承的版本化研究规则，不是最优参数/上涨概率。新实现必须给每状态的完整有序表达、全部必需字段/正反边界及独立Oracle；摘要不得替代精确AST。更换规则须显式新算法版本，不静默改旧结果。

## S09 Seed、板块、LOO与轮动

本节Seed的Δ3统一指rps5_delta3_pp。Seed安全：Research Universe、actual bar、合法价格、前20流动性、无price damage/严重extension，按专属三态归约。路径S1为bias<3且压缩且Δ3≥3；S2为bias<3且Δ3≥10且早期均线转强。只读Core，不读板块最终状态、Focus或高级Anchor。

板块按日期真实成员M与可评估E计算覆盖，最低成员5、quote coverage≥0.8；RS_N为retN中位数，板块百分位在同日同类型/源/成员质量合格全集排名。dq5为RS5百分位跨3会话差；跨日宽度/MA变化在两端共同成员可评估集合分别重算，成员加入/退出单列。

Seed宽度记录k/n/未知与Wilson下界启发式(z=1.96)，其值为`(p+z²/(2n)−z*sqrt(p*(1−p)/n+z²/(4n²)))/(1+z²/n)`，p=k/n，n=0为UNKNOWN；仅作小样本惩罚，不称统计置信/上涨概率。强势成员RPS20≥80；留存分母冻结，未知成员不当退出，不删缺值后改分母。无适用前强势集合的成熟留存为NA，不应阻止有独立证据的早期Rotation。

板块PREWATCH：安全 AND dq5≥3 AND(breadth_delta3>0 OR ma20_delta3>0) AND adjusted_seed_width≥0.05。WARM/CONFIRMED按算法合同ALG-23的PURE_CORE_SECTOR_QUALIFICATION_R2明确设计，不继承Amount A硬依赖；这是新规则选择，CHG-016已明确采用，分母/safety/未知按算法§11.2；实际功能和独立验证仍须完成。

行业/概念底层独立，Overlap提供Jaccard/簇/unique share/集中度；不做1/N成员稀释。LOO排除目标后重算中位数、rank、Seed/板块原始资格及必要历史，不能只减计数。算法支持板块选择按LOO确认/成熟、emergence、Seed宽度、stable_id，不能提升个股硬资格。

Rotation有NONE/PULSE/IN/ACCEPTED/EXPANDING/REACCELERATING/OUT/FAILED/UNKNOWN；pulse使用dq5≥10且breadth_delta1≥0.05，冻结pulse前基准/当时成员/Seed和上涨集合。价格retention用固定篮子，不将每日中位收益拼成指数。

early与mature retention分开，前强势分母0只影响mature；单龙头高集中且无扩散不得通过接受/扩散。脉冲后至少1个可评估会话才IN、至少2个才ACCEPTED，成熟留存/扩散才EXPANDING或REACCELERATING；失败/连续衰退/第6个可评估会话仍未接受按冻结有序规则终止；未知暂停计数。新项目已采用`ROTATION_RESEARCH_DEFAULTS_R1`阈值并由ROTATION_EXECUTION_R2限定执行，继承旧R5工程候选数值但重新登记采用状态：early_seed_retention≥2/3、early_breadth_retention≥0.6、breadth_delta≥0、early_top1_concentration≤1/3、mature_strong_retention≥0.75、diffusion_top1≤0.5。emergence HIGH阈值为dq5≥10、breadth_delta3≥0.05、adjusted_seed_width≥0.1。各字段分母、冻结pulse基准与未知规则按前述固定篮子/共同成员合同执行，不能仅有阈值而缺公式。旧配置formal_consumer_enabled=false不继承；以上阈值在本项目已采用，精确篮子/计时/转换见算法§11.3，不是旧系统权限继承或市场效果证明。

## S10 市场轴、三族风险与Priority

趋势使用动态参考目录中被配置为PRIMARY_MARKET角色的类型化市场序列，不在代码写指数证券号。当前publication绑定close、MA20、5市场会话前MA20及坐标，严格大于为STRONG、严格小于为WEAK，其余NEUTRAL，缺证UNKNOWN。

Breadth主式`mean(I(ret1_t>0)−I(ret1_t−3>0))`，零收益为0，精确比较日集合及两端收益可核验；SIGN差单列诊断，不替换。Core仅在日期/范围/精确身份集合与合法历史Universe相等时作为加速，不能只比数量。

Participation为合法amount_ratio20中位数；压力等级为同日合法限价覆盖下跌停数/可评估数，压力变化用前后同口径可比集合。缺当日限价只关压力，缺前日只关变化；共享身份/摘要损坏关其全部依赖，不能全局抹掉独立可算轴。

继承研究阈值：宽度±0.05，参与扩张≥1.2/稀薄<0.8，限价最低覆盖0.8，压力elevated≥0.01/high≥0.05。复合环境按算法§11.4已采用有序规则和hysteresis，不用投票补缺轴；真实可计算和获准用于Priority是不同状态。指数参考、横截面均值、Forward固定篮子不得互换。

股票/板块/市场三族风险分别物化总体/known/unknown/coverage/变化，first-observed与前态变化分开，不用Focus行数替代全市场分母。

Priority的枚举rank、方向和缺键处理见算法合同ALG-32，Δ3明确为rps5_delta3_pp。Priority保留研究序，不合神秘单总分；资格、完整性、桶、emergence、structure、risk、staleness与stable tie-break明确。股票按bucket→emergence→structure→Δ3降序→risk→改善年龄→prior20金额→stable_id；板块按emergence→rank_velocity→breadth/MA变化→调整Seed宽度→exhaustion→stable_id。风险枚举不按字符串排序，必需排序键未知归UNKNOWN且不进入RANKABLE；已知资格仍独立保存。可选展示键缺失可置其明确末位，不制造默认中性。

新项目若采用操作型分值变体，必须独立contract/version/全量Oracle，不与上述Priority V1同名混用。rps20可为完整性前提但不直接加分；合法TARGET_COORDINATE不得被展示丢弃。已知FALSE/必需UNKNOWN的支配按本规则表，全部原因与互斥主因分开。事件变化序、研究排序、显示cap及手动关注分开；NEW不自动第一名。

## S11 D0、D1、D2与状态：补齐设计而不补造旧事实

D0四场景的原语、全部谓词、冻结输入和主次顺序按算法合同ALG-27/28及§11.6的D0_SCENARIO_FACTS_R3设计，替代旧未闭合legacy标志。STRONG_PULLBACK、TREND_CONTINUE依赖合法t−1 episode/LOO raw，不能从同日reducer/Focus反推。原始谓词、场景治理、正式聚合分列；每次运行预先固定FORMAL/DIAGNOSTIC角色，诊断UNKNOWN不污染正式集合。完整目标包括四场景，部分实施只能报具名范围。

新项目封口方案`SOURCE_SEAL_R1`：manifest列全部必需数据集、time_role、原内容/日期身份、实际available/captured时钟、规则/参数与来源验证。对真实盘后首捕，时间边界取官方日历下一常规交易会话open，严格在此前实际就绪/冻结才可申请LIVE/PIT分能力资格；迟到保留LATE/NON_PIT。开发封口只声明真实验证事实，不伪装第三方签发；布尔sealed不能代替证据。历史缺真实捕获不能补首冻。

D1覆盖突破、回踩、恢复、impulse、支撑/接受/失效、多Anchor与活跃选择器。锚在创建时冻结类型/价格区间/坐标/来源/窗口/算法；新锚不在同日自证支撑，使用t−1事件。突破示例继承C>prior_high20+0.1ATR20且CLV≥0.7，创建日tentative，至少两连续可评估守住才accepted。没有前置有效上涨/突破不能叫pullback，收复事件与真正守住分开。

**D2新项目设计`FROZEN_SCENARIO_INVALIDATION_R2`**按算法合同ALG-30执行，替代R1泛指PRIOR_HIGH的方案：LAUNCH冻结PHC20，RECOVERY冻结实际触发的MA20或MA5，PULLBACK冻结关联旧support lower，CONTINUE冻结关联旧support lower或明确T0 MA20事件线。输出failure_level/coordinate/source_window/source_event/effective_from，出生要求C>failure_level，最早下一市场会话生效；后继同坐标C<=冻结线只失效关联episode。不得用PHH20替代只突破收盘平台的事件，不读未来低点，不回填旧缺T0证据的episode。D1缓冲破坏与D2无缓冲事件失效分开，新设计不冒充旧批准合同或效果证明。

状态分maturity(NONE/SEED/PREWATCH/WARM/CONFIRMED)、health、validity、tracking、scenario。硬失效优先，必需未知保留last_known+STALE但不作今日资格；上升当日生效、下降按连续可评估会话及hysteresis；未知/停牌断连续，不增长expiry。旧某锚坏只影响关联episode，重入新episode含parent，旧到期工作继续。

Expiry继承10个可评估会话无升级/改善门槛才退出对应Seed/PREWATCH，市场年龄另显示；退出当天不新建重入。模型边界与普通状态变化分开，事件diff/Why Now/Waiting For/Invalid If/反证可追至谓词。

## S12 Focus、独立Cohort与Forward

Focus是研究跟踪，不是持仓，也不是验证样本全集。独立Cohort按冻结规则留全体MATCH/NO_MATCH/UNKNOWN及未入组分母，不能只留Top N/手工关注/后来赢家。

为消除排序空白，提出新项目`FOCUS_RECENCY_ORDER_R1`：默认按最后已验证跟踪事件交易日降序，再entity_id/episode_id稳定排序；风险/有效性为显式过滤/分组，手动置顶作为独立用户视图，不改研究Priority/Cohort。本项目通过CHG-016正式采用，事件白名单和手动操作语义见算法§11.8，非旧合同追认；后续变更另升版本，不临场改权重。

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

本节完整正文已移动到[工程与存储规范](docs/design/工程与存储规范.md#s16)，所有原约束继续适用；本节仅作稳定定位入口。

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

本节完整正文已移动到[工程与存储规范](docs/design/工程与存储规范.md#s18)，所有原约束继续适用；本节仅作稳定定位入口。

## S19 测试、性能与资源验收

本节完整正文已移动到[工程与存储规范](docs/design/工程与存储规范.md#s19)，所有原约束继续适用；本节仅作稳定定位入口。

## S20 实施、关闭标准与不重复旧路

开发阶段工作流、原生Codex上下文续接与独立审查按根AGENTS.md A27执行；开发过程规则不取代本规格的业务阶段、来源、PG、API、UI和实际发布验收合同。

M0骨架：当前唯一实现/固定schema/typed存储/动态上下文/来源保护/日志容量/测试安全/严格入口/单CLI先成立；原件策略和初始规模可测。不得先建模型平台/巨型审计容器。

M1真实0→1：一个真实完整交易日的声明基础字段、全市场身份/质量/独立数值、PG/API/页面及一次失败last_good形成可复核样本；仅SourceIngress不算。

M2持续闭环：第二真实日正常APPEND、同输入幂等、修订拒绝或有界修复、G1–G4、恢复、真实worker读性能与空间不异常增长；旧身份/当前版本正确。

M3完整研究：按依赖完成Seed/板块/市场/Priority/D0/D1/D2/Focus/独立Forward与六入口；封口/D2/Focus及算法§11主线领域设计已明确采用，新实现按同一合同执行，不重新设置全项目审批门。完整产品所有声明能力真实验，缺源/未来成熟仍精确披露。

M4交付与持续实证：实际安装包、运行身份、生产授权/切换、连续真实日稳定与Forward自然成熟；FEP按独立能力阶段接入。首捕在其源/规则就绪时先行，不等M4才开始积累。

外部过程资料中的ISSUES从V4全部问题台账导入可适用问题的ID/防线/回归/验收责任，执行台账和测试证据不提交Git；禁止只复制叙述不落实机器检查。结构问题必须在M0/M1关闭，算法/结算问题在对应模块准入前关闭；历史不可补证不能靠代码填造。文档完整不等于未来不再发现未知问题，任何新增问题按同一台账与影响域处理，不能再靠新建平行系统逃避。

## S21 配置与决策清单

正式配置文件唯一根为G:/codex work/Stock A/config，按配置目录登记并拒绝根外/未知/测试配置；测试文档与报告不进入正式规范目录。启动配置必须明确：用户批准源码/产物/源根、PG逻辑身份/实际合法凭据加载、单schema、固定端口角色、日历/参考目录、当前数据集/算法/参数/成员/价格合同、请求及资源预算、保留/备份/恢复、已启用能力。缺少必需项拒绝对应操作，不默取旧V4路径/日期/证券。

PG默认G盘为用户确定约束，不属于候选资源预算；新写入/发布前连接验证`SHOW data_directory`、日志/WAL/表空间和服务参数。当前E盘实例可读只证明旧数据仍可访问，不计作新项目G盘存储通过；本轮没有声称已改变运行服务。

确定继承：三窗口、RPS中秩、amr分母排除当前、breadth主式、Core/Supplemental隔离、单PG/schema、完整安装路线与FEP方法。当前采用：SOURCE_SEAL_R1、§10四R1、算法§11全部主线领域合同和FOCUS_RECENCY_ORDER_R1；原板块资格R2/Rotation候选阈值由新确定执行合同显式采用，原D0 raw R2由R3限定替代。资源预算是工程限额，不是市场规律；实际实现/来源/运行准入独立，不伪装旧批准。

真实来源缺口、缺历史首冻、未来到期、外部访问能力无法由本规格制造；应在运行范围与完成谓词中如实反映。用户已授权在Stock A目录放置根规范和Git准入；业务编码、旧代码清理、旧库迁移/复用、服务停止/切换仍按实际后续任务，不能从文档放置推定已完成实施。

## S22 领域合同落地清单与变更边界

本规格是完整产品和工程主规格，领域级大量AST/状态向量应在同一现行合同库表达，不把旧数百份任务文档全部搬成新的活动规范。M0必须生成下列合同目录和schema，M1起只为实际实施字段填充；缺少一项必需表达就不得发布对应能力。

| 合同域 | 最低完整内容 | 对应实施前的关闭条件 |
|---|---|---|
| 字段与Profile | 字段DAG、公式/AST、窗口/坐标/时钟、全部枚举分支、等号、质量、单位与数值向量 | 不允许“已有规则”等未解析引用进入可执行注册表；独立解释器一致 |
| Seed/板块/Rotation | 资格真值表、WARM/CONFIRMED路径、membership版本、LOO、固定篮子、retention/hysteresis全部参数 | 每参数采用状态/默认/来源明确，空参数禁用其能力而非填猜测 |
| Priority/风险 | 各桶边界、有序键、字段必需性、FALSE/UNKNOWN支配、风险主因及全量原因 | 全集独立排序与未知向量；显示截断不改变结果 |
| D0/Anchor/状态 | 每场景原始谓词、来源封口、前态/锚选择、四场景适用性、失效与连续计数 | 按已采用封口、D2四R1和D0 raw R3执行，缺冻结价不得生成可评估episode |
| Cohort/Forward/FEP | 采样总体、Lane/时钟、horizon、due/terminal/path、固定篮子/对照、标签修订与fold | 首捕/迟采/历史分离；PENDING→DUE及各horizon单独证据 |
| 服务/交付 | API字段/分页/错误、manifest token、缓存来源闭包、包资源与启动恢复 | 真PG→实际worker API→真实浏览器→实际包读回，禁止源码结果代验 |

继承参考中的阈值/候选不能仅凭旧文件标FROZEN_CANDIDATE就自动变正式。本轮补齐了方向、关键公式和防复发条件；主线参数/领域规则现按CHG-016明确采用，可选FEP/分钟继续独立延后；真实来源证据、实际功能与效果仍分别报告，不宣称已经完成实施或外部验收。完整规格应让未闭合项可识别和可执行关闭，而不是隐去它们。

## S23 全部136项现行产品需求的现行承接条款

完整136项需求正文已移动到[产品需求](docs/requirements/产品需求.md#s23)，需求ID保持；本节仅作稳定定位入口，不保留第二份需求表。

## S24 Stock A目录、Git准入与外部过程材料

本节完整正文已移动到[工程与存储规范](docs/design/工程与存储规范.md#s24)，所有原约束继续适用；本节仅作稳定定位入口。

## S25 唯一完整业务链与算法设计入口

详细算法口径及升级选择以[算法合同与升级规则](docs/design/算法合同与升级规则.md)为唯一现行设计；字段单位/窗口、39类算法、四场景D0、Pure Core板块资格、D2失效及Priority不得继续从不同代旧代码拼接。

端到端输入/产物/消费者、26阶段DAG、来源落点、Owner封存、无循环Manifest/ReadIndex/Publication身份、Head CAS、接受后observer/Forward、六入口与前端ACK、首日/增量/修订/恢复按[数据业务完整闭环](docs/design/数据业务完整闭环.md)及[业务依赖合同](config/contracts/day-chain.contract.json)执行。P08交易状态、P09复权/周期及P20来源seal为显式required输入；按字段所属产物反查阶段闭包和Owner直接封存输入，拒绝latest与隐式读盘补边。机器清单是待绑定实现的正式业务合同，不是现成调度程序或执行回执；缺实际生产者不得凭设计节点存在判通过。

前端日常显示ACK由产品自身提交，外部浏览器测试仅用于发布验收，不能成为生产运行导入依赖。API GET保持只读。Forward读取generation在追加事实的同一事务内按分区行锁递增，不以max sequence或行数替代。FEP尚未实施不阻断主研究scope，也不能被报成已实现；新闻功能在所有scope中排除。

## S26 旧代码复用与开发起点

优先使用已具备正确计算语义的旧代码，在新项目唯一业务包中按[代码复用与模块边界](docs/design/代码复用与模块边界.md)抽取、修复和绑定合同。复用单位是函数或可闭合的小模块；必须保留公式与输入语义，移除旧目录/脚本/数据/Head依赖，已知错误采用新合同并给出差异理由。不能以“新项目”为理由把所有算法、解析器、图表和桌面交互重写一遍，也不能照搬旧仓技术债。

新算法提案与工程接线分开；未采用的新规则只限制对应能力，不阻碍已确定基础内核的复用。开发以真实来源→规范事实→Core全市场画像→固定PG→同context API/实际页面的首条基础链开始，再按完整图补齐研究域，基础scope不冒充完整研究链。全部测试和阶段实施方案、交接与回执仍放外部测试/交付目录。

## S27 外部文件引用与每日产物消费

完整规范见[工程与存储规范](docs/design/工程与存储规范.md#s27)。本节仅作稳定定位入口：日更业务结果、任务/恢复状态全部存PG必要类型化字段，禁止生成每日业务文件或将其二进制/整对象搬入PG；配置仅限新项目config，测试配置/文档/报告与正式业务隔离。

## S28 主线设计关闭与版本治理

设计/功能/研究有效性为三个独立轴。主线领域采用算法§11及详细设计§17，机器domain_decision_contracts/service_product_contract为配置合同；六项设计条件满足后进入分域实现，不等待其它领域或真实效果最优证明。既有四R1和Foundation保持；可选FEP/分钟DEFERRED不冒充主线完成项，也不移除已批准需求。新问题须有具体反例/需求或来源变化及影响版本，不以相邻模块未实现重开全部设计。

## S29 禁止 Git LFS

用户明确要求旧V4和新项目后续均禁止使用Git LFS。不得新增LFS指针、跟踪规则、对象上传或依赖LFS下载的文件；`.gitattributes`不得配置`filter=lfs`、`diff=lfs`或`merge=lfs`。源码及正式规范仅按S24 Git白名单进入普通Git；数据集、每日业务结果、测试、审计证据、报告和快照不得因禁用LFS而改塞进普通Git，应按各自的PG存储、交付与外部过程资料合同处理。旧V4的历史远端已按用户指令删除，不从本地旧历史重新推送或重建。
