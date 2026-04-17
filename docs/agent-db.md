# AI Agent 时代的 Operational Database

Li Shen [shenli3514@gmail.com](mailto:shenli3514@gmail.com) 

这是一篇思考记录以及讨论笔记,不是结论，大部分内容是让 AI 写的，可能风格会有 AI slop。起点是这几个月和一些朋友的聊天以及 Braintrust 的 Ankur Goyal 最近写的 [AI observability is a database problem: how Brainstore works](https://x.com/ankrgyl/status/2041206959848735107)——他们为 AI observability 自建了一个数据库 Brainstore, 面向 observability,我们讨论的是 operational database。不过 Ankur 的**推理路径**挺不错:从 workload 的具体特征出发,推导产品需求,再到架构选择。这个"workload → 需求 → 架构"的思考方式,比他们具体的技术方案更值得借用。

存储并不是一个单独的东西，而是分了几个 category，包括 datalake、data warehouse，RDBMS，cache，NoSQL 等。每个 category 满足不同的需求，没有一个产品能 one-size-fit-all。这里只讨论 operational database，是给在线服务用的 db。目前可以遇见的未来主角是 AI agents，所以这讨论的是给 AI Agents 用的 operational DB。

AI agent 的数据访问模式和传统 OLTP 有本质差异,这些差异是否足以支撑一个新的 operational db 形态?Postgres 够不够用?如果不够,机会在哪里?

下面的讨论我会涉及一些具体存储引擎的选择。我最熟悉的是 TiKV,所以举例都以它为参考——不是说它一定是唯一答案,只是用作具体的讨论锚点。换成 FoundationDB 或其他分布式 KV 底座,大部分论点也成立。

我把思路分四层:需求 → 设计 → 架构 → 实现权衡。讨论 agent db 时很容易把这四层混在一起,导致谁都说不清在哪一层没共识。所以这篇刻意分层写,每层讨论完再往下走。Open questions 放在最后,一起讨论。

在进入四层之前,先说清楚这篇讨论的基础判断——为什么现在这个问题值得聊一聊。  
---

## 为什么现在讨论 Agent-Native DB

这个问题的答案不在"AI 很火",而在**用户行为的结构性变化**。Agent-Native DB 并不是什么 chat2query，也不仅仅是什么100万个表。这些要不就是 trivial 的特性，要不是某种架构设计下的技术实现。我思考的是因为“用户”行为的变化带来了新的需求和挑战，导致需要一个更合适的 DB。这里的用户不一定是人，或者说大概率不是人，而是 AI agents。

### 从"用户手动触发"到"agent 自动触发"

以前一个 SaaS 用户一天在产品里操作几十到几百次。每次 action 是用户显式触发:点按钮、填表单、发消息。总量被"人的时间和精力"封顶。一个产品 100 万用户,每天产生的 action 数量大致是 10^8 量级。

现在的变化是:用户给 agent 一个任务,agent 自己迭代几十轮完成。每一轮是一次 action——读数据、调 API、写 state、调 LLM。用户没触发,是 agent 自己触发。

更关键的是 agent 可以:

- **并行**——用户开一个任务,agent 可能 spawn 多个 subagent 同时做  
- **24/7 运行**——用户睡觉了 agent 还在跑(定时任务、监控、长期研究)  
- **跨工具组合**——一个 agent 任务可能触发几十个工具调用,每个都是一次 action

同样 100 万用户,action 数量可能从 10^8 跳到 10^10 甚至 10^12。这不是 "action 稍微多点",是 **1000x 到 10000x 的数量级跃迁**。

**这个是需求侧的结构性变化,不依赖任何特定平台成功**。就算 OpenAI GPTs 失败、Manus 倒闭,只要"用户通过 agent 自动化"这个行为模式成立,action 数量的爆炸就成立。人类把重复劳动外包给机器是一个几百年的单调递增趋势,agent 只是最新一次跃迁。

### 双重压力:Action 数量 × Payload 体积

Action 数量跃迁只是一半。另一半是单次 action 的 payload 膨胀。

- 传统 OLTP 一次 action 写几百字节——一条订单、一次点击、一个状态变更  
- Agent 一次 action 写几 KB 到几 MB——完整 context \+ LLM response \+ tool call result \+ intermediate state

Action 数量 × 单次 payload \= 写入体积的双重放大。Action 数量 100x、payload 10-100x,总写入体积可能是 1000x 到 10000x。

这个双重放大是为什么"Postgres \+ jsonb 勉强能跑 → 完全跑不动"的门槛跨越很快。不是"稍微多点就扛不住",是**一旦跨过 agent 普及的临界点,所有传统 db 同时失效**。

### 这个判断的含义

如果上面的判断成立——action 数量跃迁 1000x 以上——那么:

- **现有 db 架构是为"人触发 action"的量级设计的**,按这个量级线性扩展无法覆盖 agent 时代  
- **不是渐进优化能解决的**,需要重新审视每一条架构假设  
- **现在做这个准备是合理的**,趋势已经开始,等几年后再做就晚了

反过来,如果这个判断**不成立**——比如 agent 只在少数场景下替代人类 action、大部分用户还是以手动为主——那么这篇文章讨论的架构就是**过度设计**。几百到几千个 agent 的场景,Postgres \+ jsonb 就够用,没必要 object storage、没必要 box 架构、没必要 tunable 事务。

所以整篇文章的前提是 **"action 数量跃迁"会真实发生**。后面讨论的架构,本质上是为这个量级跃迁后的世界设计的 operational db。  
---

## 第一部分:需求层——Agent Workload 有哪些特点

这一层只描述现象,尽量不提任何 db 实现技术。

### 1.1 访问模式和传统 OLTP 的差异

| 维度 | 传统 OLTP | Agent workload |
| :---- | :---- | :---- |
| 延迟 | p99 \< 10ms 是刚需 | p99 \< 100ms 都能接受,推理才是瓶颈 |
| 读写比 | 读多写少(典型 10:1) | 写多读少(单次 run 几百次写、几次读) |
| Value 大小 | \<1KB 为主 | 1KB \~ MB 级,偶尔更大 |
| 访问模式 | 随机点查为主 | Session 内顺序 append \+ prefix scan |
| 并发 | 高 QPS 小事务 | 中等 QPS,事务内 op 数多 |

这几条合起来意味着:**agent 场景下 tail latency 的容忍度比传统 OLTP 宽松得多**。这个容忍度是后面所有架构选择的基础。

### 1.2 数据以 semi-structured 为主

Agent 的 state 本质上是半结构化的:conversation history、tool call trace、scratchpad、中间 artifact。这些东西塞进关系表硬要拆列,schema 会频繁变动;塞进 `jsonb` 能跑,但索引、查询、schema 演化都是打折体验。

MongoDB 当年为了 web 应用做的 document 模型,在 agent 场景下反而更契合。

### 1.3 数据生命周期:活跃度断崖式下降

这是我觉得 agent workload 和传统 OLTP 最大的差异。

传统 OLTP 的访问分布是 Zipf——热点反复读写,冷数据偶尔触达,分布是**连续的**。你很难画一条线说"这边热那边冷"。

Agent 场景是**断崖式**的:

- 活跃 session 期间(几分钟到几小时):高频读写,以 append 为主  
- Session idle 后几乎立刻冷掉,但不是"永远不会动了"——chat 应用用户可能几天后回来继续、agent 可能收到迟到的 feedback、可能被 replay 用于 debug  
- 冷数据的一部分会被"提炼"成 memory:摘要、embedding、extracted entities,以完全不同的形态被后续 agent 检索

更准确地描述这个特点:

- **原地 update 在 agent 场景极少**——历史 event 通常不改,改历史会破坏可审计性和 replay 能力  
- 但 **append 会持续发生,只是频率断崖式下降**(活跃期高频 → idle 期偶发 → 最终真正冷掉)  
- 冷热边界不是二元的 open/close,是一个活跃度谱系

存储系统需要处理的不是"活跃"和"归档"两种状态,至少是三种:

- **Warm**:最近活跃或频繁被访问的 session  
- **Cold**:已归档但仍可能被 append 或重新激活  
- **Archive**:几乎不会再被原样访问,只有派生物(embedding/summary)还会被查

### 1.4 工作集和全量数据比例悬殊

假设一个 agent 平台托管 10 万个 agent,每个 agent 一天跑 100 次,每次产生 10MB 数据——一天新增 100TB。但同一时刻真正活跃的 session 可能只有几千个,对应工作集几 GB 到几十 GB。

这是**热集小、冷集海量**的分布。活跃工作集占比可能只有千分之几。这个比例决定了存储成本结构:冷数据能多便宜存下来,直接决定产品的定价竞争力。

### 1.5 查询模式比 OLTP 集中

Agent 的查询模式相当集中,不是传统 OLTP 那种 open-ended SQL:

- **Session load**:按 session\_id 拉全部 event,最高频  
- **Prefix scan**:按 agent\_id \+ 时间范围扫 event,用于 replay、debug、audit  
- **Hybrid retrieval**:vector 检索 \+ 结构化 filter,memory 层核心查询  
- **Full-text search**:内容搜索,主要用于 debug  
- **Aggregation**:统计分析,监控用

这五种基本覆盖 90%+ 的查询。跟传统 OLTP 的"任意 SQL"相比收敛很多,是系统设计的机会点。

### 1.6 操作的聚合单位:以 session/agent 为天然单位

传统 OLTP 的操作是离散的(单条记录点查),没有强聚合单位。Agent 操作有**天然的聚合单位**——同一 session 的写入、读取、索引、查询几乎总是一起发生。

具体来说:

- Agent 一步迭代的所有写入属于同一 session  
- Session load 是 session 内的全量读取  
- Hybrid retrieval 通常带 `agent_id =` 或 `session_id =` 的 filter  
- Replay / debug 的单位是 session 或 session 集合

这个特点在架构层是可以被充分利用的——只要数据布局跟这个聚合单位对齐,很多分布式系统的协调成本就可以绕开。

### 1.7 事务需求:比 OLTP 弱得多

Agent 场景需要的事务能力和传统 OLTP 差异巨大。我梳理过几类典型操作:

- **单 event append**:不需要事务,单 KV put 就够  
- **Event append \+ state update**:可以塞进单 document 用原子写解决,不需要多 key 事务  
- **Context fork**:需要 snapshot read 一致性,但不是写事务  
- **Multi-agent 协作改共享 state**:大部分场景用 append-only 日志 \+ last-write-wins 解决,真需要 serializable 的极少  
- **派生数据写回**:纯 insert,幂等设计(session\_id 做 primary key)就够  
- **Checkpoint**:单 document 原子写

结论是 **agent 场景需要的事务能力是受限的**:

- 单 document 原子写(基线,所有场景需要)  
- Snapshot read(replay、debug、fork 需要,但这是只读一致性)  
- 跨 key 原子写(偶尔需要,频率低,通常能塞进单 document 避开)

**几乎不需要的**:serializable 隔离、跨行的 read-modify-write 并发控制、传统银行转账那种强事务。

这和传统 OLTP 巨大差异。传统 OLTP 为了支撑任意并发 read-modify-write 构建了完整 MVCC \+ 锁机制,这套机制在 agent 场景大部分用不上,但每次写入都在为它付代价(版本链、锁表、冲突检测)。能做和适合做是两个事情，回想当初 TiDB 硬要上日志存储和分析的场景，比其他方案贵很多还慢很多，并不是合适的选择。

### 1.8 多租户:从"有限大应用"到"无数小应用"

这一点我觉得是 agent workload 和传统多租户最本质的差异,也是很多传统架构失效的根源。Agent workload 带来的多租户挑战,要放在多租户的复杂度谱系里才能看清楚。多租户不是只有一种形态,它的复杂度是层层叠加的——每一层在前一层基础上增加新的诉求。

**第一类:内部多服务共享基础设施**

典型例子是 Airbnb、Databricks、Uber 这种大平台内部,几十上百个业务共用底层数据平台。或者一套 Kafka 承载不同团队的 topic、一套 Redis 集群服务不同 service。

- Tenant 数量:10^1 到 10^3  
- 每个 tenant:内部 service,规模差异大但都有专人维护  
- 核心诉求:**资源隔离 \+ 优先级控制**——防止某个 service 把共享资源吃满,保证关键业务优先  
- 数据在内部互相可见不是问题,schema 各 service 独立管理  
- 成熟解法:namespace \+ quota \+ 分 pool \+ QoS  
- noisy neighbor 需要考虑 service 的优先级，是一个需求

**第二类:SaaS 多租户**

典型例子是 Salesforce、Shopify、Notion。在第一类基础上,多出两个核心诉求:

- **数据隐私**:客户之间绝对不能看到彼此的数据,任何数据泄漏都是严重事故  
- **Schema evolution**:不同客户可能订阅不同版本、使用不同 feature,schema 要支持平滑演化、灰度发布、per-tenant 差异  
- Tenant 数量:10^2 到 10^5  
- noisy neighbor 是一个问题，业务很可能对延迟敏感，大租户会成为 noisy neighbor，有需要高的 SLA，不想被小租户影响  
- 每个 tenant:稳定基线负载,可预测  
- 成熟解法:row-level security、per-tenant schema version、独立备份/恢复

复杂度明显高于第一类。但解法的本质还是**per-tenant 精细化**——每个 tenant 有自己的配额、schema 版本、备份策略,线性扩展。

**第三类:Agent 平台多租户**

在第二类基础上,叠加数量级跃迁带来的全新问题:

- Tenant 数量:10^5 到 10^7(每个 agent 就是一个 tenant)  
- 每个 tenant 极小,绝大多数时候完全空闲,偶尔短时间爆发

这个分布不是 "load varies",是**极端稀疏 \+ 突发**:

- 99%+ 的 tenant 在任意时刻是 0 QPS  
- 活跃 tenant 里绝大多数也只有个位数 QPS  
- 少数头部 tenant(被广泛使用的 agent)可能高频访问  
- 个别 agent 可能"爆炸式失控"(bug 导致死循环调用)

**第二类到第三类的跳跃不是量的增加,是质的变化**。第一类到第二类是诉求维度增加,但每种诉求的解法都是线性扩展——tenant 多 10 倍,per-tenant 处理跑 10 次就行。第二类到第三类,数量级跃迁让 per-tenant 处理本身不成立:

- Per-tenant metering:100 万 tenant 每个都精确 metering,metering 本身成本超过小 tenant 收入  
- Per-tenant schema:100 万 tenant 每个独立 schema 版本,schema 元数据本身就 PB 级  
- Per-tenant quota:100 万 tenant 每个配额检查,检查本身成瓶颈  
- Per-tenant backup:100 万独立备份,备份系统本身需要一个数据库管理

所以第三类需要的不是"第二类的更好实现",是**放弃 per-tenant 精细化,转向批量共享 \+ 采样 \+ 粗粒度隔离**的新范式。

这个范式下 noisy neighbor 呈现几种前两类没有的新形态:

**冷启动风暴**。99% 的 tenant 处于冷状态,数据在 object storage 或 archive。某时刻几千个冷 tenant 同时被激活——每个都要拉数据、建缓存、预热索引。这几千个激活操作会把 object storage 带宽、本地缓存、元数据服务全部打爆。前两类架构下 tenant 总是"温的",没这个问题。

**长尾资源碎片**。每个 tenant 占一点点资源——几 KB 元数据、几个连接、几个索引 entry。100 万 tenant 就是几百 GB 元数据、十万级连接、亿级索引 entry。单看微不足道,汇总起来变成主要成本。Postgres 一张表装几百万行 tenant 元数据,查询和维护都开始痛。

**爆发式写入冲击**。某个 agent 突然跑起来,一秒几百次 event append。几百个 agent 同时这样——在 agent 平台是常态——瞬时写入压力是平时几十倍。传统 per-tenant 限流对"每个 tenant 都没超、总和超了"无效,需要 per-box 和全局层面的 admission control。

**计量和计费粒度问题**。"按使用量计费"是 agent 平台自然商业模式。但精确计量 10^7 个 tenant 的访问次数、存储、带宽——**计量本身的成本**可能超过小 tenant 收入。需要分层计量:头部精确、长尾采样。

**故障隔离的爆炸半径**。前两类多租户下一个 bad tenant 影响范围是"同节点几十到几百个 tenant";agent 场景下一个节点可能承载几万个 tenant,爆炸半径从几百变几万。故障隔离需要更细粒度——这就是 box 架构的动机。

这些问题的共同根源是:**tenant 数量级发生了 1000x 到 100000x 的跃迁**。前两类的每一条架构假设都需要重新审视。

### 1.9 自托管需求普遍

Agent 的输入输出经常包含 PII、商业机密、代码等敏感数据。大量客户要求 self-hosted,数据不能出他们的网络边界。

这对 db 的部署模型提出要求:不能依赖复杂的 operator、不能要求客户懂分布式 db 运维、最好是 stateless container \+ 标准云服务(S3、Postgres、Redis)的组合。要在设计的时候考虑可监控性和可诊断性。

---

## 第二部分:设计层——Agent-Native DB 应该暴露什么抽象

这一层讨论数据模型、生命周期概念、查询原语、API 形态。不讨论底层用什么引擎。

### 2.1 Document 作为一等公民

"一等公民"容易变成营销话术,具体应该包含四条:

**存储层**:不 shred(不把嵌套字段拆成独立列存储,Parquet / 传统数仓处理 JSON 的方式)。整个 document 作为一个 value 存,避免拆列重组的开销。

**索引层**:nested field 能直接建索引,不需要先 materialize。`tool_calls.*.error_type` 这种路径要支持数组展开(类似 MongoDB 的 multikey index)。

**查询层**:路径表达式是 native 语法,不是 `->> '$.a.b'` 这种字符串操作。查询计划器能理解路径,做 predicate pushdown。

**Schema 演化**:不需要 DDL。新字段直接写,旧字段自然不存在。查询时 missing field 有明确语义——需要一个 `exists` 原语。

Postgres 的 jsonb 前两条能做、后两条做不好。MongoDB 的 BSON 四条都好。Agent db 需要的是 MongoDB 的接口抽象。

### 2.2 Locality 作为核心设计原则

基于 1.6 的观察——agent 操作有天然聚合单位——locality 不是性能优化手段,是**第一性的设计原则**。一旦 locality 被违反,agent db 的性能优势就消失。

Locality 应该体现在几个层次:

**数据 locality**。同 session 的所有 event、state、metadata 物理聚簇,落在同一 shard、同一 segment、相邻存储块。这样 session load 是一次顺序读,不跨节点 fan-out。

**索引 locality**。每个 shard 维护自己那部分数据的 local secondary index,不用 global index。agent 查询天然带 `session_id =` 或 `agent_id =` 的 filter,可以直接定位到 shard 子集,local index 在定位后扫描就够——不需要 global index 的两跳。

**事务 locality**。大部分事务天然是单 shard 范围(同 session 内的操作),走轻量事务路径——不需要 2PC、不需要全局时间戳协调。跨 shard 事务保留作为可选能力,但通过 partition 策略压到极低频率。

**计算 locality**。把能下推的计算推到存储节点——过滤、投影、简单 aggregation、vector similarity、full-text 匹配都可以在数据旁边做。agent 数据 value 大(MB 级),避免数据搬运收益很大。

**缓存 locality**。活跃 session 全量驻留本地 SSD 缓存,不跨节点。Session 活跃期所有操作命中本地缓存,只有 WAL 写入走远端。

Locality 的反面不是"性能差",是"整个架构失去合理性"。一旦默认需要跨 shard 协调、global index、分布式事务,agent db 相对传统分布式 OLTP 的差异化就没了。

### 2.3 事务模型:写弱读强,强一致显式申请

基于 1.7 的事务需求分析,agent db 的事务模型应该是:

**写路径:默认 read committed,单 document 原子写**。大部分写入是单 document append 或 upsert,read committed 够用。不默认提供 serializable,因为基本没人需要、提供了反而增加协调开销。

**读路径:默认 snapshot read**。查询时给一个一致的时间点视图,这对 agent 的 replay、debug、context fork 都至关重要。底层 KV 如果支持 MVCC,snapshot 是免费能力,只需要在 API 层暴露。

**可选的强隔离:按需申请**。少数真需要的操作(多 agent 共享计数器、quota 跟踪)可以显式申请跨 key 事务。但这应该是**需要明确调用的能力**,不是默认行为——让用户为复杂度显式付费。

这种"读强、写弱、强一致显式申请"的设计,思路类似 DynamoDB(transactional write 是单独 API,默认不启用)。对 agent 场景是合适的。

### 2.4 Session Lifecycle 作为原生概念

生命周期管理应该是 db 原生的,不是应用层约定。具体需要:

- **Session 作为一等对象**:不只是按 session\_id prefix 分组,session 本身有元数据(created\_at、last\_active\_at、status、owner)  
- **活跃度信号**:db 基于 idle timeout 或显式 archive 触发冷热迁移,不要求硬性 `session.close()`  
- **声明式 lifecycle hook**:"idle 超过 7 天触发归档"、"归档时生成派生物"这类规则在 schema 里声明  
- **重新激活能力**:冷 session 能以较低代价被 append 或 reload 回 warm 状态,不需要数据搬迁

### 2.5 派生数据作为一等公民

Agent 的冷数据会以多种形态存在:原始 trace、summary、embedding、extracted entities。传统 db 只管存原始数据,派生物是应用的事。

Agent db 需要把派生数据管理纳入原生能力,但边界要清晰:

- **生成派生物的计算**(LLM 调用)放在应用层或 companion service——db 不应该有 LLM 调用能力,那是巨大的复杂度  
- **派生物的存储、索引、查询**是 db 的事——和原始数据共享事务视图,查询时能关联  
- **触发机制**是 db 提供的——lifecycle hook \+ webhook,应用响应 hook 生成派生物写回

db 承担"调度和存储",应用承担"计算",边界清晰。

### 2.6 Tenant 作为逻辑单位

基于 1.8 "无数小应用"的观察,tenant 的物理表示必须极轻,不能每个 tenant 都分配独立连接池、独立元数据条目、独立索引结构。

Tenant 应该是**逻辑概念**:

- 按逻辑 partition 计费和隔离  
- 物理上共享更大粒度的资源(比如 1000 个 tenant 共享一个 shard、共享同一个索引文件用 prefix 隔离)  
- 添加 tenant 是元数据操作,不是物理资源分配

这和"每 tenant 独立 partition"不矛盾——partition 是逻辑的可计费单位,不是物理的资源分配单位。物理上的隔离单位应该是更大的——后面架构层讲 cell 时展开。

### 2.7 查询原语

基于 1.5 的查询模式,几个核心原语:

- **Session scan**:按 session\_id 高效拉全部 event  
- **Prefix range**:按 primary key prefix \+ 时间范围扫描  
- **Hybrid retrieval**:`vector_similar(embedding, top_k=10) WHERE agent_id = X AND ts > Y`,向量检索和结构化 filter 在同一个 query plan 里协调  
- **Full-text**:内容搜索,可以和结构化 filter 组合  
- **Append-and-fetch-last-N**:agent 最高频的原子操作——写一条 event,返回最近 N 条用作 context,合并成一次 round trip

其中 **hybrid retrieval 是最有差异化的**。难点是 vector search 和 structured filter 的顺序。做在 db 内部的好处是 filter 和 vector 共享同一份数据、同一个事务视图,外挂方案做不到这个一致性。

### 2.8 Context Fork 原语

这个原语值不值得做进 db,我自己还在犹豫。

需求:parent agent spawn subagent 时需要给一份 context 快照。应用层做是深拷贝,性能差且占空间。db 层做可以是 copy-on-write:暴露 `fork(session_id) -> new_session_id`,底层共享数据直到分叉后有写入。

支持做进 db 的理由:底层有 MVCC 能做、应用层做不好、multi-agent 场景必需。

反对的理由:如果 multi-agent 架构本身被证伪(LLM 快速切换角色比 spawn 多个 agent 更有效),这个原语就没意义了。

我倾向支持,但标记为"待验证"。

### 2.9 哪些不应该放进 db

划清边界很重要,不然产品会膨胀失焦。下面这些应该明确不做:

- **LLM 调用能力**(包括自动 embedding 生成):应用层的事,db 自动做反而绑死模型选择和版本管理  
- **Agent 间通信**:消息队列或应用层协调,不是 db 职责  
- **Tool execution**:显然  
- **Workflow 编排**:Temporal、LangGraph 在做,db 跨界会失焦  
- **Semantic caching**:中间件职责,混进 db 概念会乱

默认原则:**放应用层,除非有强理由下沉**。强理由只有三个——性能必须贴近数据才够、跨操作原子性必须事务保证、几乎每个 agent 都要用的通用能力。

---

## 第三部分:架构层——怎么实现这些抽象

这一层讨论存储引擎选择、读写路径、索引结构、物理隔离。

### 3.1 Object Storage 作为主存储

我觉得新一代 operational db 都应该基于 object storage 构建,agent-native db 尤其如此。理由是 1.1 到 1.9 那些需求**几乎每一条都能在 object storage 上找到对应支撑**:

- **延迟容忍度宽松**(1.1):Object storage 首次访问延迟 30-100ms 对传统 OLTP 致命,对 agent workload 不致命——活跃数据在本地缓存、冷数据访问时用户能接受等一下,反正后面还要等 LLM 推理几秒  
- **工作集和全量比例悬殊**(1.4):活跃集几 GB、冷集几百 TB,这是"object storage \+ 本地缓存"模型的最佳适配  
- **数据量膨胀速度**(1.4):本地 SSD 几 TB 就要分片,运维成本失控;object storage 容量无限  
- **Session 生命周期**(1.3):冷数据能下沉到 S3 IA 或 Glacier,和 warm/cold/archive 分层天然对齐  
- **多租户稀疏**(1.8):"几乎零成本地维护一个休眠 tenant"只有在 object storage 上成立——在物理磁盘架构下每 tenant 预留空间哪怕 MB 级,百万 tenant 就是 PB 级浪费  
- **自托管**(1.9):S3 私有化有 MinIO/Ceph 成熟方案;stateless container \+ S3 \+ Postgres/Redis 是最省心的自托管形态  
- **Context Fork**(2.8):Object 的 immutable 特性 \+ 元数据指针是 copy-on-write 的原生形态。Fork 就是生成新元数据指向同一组 object,分叉后写新 object

还有一个直接碾压的因素:**成本**。粗略看 AWS us-east-1:

- EBS gp3:$0.08/GB/月  
- S3 Standard:$0.023/GB/月  
- S3 IA:$0.0125/GB/月  
- S3 Glacier Instant:$0.004/GB/月

存 1PB 冷数据:EBS 要 $80K/月,S3 IA 要 $12.5K/月,Glacier 只要 $4K/月。agent 场景 95% 数据是冷的,这个差距直接决定定价能力。"比 DynamoDB 便宜一半"的空间主要就从这里来。

很多"部分用 S3"的方案还依赖本地盘存元数据、统计信息、consensus 状态——这会继承两边的运维复杂度。**要做就做彻底**:元数据放 Postgres、coordination 放 Redis、数据全走 object storage。

### 3.2 分布式 KV 作为中间层,但事务能力要 tunable

Object storage 粒度太大,不能直接承载 agent 的细粒度读写。中间需要分布式 KV 层做:

- WAL 接收高频写入(一请求一文件落 object storage)  
- 事务协调(按需,不是默认)  
- 活跃数据的本地缓存  
- Range scan 和 prefix 访问

这一层我以 TiKV 为例讨论,因为我最熟悉它。换成 FoundationDB、etcd(更轻量)、或自研方案,下面的论点大部分也成立——关键是这一层应该具备什么能力、应该避免什么复杂度。

TiKV 作为这一层合适——生产级成熟、range scan 原生好、MVCC snapshot read 默认可用、有 object storage 集成基础。

但 TiKV 的事务能力在 agent 场景下**要 tunable**。TiKV 支持完整的分布式 ACID 事务(基于 Percolator 协议、2PC、全局时间戳 PD),这个能力对金融场景是刚需,对 agent 场景大部分操作是负担——每次写入都在为用不上的分布式协调付代价。

基于 2.3 事务模型 \+ 2.2 locality 原则,在 TiKV 上应该是:

- **默认走单 shard 路径**:依赖本地 MVCC,不触发 2PC,不等待全局时间戳  
- **跨 shard 事务作为可选**:需要分布式协调时显式调用,走完整 Percolator 路径  
- **Snapshot read 默认可用**:TiKV 本来就有,只要在上层 API 暴露

Partition 策略上配合——同 session 的数据物理聚簇在同一 shard、同 tenant 的数据聚簇在一组 shard 内。这样绝大部分业务操作天然是单 shard 事务,跨 shard 事务的频率被压到极低。**事务边界 \= 业务边界 \= 物理边界,通过数据布局把分布式事务的成本消除掉**。

关于选型的一点补充:TiDB 把 TiKV 包成了 MySQL 兼容的关系库,这个包装对 agent 场景是错的抽象。不是关系模型错,是协议错——北美 vibe coding 基本都是 pg,document 场景关系模型本身也不匹配。TiKV 底座的潜力被 TiDB 的叙事严重低估了。

### 3.3 Document 接口 vs Postgres 接口

如果资源有限必须二选一,我倾向 **document 接口优先**,理由三条:

**竞争格局更好打**。pg 兼容赛道已经挤满:Supabase、Neon、PlanetScale、Aurora、AlloyDB。Document scale-out 赛道除了 Mongo Atlas 就是 DynamoDB,中间档位基本空白——SurrealDB 太小众,FerretDB 只是协议翻译不解决 scale。

**Agent 场景更刚需 document**。硬塞 jsonb 能跑但不是 native 体验。Native document \+ object storage scale 是真实差异化。

**工程复杂度更低**。Postgres 兼容要做 optimizer、类型系统、extension 生态。Document 接口只需要 query 翻译 \+ secondary index,核心重担不在存储层。

不用完全绑死,折中路径:

- 底层 TiKV \+ object storage  
- 中间 document 抽象,提供 put/get/query/index 原生 API  
- 接口层主推 document 原生 API,加一个 MongoDB wire protocol 兼容层吃 Mongo 生态,再加一个有限的 pg 兼容(只支持 jsonb 相关语法,不承诺完整 pg)

核心 bet 是 document 抽象本身。

### 3.4 读写路径

**写路径**:请求 → WAL(一请求一文件落 object storage)→ 后台异步 compact 成 segment(按 session\_id 聚簇)→ 每个 segment 多种本地索引(row store、inverted index、column store、vector index、bloom filter)→ 元数据写 Postgres。

**读路径**:查询 → 解析 → 合并三个源——未处理 WAL \+ 已处理未 compact \+ 已索引 data。活跃 session 全部命中本地缓存,冷 session 按需从 object storage 拉。

**分层对应**:

- Warm:segment 本地 SSD 缓存  
- Cold:segment 在 S3 Standard,元数据轻量  
- Archive:原始数据 S3 IA / Glacier,派生物(embedding/summary)保留在 cold 层可查

### 3.5 物理隔离:Cell / Box 架构

基于 1.8 "无数小应用"的需求,物理隔离单位不能是"单个 tenant"——那样元数据成本爆炸。也不能是"全局共享资源池 \+ per-tenant quota"——爆炸半径太大。

合理的单位是**中间粒度**:一组 tenant 共享一组物理资源,tenant 之间逻辑隔离,组和组之间物理隔离。这个中间粒度可以叫 cell,也可以叫 box,本质一样:

- 每个 box 承载固定数量 tenant(比如 1000 到 10000 个)  
- Box 内 tenant 共享计算节点、缓存、索引文件(用 prefix 隔离数据)  
- Box 之间资源独立、故障独立、限流独立  
- 单个 tenant 的坏行为最多影响同 box 的 tenant,爆炸半径被限定  
- Box 可以作为独立单位扩容、迁移、升级

Box 架构和之前 AWS 的 cell-based architecture 思路一致,但对 agent db 来说,box 还要承担另外一个职责——**tenant 的物理聚簇**。同一 box 内的 tenant 共享 object storage prefix、共享本地缓存预算、共享 WAL 文件批量写入,这些"摊薄"效应让长尾小 tenant 的边际成本趋近零。

配合 box 架构的其他机制:

- **Per-box quota**:限制单 box 的总资源使用,防止冷启动风暴  
- **Per-tenant quota**:限制单 tenant 的突发,防止 runaway agent  
- **Global admission control**:防止系统级资源(object storage 带宽、元数据服务)被打满  
- **分层计量**:头部 tenant 精确计量,长尾 tenant 采样聚合,让计量本身的成本可控

---

## 第四部分:实现权衡

这一层是具体工程决策,每一条都是 trade-off。

### 4.1 Session 聚簇的 Compaction 策略

LSM 架构默认不保证"同 session 的 spans 在同一 segment"——spans 跨多个 memtable/SST 的话,session load 需要 merge 多个 SST 结果。

做到聚簇需要:compaction 策略按 session\_id 聚合,或者写入时特殊 routing。这个优化直接决定 session load 性能——做好了是 O(1) seek 扫完一个 SST,做不好是 O(log N) seek 多个 SST merge。

代价是 compaction 更复杂,可能写放大增加。

### 4.2 Local Secondary Index 的写放大

Document 接口要高效跑 nested field 查询,需要 secondary index。local index 每次写入原子更新多个 KV 对(都在同一 shard 内,走轻量事务路径),写放大比 global index 低但依然存在。

Agent 场景写入量本来就大,这个放大是否可接受需要实测。可能的优化:

- 异步索引(写入立即返回,索引后台建,读取时 merge WAL)  
- 选择性索引(只索引高频查询字段,不是所有字段全建)  
- 索引粒度可配置(session 粒度 vs event 粒度)

### 4.3 Nested Field Indexing:Shred 还是不 Shred

`tool_calls.3.result.error_type` 这种路径表达式索引有两条路:

- **Shred**:把 nested 字段拆成独立 KV 对建索引——索引效率高,但回到"warehouse shred JSON"的老问题,schema 变化时代价大  
- **不 shred**:整个 document 存一起,索引用 path pointer——schema 灵活,但索引效率差

Mongo 的做法是 shred 得很聪明(multikey index)。TiKV 上做需要重新设计。

### 4.4 Hybrid Retrieval 的执行计划

Vector search 和 structured filter 的组合有几种执行顺序:

- **Pre-filter**:先结构化 filter 缩小候选集,再 vector search——候选集太大时 vector 计算爆炸  
- **Post-filter**:先 vector top-K,再 filter——top-K 可能全被 filter 掉,召回为零  
- **联合**:pre-filter index \+ vector index 协同扫描——实现复杂但效果最好

Milvus、Weaviate、Qdrant 都在卷这个。做到生产级质量是很大的工程投入,但这也是差异化的核心。

### 4.5 Lifecycle Hook 的实现

`on_idle_timeout` 这类 hook 需要一个调度器。可以放 db 内部(简单但绑定紧)或用 Temporal 之类外部 orchestrator(灵活但多一个组件)。

倾向外部——agent 平台反正都要 workflow engine,复用比较自然。db 只负责发出事件,不承担调度逻辑。

### 4.6 WAL 一请求一文件的代价

"一请求一文件"的 WAL 设计是为了极致写吞吐(无协调)和立即持久化。代价是 object storage 的 API 调用成本——S3 PUT 每 1000 次 $0.005。高吞吐时这个成本不可忽略。

对高吞吐场景要考虑 batching(牺牲少许延迟换 API 调用成本),或者 WAL 本地缓冲一段时间再上传。

### 4.7 Box Size 的权衡

Box 内承载多少 tenant 是个核心参数:

- **Box 越小**:隔离越好,爆炸半径越小,但资源利用率差(每个 box 都要有基线资源开销)  
- **Box 越大**:资源利用率好,摊薄效应强,但爆炸半径大,一个坏 tenant 影响更多

直觉上 1000 到 10000 tenant 一个 box 是合理范围,但具体数字要看 workload 特征——tenant 间突发关联性强的场景偏小、关联性弱的偏大。

### 4.8 冷启动的并发控制

几千个冷 tenant 同时激活时不能无限制并行,否则 object storage 带宽打爆。需要:

- 激活请求排队(per-box 级别)  
- 优先激活有活动的 tenant(用户主动发请求 \> 后台任务触发)  
- 元数据永远热,只有数据是冷的——避免"连元数据都要拉"的双重冷启动

---

## 其他选择:用现有工具怎么拼

前面讨论的 agent-native db 是其中一种可能的形态。但不是每个团队都要走这条路——用现有工具拼一套也是合理选择。这一节讨论几种典型拼法,以及它们各自的适用范围和痛点。

### 方案 A:Postgres 作为主要存储，S3 作为归档库

- **Postgres(jsonb)**:agent state、session event、事件日志，**pgvector** 做 embedding 向量检索  
- **Redis**:热点缓存、rate limiting  
- **S3**:冷数据归档(应用层主动搬)  
- 应用层做所有协调

**优点**:单一数据源、事务统一、运维简单、开发快。Supabase / Neon 这类托管 Postgres 服务进一步降低运维成本。

**痛点**:

- 写入量上去之后 autovacuum 跟不上,dead tuple 堆积影响查询  
- Jsonb 深嵌套字段查询效率下降明显  
- Session 多了之后 GIN 索引体积爆炸,update 操作变慢  
- 多租户隔离完全靠应用层,tenant 数量上万后管理复杂度爆炸  
- 冷热分层要自己搬数据,搬迁过程对业务有影响

**适用**:tenant \< 1000、单 agent 数据量小、写入 \< 1000 QPS 的早期产品。大部分 agent 创业公司 MVP 阶段都在这个档位。

### 方案 B:多工具分层

- **Postgres**:关系型数据(用户、计费、配置) 存储 metadata 数据  
- **MongoDB / DynamoDB**:session event、tool call trace 等半结构化数据（context 数据）  
- **Pinecone / Weaviate / Qdrant**:向量检索  
- **Elasticsearch**:全文检索(可选)  
- **S3**:原始 trace 归档  
- **应用层**:做关联和一致性协调

**优点**:每种工作负载用专用工具,单点性能好。可以按需 scale 每一层。

**痛点**:

- 三到五套 db 要维护,运维成本线性增加  
- 跨 db 查询没有事务保证,应用层要处理部分成功的情况  
- Hybrid retrieval(vector \+ filter)只能分两次查,性能打折  
- 数据一致性靠应用层保证,failure recovery 逻辑复杂  
- 每个工具的 tenant 管理各自独立,跨系统的多租户视图难建  
- 成本高——三到五个托管服务的账单加起来不是小数目

**适用**:tenant 规模中等(1000-10000)、有专门基础设施团队、业务需要精细化的读写性能。MongoDB Atlas \+ Pinecone 这种组合在 agent 初创公司里很常见。

### 方案 C:自建架构（类似 Brainstore)

- **TiKV / FoundationDB**:底层分布式 KV  
- **S3**:冷数据和 WAL  
- **Tantivy / Lucene**:倒排索引  
- **Postgres**:元数据  
- **Redis**:coordination  
- **自研**:索引层、查询层、lifecycle 管理

**优点**:量身定做、性能最优、成本结构可控。Brainstore 就是这条路走出来的。

**痛点**:

- 工程量巨大,Braintrust 做了两年  
- 团队要求高——需要有分布式系统、存储引擎经验的工程师  
- 维护成本高,每个组件都要有人 on-call  
- 不是商业化产品,新人上手需要大量文档

**适用**:核心业务重度依赖数据库、规模大到现成方案不够、有资金和人才支持自建的公司。如果你是一家做 agent 平台的 Series B+ 公司,遇到现有方案的墙,这条路可能是必然。

### 无法解决的问题

三种方案各自的痛点列完,还有几个**所有方案都没有解决**的问题——这些是 agent-native db 真正的空间:

1. **没有原生 session lifecycle**。不管哪种方案,"session idle 后冷化、冷数据派生 memory、按需重新激活"这套流程都要应用层自己写,没有 db 支持。  
2. **没有 context fork**。Subagent spawn 时的 context 快照只能应用层深拷贝,性能和成本都差。  
3. **多租户难上百万**。所有方案的 tenant 管理都停在几千到几万的量级,真到 10^6 级别都会崩。  
4. **Hybrid retrieval 不是原子的**。Vector \+ filter 的组合在所有方案下都是分步执行,没有单一 query plan 的优化空间。  
5. **冷数据成本下不来**。方案 A 靠 EBS 扛,方案 B 靠各个托管服务的存储定价,方案 C 能用 S3 但要自己做 tiering。没有方案能做到"95% 数据几乎零成本存着"。

---

## 产品定位

回到开头的主论点——如果 agent 带来的 action 数量跃迁 1000x 以上,operational db 需要重新设计。如果把上面这些串起来做成产品,核心卖点:

- Document 作为一等数据模型,semi-structured 原生支持  
- Object storage 为主存储,scale 无限、成本低  
- Locality 贯穿数据布局、索引、事务、计算,性能和成本双优  
- 事务能力 tunable,单 shard 走轻量路径,跨 shard 按需升级  
- Session lifecycle 原生管理,冷热分层基于业务语义  
- Hybrid retrieval(vector \+ structured \+ full-text)在单一事务视图下  
- Box 架构支持百万级 tenant,长尾小 tenant 边际成本趋近零  
- 多租户逻辑隔离、物理共享、按使用量精细计量

竞品定位:

- **vs DynamoDB**:更便宜(object storage 成本优势)、document 语义更适合 agent、hybrid retrieval 内置  
- **vs MongoDB Atlas**:scale 更好、成本低、agent 原语原生  
- **vs Postgres(Supabase/Neon)**:document native、agent workload 优化  
- **vs Pinecone/Weaviate**:一体化事务、不只做 memory 而是完整 operational db

---

## Open Questions

下面这些我自己没想清楚,想听你们判断。

**1\. Context fork 是不是伪需求?**

我倾向认为这是最有差异化的原语——底层 MVCC 能做、应用层做不好、multi-agent 场景必需。但前提是 multi-agent 会成为主流架构。如果 multi-agent 更多是"LLM 快速切换角色"而不是真的 spawn 多个 agent,这个原语的价值就弱很多。

**2\. Session 边界谁来定义?**

Agent 框架五花八门,LangGraph 的 thread、CrewAI 的 crew run、自建 agent 的 task——没有统一的 session 概念。db 要求应用显式声明 session 是不是门槛太高?还是基于 idle timeout 自动判定就够?

**3\. 派生数据的 LLM 调用成本谁付?**

生成 embedding 调 OpenAI、生成 summary 调 Claude,这些钱谁付?db 不可能替用户付。所以派生流水线必然是应用层调度、db 层存储。那 db 的差异化就只剩"支持 hybrid retrieval"和"lifecycle hook 调度"——够不够硬?

**4\. 冷数据真的需要保留原始 trace 吗?**

如果 95% 冷数据永远不会被原样查回来,只有派生物会被用,是不是可以让用户选择"只保留派生物、原始数据直接丢"? 或者是只保留有限的时间。这个取舍很大程度决定产品的成本结构和叙事。

**5\. Agent 的 db 需求会不会被 MCP \+ 文件系统吃掉?**

现在很多 agent 实际上在用 filesystem \+ markdown 做 state,根本没碰 db。如果 MCP 把"访问结构化数据"这件事标准化了,会不会让专门的 agent db 变成多余?还是反过来——MCP 会催生更多需要 db 支撑的 agent 应用?

**6\. 可观测性和调试是不是应该作为独立维度?**

Agent 经常要 replay、时光回溯、对比不同 run,这对 db 的版本语义有特殊要求。这是不是应该作为 agent-native db 的独立核心需求?还是说用现有维度的组合(session lifecycle \+ append-only \+ snapshot read)就能推导出来?

**7\. 事务模型是否要 tunable?**

2.3 节提到的"默认弱、强一致显式申请",实施时要不要真的做成多个分档产品(基础档/标准档/企业档,对应不同事务级别)?还是统一一个中间档就够?分档能做成定价策略,但增加产品复杂度、客户容易选错。

**8\. Box size 的实际值?**

1000 到 10000 tenant 一个 box 是直觉值,实际应该怎么定?取决于 tenant 之间突发的相关性——这个数据从哪里来?没上线前怎么验证?  
---

## 参考

- Ankur Goyal, [AI observability is a database problem: how Brainstore works](https://x.com/ankrgyl/status/2041206959848735107)  
- PingCAP, [How Manus 1.5 uses TiDB X to let agents ship full-stack apps at scale](https://www.pingcap.com/blog/how-manus-1-5-uses-tidb-x-to-let-agents-ship-full-stack-apps-at-scale/)  
- [SurrealDB](https://surrealdb.com/) 在 TiKV 上做多模接口的尝试

