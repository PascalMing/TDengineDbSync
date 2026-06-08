# TDengineDbSync

TDengine 3.0+ 数据库数据导出与导入工具，基于 Java 21 + Spring Boot 3.4.5 构建，命令行模式运行。

**来源**: [https://github.com/PascalMing](https://github.com/PascalMing)

## 目录

- [功能特性](#功能特性)
- [系统要求](#系统要求)
- [快速开始](#快速开始)
- [配置说明](#配置说明)
- [架构设计](#架构设计)
- [数据流](#数据流)
- [断点恢复机制](#断点恢复机制)
- [性能优化](#性能优化)
- [文件格式](#文件格式)
- [使用示例](#使用示例)

---

## 功能特性

- **双连接模式**: 支持 JDBC 原生连接和 REST API 连接
- **并行导出**: 时间窗口分区并行导出，按可配置的时间窗口（默认5分钟）将时间范围切片，所有分区提交到线程池并行执行，线程数可配置
- **CSV/JSON双格式**: CSV 格式高性能，JSON 格式兼容性好
- **数据压缩**: Gzip 压缩，按文件大小（默认100MB）分文件
- **组合查询条件**: 结构化时间范围(start-time/end-time) + 通用非时间条件 + 超级表级别附加条件
- **导入侧条件过滤**: 支持独立于导出的导入条件（`import-conditions` / `import-stable-conditions`），仅创建和导入满足条件的子表及数据行，实现「导出全量、导入子集」
- **分区目录组织**: 导出按 `{stableName}/{yyyyMMdd}/slice_{HHmmss}_{idx}/` 分目录存储，每个分区独立子目录
- **LIMIT/OFFSET 分页**: 每页通过 SQL 级 LIMIT/OFFSET 拉取，避免 REST API 内存溢出（OOM）
- **断点恢复**: 基于已完成文件的导入/导出恢复，支持中断后跳过已完成分区
- **Schema一致性校验**: 导入时比对超级表DDL，不一致终止并提示
- **高效导入**: 多消费者并行写入、生产者-消费者流水线、子表先创建后仅插入、多表合并INSERT、**多超级表并行导入**
- **连接池**: 基于Semaphore的连接池，限制最大并发连接数，避免连接泄露
- **子表名保持**: CSV 含 `tbname` 列，导入时直接使用原始子表名（REST API模式也支持）
- **子表清单**: 先导出一次结构集，记录子表名和 tag；导入时先执行结构集，再导数据
- **导出/导入库分离**: 支持导出库与导入库不同名，通过 `target-database` 配置
- **进度日志**: 每10秒输出一次进度，启动时打印断点摘要，优雅退出时保存断点

---

## 系统要求

| 项目 | 版本 |
|------|------|
| JDK | 21+ |
| TDengine | 3.0+ |
| Maven | 3.8+ |

---

## 快速开始

### 构建

```bash
mvn clean package -DskipTests
```

### 导出数据

```bash
java -jar target/dbsync-1.0.0.jar \
  --tdengine.mode=export \
  --tdengine.database=mydb \
  --tdengine.connection-mode=jdbc
```

### 导入数据

```bash
java -jar target/dbsync-1.0.0.jar \
  --tdengine.mode=import \
  --tdengine.database=mydb \
  --tdengine.connection-mode=jdbc
```

### 使用 REST API 模式

```bash
java -jar target/dbsync-1.0.0.jar \
  --tdengine.mode=export \
  --tdengine.connection-mode=restful
```

---

## 配置说明

### 完整配置项 (application.yml)

```yaml
tdengine:
  # 连接模式: jdbc(原生,高性能) / restful(REST API,无需客户端驱动)
  connection-mode: jdbc

  # JDBC 原生连接配置
  jdbc:
    driver-class-name: com.taosdata.jdbc.TSDBDriver
    url: jdbc:TAOS://localhost:6030
    username: root
    password: taosdata

  # REST API 连接配置
  restful:
    url: http://localhost:6041
    username: root
    password: taosdata
    connect-timeout: 300000   # REST 连接建立超时，单位 ms
    socket-timeout: 300000    # REST 读超时，单位 ms
    request-timeout: 300000   # 服务端响应等待超时，单位 ms

  # 运行模式: export(导出) / import(导入)
  mode: export

  # 数据文件存储目录
  data-dir: ./data

  # 源数据库名称（导出库）
  database: test_db

  # 导入目标数据库（可选，不设置时与 database 相同）
  # 用于将数据导入到与导出不同的库
  target-database:

  # 指定超级表列表(为空则导出/导入全部超级表)
  super-tables: []

  # 导出起始时间(含), 格式 "yyyy-MM-dd" 或 "yyyy-MM-dd HH:mm:ss"
  # 留空则自动从数据库查询MIN(ts)
  start-time: "2024-01-01"
  # 导出结束时间(不含), 格式同上
  # 留空则自动从数据库查询MAX(ts)
  end-time: "2025-01-01"

  # 通用非时间条件(应用于所有超级表, 如列过滤)
  export-conditions: ""

  # 导出 SQL 是否按时间升序排序。默认 false，以获得更高吞吐
  export-order-by-ts: false

  # 每个超级表的附加非时间条件(用AND与通用条件合并)
  stable-conditions:
    st1: "v1 > 100"
    st2: "status = 'active'"

  # 通用导入过滤条件(应用于所有超级表, 仅导入满足条件的子表及数据)
  import-conditions: ""

  # 每个超级表的附加导入过滤条件(用AND与通用导入条件合并)
  import-stable-conditions:
    st1: "dev = 'dev0'"
    st2: "loc IN ('bj', 'sh')"

  # 每个导出文件大小，单位 MB
  file-size-mb: 100

  # 压缩格式
  compression: gzip

  # 数据文件格式: csv(高性能) / json(兼容)
  format: csv

  # 并行线程数(导出: 按时间窗口分区并行; 导入: 多超级表并行+多消费者写入)
  parallel: 30

  # 导入批量写入大小
  batch-size: 5000

  # 导入流水线队列深度(读取与写入之间的缓冲批次数)
  pipeline-queue-size: 10

  # 连接池大小(0禁用连接池)
  connection-pool-size: 50

  # LIMIT/OFFSET 分页大小(导出)，越大单页数据越多但内存消耗越大
  page-size: 5000

  # 时间窗口大小(分钟)，每个窗口为独立导出分区，越小并行度越高但文件越多
  partition-window-minutes: 5
```

### 配置项说明

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `connection-mode` | Enum | `jdbc` | 连接模式，`jdbc`原生驱动性能更高，`restful`无需安装客户端 |
| `mode` | Enum | `export` | 运行模式，`export`导出或`import`导入 |
| `data-dir` | String | `./data` | 数据文件存储根目录 |
| `database` | String | 必填 | 源数据库名称（导出库） |
| `target-database` | String | `database`值 | 导入目标库，不设置时与`database`相同 |
| `super-tables` | List | `[]` | 指定超级表列表，为空时处理所有超级表 |
| `start-time` | String | `null` | 导出起始时间(含)，留空自动检测 |
| `end-time` | String | `null` | 导出结束时间(不含)，留空自动检测 |
| `export-conditions` | String | `""` | 通用非时间WHERE条件，应用于所有超级表 |
| `export-order-by-ts` | boolean | `false` | 导出 SQL 是否按时间升序排序；默认关闭以提升吞吐 |
| `stable-conditions` | Map | `{}` | 每个超级表的附加WHERE条件，与通用条件AND合并 |
| `import-conditions` | String | `""` | 导入侧通用过滤条件，仅导入满足TAG条件的子表及数据行 |
| `import-stable-conditions` | Map | `{}` | 导入侧每个超级表的附加过滤条件，与`import-conditions` AND 合并 |
| `file-size-mb` | int | `100` | 每个导出文件大小，单位 MB |
| `compression` | Enum | `gzip` | 压缩格式 |
| `format` | Enum | `csv` | 数据格式，`csv`性能最优，`json`兼容性好 |
| `parallel` | int | `30` | 并行线程数，导出时按时间窗口分区并行，导入时多消费者+多超级表并行 |
| `batch-size` | int | `5000` | 导入批量INSERT大小。REST API模式下建议5000+以减少HTTP往返次数 |
| `pipeline-queue-size` | int | `10` | 导入流水线队列深度 |
| `connection-pool-size` | int | `50` | 连接池最大连接数，0禁用连接池 |
| `page-size` | int | `5000` | LIMIT/OFFSET 分页大小，越大单页数据越多但内存消耗越大 |
| `partition-window-minutes` | int | `5` | 时间窗口大小(分钟)，每个窗口为一个独立导出分区，越小并行度越高 |
| `restful.connect-timeout` | int | `300000` | REST 连接建立超时，单位毫秒 |
| `restful.socket-timeout` | int | `300000` | REST 读超时，单位毫秒 |
| `restful.request-timeout` | int | `300000` | REST 请求等待超时，单位毫秒 |

说明: `export-order-by-ts=false` 是默认值，面向大规模导出时优先保证吞吐；如需严格时间有序输出，再显式开启。
导出时间范围自动按 `partition-window-minutes` 划分为 N 分钟的时间窗口，每个窗口为一个独立分区并行导出，
写入各自独立的 `slice_{HHmmss}_{idx}/` 子目录。文件切分按 `file-size-mb` 控制。

导入侧条件过滤（`import-conditions` / `import-stable-conditions`）独立于导出条件运作：
1. **子表层**: reconciliation 阶段读取 manifest 后按 TAG 条件过滤，不满足条件的子表不创建、不校验
2. **数据行层**: `insertBatch` 通过 tbname 匹配跳过被过滤子表的数据行；per-child-table 文件整文件跳过并标记 checkpoint
3. 纯数据列条件（如 `value > 100`）由导出侧处理，导入侧仅做子表名匹配

---

## 架构设计

### 项目结构

```
TDengineDbSync/
├── pom.xml
├── src/main/resources/
│   └── application.yml
└── src/main/java/com/tdengine/dbsync/
    ├── DbSyncApplication.java              # SpringBoot 启动类
    ├── config/
    │   └── SyncProperties.java             # 配置属性映射 (17个配置项, 4个枚举)
    ├── connection/
    │   ├── TdConnection.java               # 连接抽象接口 (Strategy模式)
    │   ├── JdbcConnection.java             # JDBC原生连接实现
    │   ├── RestApiConnection.java          # REST API连接实现 (JDBC-REST桥接)
    │   ├── TdConnectionFactory.java        # 连接工厂 (Factory模式)
    │   └── TdConnectionPool.java           # 连接池 (Object Pool模式, Semaphore实现)
    ├── model/
    │   ├── DataColumn.java                 # 列定义 (名称/类型/长度, 含equals校验)
    │   ├── SuperTableMeta.java             # 超级表元数据 (DDL/列/标签)
    │   ├── SchemaFile.java                 # Schema文件模型
    │   ├── ChildTableMeta.java             # 子表元数据 (tbname+tag值) (新增)
    │   ├── ChildTableManifest.java         # 子表清单文件模型 (新增)
    │   └── ProgressCheckpoint.java         # 断点进度模型 (含lastExportTs)
    ├── exporter/
    │   ├── DataExporter.java               # 导出器接口
    │   └── TdDataExporter.java             # 导出实现 (并行+CSV+时间戳断点)
    ├── importer/
    │   ├── DataImporter.java               # 导入器接口
    │   └── TdDataImporter.java             # 导入实现 (流水线+批量+子表缓存)
    ├── service/
    │   ├── SyncService.java                # 同步调度服务 (Facade模式)
    │   └── CheckpointManager.java          # 断点管理器 (dirty标记+shutdown hook)
    └── runner/
        └── SyncRunner.java                 # CommandLineRunner入口
```

### 设计模式

| 模式 | 应用位置 | 说明 |
|------|---------|------|
| Strategy | `TdConnection` 接口 | JDBC/REST双连接策略切换 |
| Factory | `TdConnectionFactory` | 按配置创建连接实例 |
| Object Pool | `TdConnectionPool` | Semaphore+ConcurrentLinkedQueue 连接池, 限制最大并发 |
| Producer-Consumer | `TdDataImporter` | BlockingQueue解耦读取与写入, PipelineCtx保证线程安全 |
| Template Method | 导出/导入接口 | 统一的执行流程框架 |
| Facade | `SyncService` | 统一入口协调各组件 |
| Dirty Flag | `CheckpointManager` | 仅在有变更时持久化断点 |

### 核心类关系

```
SyncRunner (CommandLineRunner)
  └─ SyncService (Facade)
       ├─ CheckpointManager (断点持久化)
       ├─ TdConnectionFactory
       │    └─ TdConnection <<interface>>
       │         ├─ JdbcConnection
       │         └─ RestApiConnection
       ├─ TdConnectionPool (连接池, 所有连接复用)
       ├─ TdDataExporter (并行导出)
       │    ├─ TdConnectionPool (从池中借连接)
       │    ├─ CheckpointManager (断点读写)
       │    └─ SchemaFile / SuperTableMeta
       └─ TdDataImporter (流水线导入)
            ├─ TdConnectionPool (每个写线程从池借独立连接)
            ├─ CheckpointManager (断点读写)
            └─ SchemaFile / SuperTableMeta
```

---

## 数据流

### 导出数据流

```

                        ┌──────────────────────────────┐

                        │      SyncService.execute()    │

                        └──────────────┬───────────────┘

                                       │

                        ┌──────────────▼───────────────┐

                        │    CheckpointManager.init()   │

                        │  (加载或创建进度断点文件)       │

                        └──────────────┬───────────────┘

                                       │

                   ┌───────────────────▼───────────────────┐

                   │        TdDataExporter.exportData()     │

                   └───────────────────┬───────────────────┘

                                       │

              ┌────────────────────────▼────────────────────┐

              │ 1. 元数据连接 → 查询超级表列表、导出Schema   │

              │    输出: {database}_schema.json              │

              │ 2. 导出子表结构集                             │

              │    输出: structure/{stable}.jsonl.gz         │

              └────────────────────────┬────────────────────┘

                                       │

              ┌────────────────────────▼────────────────────┐

              │ 3. 确定时间范围                                │

              │    - 配置了start-time/end-time → 使用配置值   │

              │    - 未配置 → 自动查询MIN(ts)/MAX(ts)         │

              │ 4. 构建时间窗口分区列表                        │

              │    - 按 partitionWindowMinutes 切分时间范围    │

              │    - 每个窗口: {stableName}/{yyyyMMdd}/       │

              │      slice_{HHmmss}_{idx}/                    │

              └────────────────────────┬────────────────────┘

                                       │

           ┌───────────────────────────▼───────────────────────────┐

           │   并行提交所有分区到线程池 (ExecutorService)             │

           │                                                        │

           │   ┌───────┐ ┌───────┐ ┌───────┐      ┌───────┐       │

           │   │Part-0 │ │Part-1 │ │Part-2 │ ...  │Part-N │       │

           │   │st1    │ │st1    │ │st2    │      │st2    │       │

           │   │00:00~ │ │05:00~ │ │00:00~ │      │10:00~ │       │

           │   │05:00  │ │10:00  │ │05:00  │      │15:00  │       │

           │   └─┬─────┘ └─┬─────┘ └─┬─────┘      └─┬─────┘       │

           │     │ 独立连接  │ 独立连接  │ 独立连接     │ 独立连接     │

           └─────┼──────────┼──────────┼──────────────┼───────────┘

                 │          │          │              │

         ┌───────▼──┐ ┌────▼───┐ ┌────▼───┐      ┌───▼───────┐

         │ 构建SQL:  │ │构建SQL:│ │构建SQL: │      │ 构建SQL:   │

         │ SELECT   │ │SELECT  │ │SELECT  │      │ SELECT    │

         │ tbname,* │ │...     │ │...     │      │ ...       │

         │ FROM db  │ │        │ │        │      │           │

         │ .stable  │ │        │ │        │      │           │

         │ WHERE ts │ │        │ │        │      │           │

         │>= wStart │ │        │ │        │      │           │

         │AND ts<   │ │        │ │        │      │           │

         │ wEnd     │ │        │ │        │      │           │

         │[AND user │ │        │ │        │      │           │

         │ cond.]   │ │        │ │        │      │           │

         └────┬─────┘ └───┬────┘ └───┬────┘      └───┬───────┘

              │           │          │               │

         ┌────▼────┐ ┌───▼────┐ ┌───▼────┐      ┌────▼────┐

         │LIMIT/   │ │LIMIT/  │ │LIMIT/  │      │LIMIT/   │

         │OFFSET   │ │OFFSET  │ │OFFSET  │      │OFFSET   │

         │分页拉取  │ │分页拉取 │ │分页拉取 │      │分页拉取  │

         └────┬────┘ └───┬────┘ └───┬────┘      └───┬─────┘

              │           │          │               │

         ┌────▼──────────▼──┐  ┌────▼───────────────▼──┐

         │ 按 file-size-mb  │  │ 按 file-size-mb       │

         │ 轮转, 写入 .gz    │  │ 轮转, 写入 .gz       │

         │ 输出目录:         │  │ 输出目录:             │

         │ {stableName}/    │  │ {stableName}/        │

         │  {yyyyMMdd}/     │  │  {yyyyMMdd}/         │

         │  slice_*/        │  │  slice_*/            │

         │  {stable}_*.gz   │  │  {stable}_*.gz       │

         └──────────────────┘  └──────────────────────┘

                                       │

                        ┌──────────────▼───────────────┐

                        │ 全部完成 → 删除断点文件        │

                        │ 异常退出 → 保留断点可恢复      │

                        └──────────────────────────────┘

```


### 导入数据流

```
                        ┌──────────────────────────────┐
                        │      SyncService.execute()    │
                        └──────────────┬───────────────┘
                                       │
                   ┌───────────────────▼───────────────────┐
                   │        TdDataImporter.importData()     │
                   └───────────────────┬───────────────────┘
                                       │
              ┌────────────────────────▼────────────────────┐
              │ 1. 加载 {database}_schema.json               │
              │ 2. CREATE DATABASE IF NOT EXISTS             │
              │    (目标库, 可与导出库不同名)                  │
              │ 3. 校验 Schema 一致性 (列名/类型/长度)        │
              │    不一致 → 打印差异并终止                     │
              │ 4. 查询目标库已存在的 super table / child table │
              │ 5. 差集补建缺失子表，并重置 tag 不一致的子表    │
              └────────────────────────┬────────────────────┘
                                       │
           ┌───────────────────────────▼───────────────────────────┐
           │          多超级表并行导入 (ExecutorService)              │
           │                                                        │
           │  ┌──────────────────┐  ┌──────────────────┐            │
           │  │ Pipeline-1: st1  │  │ Pipeline-2: st2  │   ...      │
           │  │ ┌──────────────┐│  │ ┌──────────────┐│            │
           │  │ │Producer-Cons.││  │ │Producer-Cons.││            │
           │  │ │分│ 读文件      ││  │ │分│ 读文件      ││            │
           │  │ │别│ 解压        ││  │ │别│ 解压        ││            │
           │  │ │连│ 解析CSV     ││  │ │连│ 解析CSV     ││            │
           │  │ │接│ 积累批次    ││  │ │接│ 积累批次    ││            │
           │  │ │池│ BlockQ→插入 ││  │ │池│ BlockQ→插入 ││            │
           │  │ └──────────────┘│  │ └──────────────┘│            │
           │  │ 3个消费者虚拟线程│  │ 3个消费者虚拟线程 │            │
           │  └──────────────────┘  └──────────────────┘            │
           │                                                        │
           │ 每个超级表的 Pipeline 完全独立:                          │
           │ - 独立的文件列表、独立的 BlockingQueue                   │
           │ - 独立的 PipelineCtx (线程安全, 无共享可变状态)           │
           │ - 从同一连接池 borrow 连接 (Semaphore 确保不超过上限)      │
           └────────────────────────────────────────────────────────┘
           │
           │  INSERT SQL 构建:                                      │
           │  ┌──────────────────────────────────────────────┐      │
           │  │ 子表不存在:                                     │      │
           │  │   CREATE TABLE ... USING ... TAGS(...)         │      │
           │  │ 子表存在但 tag 不一致:                         │      │
           │  │   ALTER TABLE ... SET TAG                      │      │
           │  │ 子表已存在且 tag 一致:                         │      │
           │  │   INSERT INTO t1 VALUES (...) t2 VALUES (...)  │      │
           │  │   (多表合并, 一条SQL写入多个子表)               │      │
           │  └──────────────────────────────────────────────┘      │
           └────────────────────────────────────────────────────────┘
```
注: Schema校验和子表现状查询/差集补建通过元数据连接完成, 使用单独的非池化连接。
导入阶段每超级表使用独立 Pipeline + PooledTdConnection。

### REST API 兼容性处理

在某些 TDengine 版本中，`information_schema.ins_stables` 表的列名不一致
(如 `table_name` 在某些社区版中不可用)。
`RestApiConnection.getSuperTableNames()` 采用双重策略:

1. **优先使用 `SHOW STABLES`** — 跨版本兼容
2. **仅当 `SHOW STABLES` 失败时回退到 `information_schema`** — 静默回退, 不打印错误日志

JDBC 原生连接始终使用 `SHOW STABLES`，不受此问题影响。

---

## 断点恢复机制

### 设计思路

断点恢复是本工具的核心特性之一，确保大数据量场景下的可靠性。

### 导出断点: 基于已完成分区文件

导出断点记录每个超级表下所有已完成的导出文件（`completedFiles` 映射表）。每个分区写入的数据文件完成时即标记为已完成。

中断恢复时，程序扫描所有分区文件，已完成的文件整体跳过，未完成的分区和文件重新执行。

因为每个分区的时间窗口在构建时是确定的（基于 start-time/end-time + partitionWindowMinutes），

恢复时重新构建的分区列表与中断前一致，已完成的文件不会重复导出。



```

导出 2024-01-01 00:00 ~ 2024-01-01 00:15, partition-window-minutes=5, 在第2个分区中断:



断点记录:

  completedFiles:

    "st1/20240101/slice_000000_00/st1_000000000.gz": 100000

    "st1/20240101/slice_000500_01/st1_000500000.gz": 100000

  currentFile: null

  totalRecords: 200000



恢复导出:

  st1/20240101/slice_000000_00/ → 跳过 (completedFiles 中存在)

  st1/20240101/slice_000500_01/ → 跳过 (completedFiles 中存在)

  st1/20240101/slice_001000_02/ → 重新导出 (未完成)

```


### 导入断点: 基于文件+已提交行偏移

```
导入3个文件, 在第2个文件第3000行时中断:
  - st1_20240101.gz: ✓ 已完成 (按文件大小轮转)
  - st1_20240102.gz: ⚠ 进行中 (已完成3000行)
  - st1_20240103.gz: ✗ 未开始

恢复导入:
  - st1_20240101.gz: 跳过 (标记为已完成)
  - st1_20240102.gz: 跳过前3000行, 从第3001行继续
  - st1_20240103.gz: 正常导入
```

说明:
- 导入恢复按“已成功提交的 batch 前缀”推进
- 如果中断发生在最后一个未提交 batch 中，这个 batch 会在下次启动时重放
- 不会在导入前删除目标数据库，已存在数据会保留并按现状继续导入

### 断点文件格式 (`{database}_progress.json`)

```json
{
  "database": "test_db",
  "mode": "EXPORT",
  "lastUpdateTime": "2024-06-15T14:30:25",
  "timeRangeStart": "2024-01-01T00:00:00",
  "timeRangeEnd": "2025-01-01T00:00:00",
  "stables": {
    "st1": {
      "schemaDone": true,
      "completedFiles": {
        "st1/20240101/slice_000000_00/st1_000000000.gz": 100000,
        "st1/20240101/slice_000500_01/st1_000500000.gz": 100000,
        "st1/20240102/slice_000000_00/st1_000000000.gz": 100000
      },
      "currentFile": null,
      "currentFileOffset": 0,
      "totalRecords": 300000,
      "lastExportTs": null
    }
  }
}
```
```json
{
  "database": "test_db",
  "mode": "EXPORT",
  "lastUpdateTime": "2024-06-15T14:30:25",
  "timeRangeStart": "2024-01-01T00:00:00",
  "timeRangeEnd": "2025-01-01T00:00:00",
  "stables": {
    "st1": {
      "schemaDone": true,
      "completedFiles": {
        "20240115/st1_000000.gz": 100000,
        "20240116/st1_000000.gz": 100000
      },
      "completedDays": ["2024-01-15", "2024-01-16"],
      "currentDay": "2024-01-17",
      "currentFile": "20240117/st1_000000.gz",
      "currentFileOffset": 35000,
      "totalRecords": 235000,
      "lastExportTs": "1705334731000"
    }
  }
}
```

### 断点保存时机

| 时机 | 说明 |
|------|------|
| 每30秒 | 定期自动保存（仅dirty时） |
| 每个block完成 | 导出block写完、导入文件处理完 |
| Ctrl+C / kill | JVM ShutdownHook 保存 |
| 异常退出 | SyncService catch 中主动保存 |
| 全部完成 | 删除断点文件 |

---

## 性能优化

### 优化措施总览

| # | 优化项 | 原方案 | 优化后 | 预估提升 |
|---|--------|--------|--------|---------|
| 1 | 时间窗口分区并行 | 串行遍历超级表 | 时间窗口切分 + LIMIT/OFFSET分页 | 30x+ (窗口数x线程数) |
| 2 | CSV格式替代JSON | 每行Jackson序列化 | StringBuilder直接拼接Tab分隔 | 5-10x (序列化) |
| 3 | 时间戳断点恢复 | 重新查询+逐行跳过 | WHERE ts > lastExportTs | O(n)→O(1) |
| 4 | 去除INDENT_OUTPUT | JSON美化缩进 | 数据文件无缩进 | 减少文件体积50%+ |
| 5 | CSV导入解析 | Jackson逐行反序列化 | 单遍字符串切分 | 5-10x (解析) |
| 6 | SQL预编译 | 逐字段迭代列名 | 预分配StringBuilder+轻量数字检测 | 2x (拼接) |
| 7 | 生产者-消费者流水线 | 串行读→写 | 读取与INSERT流水线化, 多消费者并行写入 | 2-4x (IO等待+并行写入) |
| 8 | 子表差集补建 | 预创建全部子表 | 先查询现状，只补缺失与重置 tag | 大幅减少 DDL |
| 9 | **并行超级表导入** | **逐超级表串行处理** | **ExecutorService并行处理多个超级表, 每个独立流水线** | **Nx (超级表数)** |
| 10 | **连接池(Semaphore)** | **每次连接新创建, 用完就关** | **Semaphore+ConcurrentLinkedQueue复用, 消除CAS竞态** | **减少连接建立/关闭开销, 消除连接浪涌** |
| 11 | **批量大小5000** | **batchSize=300/500** | **batchSize=5000, REST API场景HTTP往返减少90%** | **5-10x (REST API模式)** |
| 12 | **SHOW STABLES代替information_schema** | **SELECT ... FROM information_schema.ins_stables** | **SHOW STABLES优先, information_schema静默回退** | **修复版本兼容性** |
| 13 | **连接池日志降级** | **每次借还都打INFO日志** | **连接建立/关闭/版本查询均为DEBUG级别** | **日志量减少99%** |

### 并行导出详解

**场景1: 多超级表并行**

```
30个超级表, 30线程并行:

Thread-1:  st1  ──── JDBC Connection 1 ────→ st1_*.gz
Thread-2:  st2  ──── JDBC Connection 2 ────→ st2_*.gz
Thread-3:  st3  ──── JDBC Connection 3 ────→ st3_*.gz
  ...        ...
Thread-30: st30 ──── JDBC Connection 30 ───→ st30_*.gz

每个线程独立连接, 互不阻塞
```

**场景2: 单超级表 + 大量子表 → 时间窗口并行**

适用于单/多超级表，将时间范围按 `partition-window-minutes` 切分为独立时间窗口，并行导出：

每个任务只扫描一次超级表，结果直接顺序写入单个输出流，达到 `file-size-mb` 后轮转。这样不会因为子表太多生成大量碎文件。

```
1个超级表 st1, 100000个子表, parallel=30:
每个分区独立查询 + LIMIT/OFFSET 分页拉取，达到 file-size-mb 后轮转文件。
每个分区独立查询，结果通过 LIMIT/OFFSET 分页拉取（page-size 控制每页行数），
避免 REST API 模式下 ResultSet 全量缓冲导致 OOM。
达到 file-size-mb 后自动轮转生成新的 .gz 文件。

时间范围: 2024-01-01 00:00 ~ 00:30, partition-window-minutes=5

Partition-0:  st1  00:00~00:05  ──── LIMIT/OFFSET 分页 ────→ slice_000000_00/st1_*.gz
Partition-1:  st1  00:05~00:10  ──── LIMIT/OFFSET 分页 ────→ slice_000500_01/st1_*.gz
Partition-2:  st1  00:10~00:15  ──── LIMIT/OFFSET 分页 ────→ slice_001000_02/st1_*.gz
  ...
Partition-5:  st1  00:25~00:30  ──── LIMIT/OFFSET 分页 ────→ slice_002500_05/st1_*.gz

每个分区使用独立连接, 提交到线程池并行执行
每个分区的 SQL 使用 WHERE ts >= 'wStart' AND ts < 'wEnd' 精确限定时间窗口
每个分区输出到独立 slice_* 子目录, 按 file-size-mb 轮转文件
所有分区完成后, 删除 checkpoint 文件
```

### 导入流水线详解

```
Producer thread (per super table):           Consumer threads (virtual threads):
┌──────────────────────────────┐            ┌──────────────────────────────┐
│ 扫描文件列表(支持断点续传)     │            │ 从 Queue 取 batch            │
│ 解压 Gzip                    │            │                              │
│ 解析 CSV 行                  │     ┌──→   │ Consumer-0: INSERT            │
│ 积累到 batchSize=5000        │     │      │   (PooledTdConnection)       │
│     │                        │     │      ├──────────────────────────────┤
│     ▼                        │     │      │ Consumer-1: INSERT            │
│ batchQueue.put() ────────────┼── Queue ──→│   (PooledTdConnection)       │
│ (阻塞 if 满载)                │     │      ├──────────────────────────────┤
└──────────────────────────────┘     │      │ Consumer-2: INSERT            │
                                     │      │   (PooledTdConnection)       │
                                     │      ├──────────────────────────────┤
                                     └──→   │ ...                          │
                                            └───────┬──────────────────────┘
                                                     │
                                            ┌────────▼──────────────────────┐
                                            │ TdConnectionPool (Semaphore)  │
                                            │ - borrow(): 获取连接          │
                                            │ - close(): 归还连接到池        │
                                            │ - 所有Pipeline共享同一连接池    │
                                            │ - maxSize=50, 30s超时          │
                                            └───────────────────────────────┘

消费者数量 = min(parallel, CPU核心数)
背压控制: Queue满时生产者阻塞, 确保内存可控
POISON信号: 生产者结束时向每个消费者发送终止信号

多超级表并行:
  importData() 使用 ExecutorService 为每个超级表提交独立 Pipeline:
    pipeline-1 (st1): 独立文件列表, 独立 BlockingQueue, 独立 PipelineCtx
    pipeline-2 (st2): 独立文件列表, 独立 BlockingQueue, 独立 PipelineCtx
    ...
  
  PipelineCtx 内部类封装了每个流水线的可变状态:
    - header: 文件头部分字段(用于识别列位置)
    - tagColumnIndices: 用于旧格式兼容的tag列索引
    - hasTbname: CSV是否含tbname列(新格式始终含)
    - currentFileChildTable: 单子表模式下的子表名
  
  PipelineCtx 非线程安全, 但通过 BlockingQueue 的 happens-before 保证可见性

结构集与数据集分离:
  1. 先执行一次结构集查询，拿到 tbname + tag values
  2. 再执行数据查询，仅选择 tbname + 超级表数据列
  3. 导入时先落结构集，再并行导数据

导出文件切分:
  - 文件大小可配置，单位 MB，默认 100MB
  - 按超级表 + 时间戳命名
  - 同一文件内可混合多个子表的数据
  - 只按文件体积轮转，不按子表拆文件
```

### 连接池设计详解

```
TdConnectionPool (Object Pool)
┌───────────────────────────────────────────────────────┐
│  Semaphore maxSize (公平锁)                            │
│  ConcurrentLinkedQueue<TdConnection> idle             │
│  Supplier<TdConnection> factory                       │
│  volatile boolean destroyed                           │
└───────────────────────┬───────────────────────────────┘
                        │
  borrow()              │          returnConnection()
  ┌─────▼─────┐         │          ┌─────▼─────┐
  │tryAcquire │         │          │idle.offer │
  │(30秒超时)  │         │          │semaphore  │
  │           │         │          │.release() │
  │idle.poll()│         │          └───────────┘
  │ ? 复用     │         │
  │ : factory │         │
  │   .get()  │         │
  └───────────┘         │
                        │
  PooledTdConnection (装饰器模式)
  ┌───────────────────────────────────┐
  │ 持有原始 TdConnection 引用         │
  │ close() → returnConnection(原始)   │
  └───────────────────────────────────┘
```

**为什么选择 Semaphore + ConcurrentLinkedQueue?**

原方案使用 `AtomicInteger created` 计数 + `LinkedBlockingQueue` 存在CAS竞态问题:
- `created` 计数器在并发借还中漂移, 导致 idle 队列满时错误关闭连接
- `LinkedBlockingQueue.offer()` 返回 false 时连接被丢弃

新方案原理:
- `Semaphore(maxSize, fair=true)`: 硬限制最大并发连接数, 无计数器漂移
- `ConcurrentLinkedQueue`: 无界非阻塞队列, 不会因为"队列满"而拒绝归还
- `borrow()`: 先获取信号量许可 → 从 idle 队列取 → 无空闲则创建新连接
- `returnConnection()`: idle.offer() + semaphore.release() — 永远不会失败
- `destroy()`: 设置 destroyed 标记, 排干 idle 队列关闭所有连接, 正在使用的连接在归还时直接关闭

配置: `connection-pool-size: 50` (默认), 设为 `0` 禁用连接池 (降级为每次新建)

### 子表创建优化详解

```sql
-- 首次出现子表 t1: 创建并打标签
INSERT INTO t1 USING st1 TAGS ('device1', 'beijing')
  VALUES (1705334730000, 25.5, 60.2)
        (1705334731000, 25.6, 60.1);

-- 后续写入子表 t1: 仅插入数据, 跳过USING TAGS
INSERT INTO t1 VALUES (1705334732000, 25.7, 59.8);

-- 多表合并INSERT (同一batch中的不同子表)
INSERT INTO t1 VALUES (...) (...) t2 VALUES (...) t3 VALUES (...);
```

---

## 文件格式

### 目录结构

```
./data/
└── {database}/
    ├── {database}_schema.json          # Schema定义文件
    ├── structure/                       # 子表结构集
    │   ├── st1.jsonl.gz                # 超级表 st1 的子表清单
    │   └── st2.jsonl.gz
    ├── {database}_progress.json        # 断点文件(运行中存在, 完成后删除)
    ├── st1/                             # 超级表 st1 的导出数据
    │   ├── 20240115/                    # 日期子目录 (yyyyMMdd)
    │   │   ├── slice_000000_00/         # 分区子目录: 00:00 ~ 00:05 窗口
    │   │   │   ├── st1_000000000.gz    # 第1个数据文件
    │   │   │   └── st1_000000000_2.gz  # 第2个数据文件(超 file-size-mb 轮转)
    │   │   └── slice_000500_01/         # 分区子目录: 00:05 ~ 00:10 窗口
    │   │       └── st1_000500000.gz
    │   └── 20240116/
    │       └── slice_000000_00/
    │           └── st1_000000000.gz
    └── st2/                             # 超级表 st2 的导出数据
        └── 20240115/
            └── slice_000000_00/
                └── st2_000000000.gz
```
### Schema 文件 (`{database}_schema.json`)

```json
{
  "database": "test_db",
  "exportTime": "2024-06-15T14:30:00",
  "superTables": {
    "st1": {
      "name": "st1",
      "createStmt": "CREATE STABLE st1 (ts TIMESTAMP, temperature FLOAT, humidity FLOAT) TAGS (device_id NCHAR(50), location NCHAR(100))",
      "columns": [
        { "name": "ts", "type": "TIMESTAMP", "length": 0 },
        { "name": "temperature", "type": "FLOAT", "length": 0 },
        { "name": "humidity", "type": "FLOAT", "length": 0 }
      ],
      "tags": [
        { "name": "device_id", "type": "NCHAR", "length": 50 },
        { "name": "location", "type": "NCHAR", "length": 100 }
      ]
    }
  }
}
```

### CSV 数据文件 (默认格式)



Tab分隔，首行为列头，仅包含 tbname + 数据列（tag 列不写入数据文件，通过子表结构集单独导出）：



```

tbname	ts	temperature	humidity

t1	1705334730000	25.5	60.2

t1	1705334731000	25.6	60.1

t2	1705334732000	22.1	55.3

```



- `NULL` 值表示为 `\N`

- Timestamp 以毫秒值存储

- 含 Tab/Newline 的字段用双引号包裹

- tbname 为子表名，导入时用于确定写入哪个子表

- tag 值通过 structure/{stable}.jsonl.gz 文件管理（导入时先执行结构集）



### JSON 数据文件 (兼容格式)

每行一条记录 (JSON Lines):

```json
{"tbname":"t1","ts":"2024-01-15T10:15:30Z","temperature":25.5,"humidity":60.2}
{"tbname":"t1","ts":"2024-01-15T10:15:31Z","temperature":25.6,"humidity":60.1}
```

---

## 使用示例

### 示例1: 导出指定时间范围的全量数据

```yaml
tdengine:
  mode: export
  database: iot_db
  start-time: "2024-01-01"
  end-time: "2025-01-01"
  format: csv
  parallel: 30
  file-size-mb: 100
```

导出时间范围自动按 partition-window-minutes 切分为时间窗口分区，
数据文件存入 `{stableName}/{yyyyMMdd}/slice_*/` 子目录。
可通过调整 partition-window-minutes 控制并行粒度（越小并行度越高但文件越多）。

### 示例2: 自动检测时间范围

不配置 start-time/end-time 时，程序自动查询数据库获取 MIN(ts)/MAX(ts)：

```yaml
tdengine:
  mode: export
  database: iot_db
  format: csv
  parallel: 30
```

### 示例3: 为每个超级表定义不同过滤条件

```yaml
tdengine:
  mode: export
  database: iot_db
  start-time: "2024-06-01"
  export-conditions: ""
  stable-conditions:
    temperature: "temperature > 0"
    pressure: "pressure BETWEEN 900 AND 1100"
    vibration: "amplitude > 0.5"
```

每个分区生成的SQL类似（按时间窗口边界）：
- temperature: `WHERE ts >=' {windowStart}' AND ts < '{windowEnd}' AND temperature > 0`
- pressure: `WHERE ts >=' {windowStart}' AND ts < '{windowEnd}' AND pressure BETWEEN 900 AND 1100`

### 示例3b: 导出全量、仅导入满足TAG条件的子表（导入侧条件过滤）

导出时不加过滤条件，导出全部数据；导入时通过 `import-conditions` 仅导入 `dev = 'dev0'` 的子表及数据：

```yaml
# 导出配置（全量导出）
tdengine:
  mode: export
  database: iot_db
  start-time: "2024-06-01"
  export-conditions: ""
  stable-conditions: {}

# 导入配置（仅导入 dev='dev0'）
tdengine:
  mode: import
  database: iot_db
  import-conditions: "dev = 'dev0'"
  import-stable-conditions: {}
```

支持更复杂的导入过滤条件：

```yaml
# 按超级表指定不同导入条件
import-stable-conditions:
  temperature: "dev IN ('dev0', 'dev1') AND loc = 'bj'"
  pressure: "sensor_type = 'digital'"
```

**工作原理**: 
1. 导入 reconciliation 阶段读取子表 manifest 后，按 import conditions 评估每个子表的 TAG 值
2. 不满足条件的子表被跳过（不创建、不校验）
3. 数据导入时通过 tbname 匹配，仅写入已创建子表的数据行

### 示例4: 仅导出/导入指定的超级表

```yaml
tdengine:
  mode: export
  database: iot_db
  super-tables:
    - temperature
    - pressure
```

### 示例5: 导入数据并调节批量大小和连接池

```yaml
tdengine:
  mode: import
  database: iot_db
  batch-size: 5000          # REST API模式下建议5000+
  pipeline-queue-size: 10
  connection-pool-size: 50  # 连接池(0禁用), 所有Pipeline共享
  format: csv
```

REST API 模式下 `batch-size` 对性能影响显著，建议 5000-10000。
JDBC 原生模式下可适当降低（500-2000），因为无 HTTP 往返开销。

### 示例6: 中断后恢复

断点恢复无需任何额外配置，只需使用相同的配置重新运行：

```bash
# 首次运行 (中途Ctrl+C中断)
java -jar dbsync-1.0.0.jar --tdengine.mode=export --tdengine.database=iot_db

# 再次运行 (自动从断点恢复)
java -jar dbsync-1.0.0.jar --tdengine.mode=export --tdengine.database=iot_db

# 日志输出:
# Checkpoint path: ./data/iot_db/iot_db_progress.json
# Resuming from previous checkpoint...
#   [st1] completedFiles=..., currentFile=..., currentFileOffset=...
#   [st1] Resumed file ... from line ...
#   REST connect timeout: 300000 ms
```

### 示例7: 使用REST API连接 (无需安装TDengine客户端)

```yaml
tdengine:
  connection-mode: restful
  restful:
    url: http://tdengine-server:6041
    username: root
    password: taosdata
  connection-pool-size: 50  # REST API下建议启用连接池,减少HTTP连接建立开销
```

REST API 模式下，taos-jdbcdriver 通过 HTTP 协议通信，连接池可显著减少连接建立和关闭的开销。

### 示例8: 导出库与导入库不同名

先配置导出（从生产库导出数据）：

```yaml
tdengine:
  mode: export
  database: hirun_node_monitor      # 导出库
  start-time: "2026-05-01 00:00:00"
  format: csv
```

再配置导入（导入到分析库，库名不同）：

```yaml
tdengine:
  mode: import
  database: hirun_node_monitor      # 数据目录名（源库名）
  target-database: monitor_archive  # 导入目标库（与源库不同名）
  batch-size: 5000
  format: csv
```

`target-database` 不配置时默认与 `database` 相同：

```yaml
tdengine:
  mode: import
  database: hirun_node_monitor      # 即从 hirun_node_monitor 目录读取数据
  # target-database 未配置         # 同时导入到 hirun_node_monitor 库
```

导入不会在开始前删除目标数据库；如果目标库里还有其他业务数据，会保持不动，仅按当前导入流程创建/校验相关超级表和子表并继续写入。

---

## 技术栈

| 组件 | 版本 | 用途 |
|------|------|------|
| Java | 21 | 运行环境 (虚拟线程支持) |
| Spring Boot | 3.4.5 | 基础框架 (非Web命令行应用) |
| taos-jdbcdriver | 3.5.3 | TDengine JDBC驱动 (含REST支持) |
| Jackson | 2.18.x | JSON序列化/反序列化 |
| Commons Compress | 1.27.1 | Gzip压缩/解压 |
| SLF4J + Logback | 2.0.x | 日志框架 |

---

## Schema一致性校验

导入时自动比对源和目标超级表的定义：

**检查项:**
- 列名是否一致（大小写不敏感）
- 列类型是否一致
- 列长度是否一致（对NCHAR/VARCHAR等变长类型）
- 标签名是否一致
- 标签类型和长度是否一致

**不一致时行为:**
- 缺少列/标签 → 终止并提示
- 类型不匹配 → 终止并提示
- 长度不�