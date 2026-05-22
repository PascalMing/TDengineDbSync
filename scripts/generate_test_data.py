#!/usr/bin/env python3
"""
TDengine 测试数据生成脚本
==========================
为 dbsync 项目生成测试数据：
  数据库 dbtest01 (保留365天)
  超级表 ST001 / ST002 (ts TIMESTAMP, value INT, TAG dev VARCHAR(30))
  子表 T1xxxxx / T2xxxxx (xxxxx=1~99999)
  数据从 2026-04-01 起，每秒一条，value = unix秒时间戳 % 100000

注意: 全量数据规模巨大 (约200K子表 × 数百万条/表)，
      请根据实际需要调整下方配置参数。
"""

import argparse
import base64
import concurrent.futures
import datetime
import json
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from math import ceil

try:
    import requests
except ImportError:
    print("请先安装 requests 库: pip install requests")
    sys.exit(1)

# =============================================================================
# 配置区域 — 请根据实际需要修改
# =============================================================================

# TDengine REST API 连接信息
TDENGINE_URL = "http://127.0.0.1:6041/rest/sql"
TDENGINE_USER = "root"
TDENGINE_PASSWORD = "taosdata"

# 目标数据库
DATABASE = "dbtest01"
KEEP_DAYS = 365

# 超级表定义
SUPER_TABLES = [
    {"name": "ST001", "child_prefix": "T1", "child_count": 99999},
    {"name": "ST002", "child_prefix": "T2", "child_count": 99999},
]

# 数据时间范围
DATA_START_TIME = "2026-04-01 00:00:00"

# ★ 结束时间 — 建议先用小范围测试 (如1小时)：
DATA_END_TIME = "2026-04-01 01:00:00"
# 全量数据时改为：
# DATA_END_TIME = "2026-05-22 23:59:59"

# 并行写入线程数
WORKER_THREADS = 8

# 写入时间间隔（秒）
WRITE_INTERVAL_SECONDS = 20

# 批处理参数
BATCH_SIZE = 500               # 每条SQL的总记录数（不超过此值）
TABLES_PER_SQL = 20            # 每条INSERT语句中包含的子表数
# 每子表每次插入记录数 = BATCH_SIZE / TABLES_PER_SQL（在逻辑中计算）

# 最大重试次数
MAX_RETRIES = 3
RETRY_DELAY_SEC = 2

# 请求超时（秒）
REQUEST_TIMEOUT = 60

# 工作线程超时（秒）：等待一个线程完成的最大时间，超时后强制取消
WORKER_TIMEOUT = 120

# =============================================================================
# 脚本逻辑
# =============================================================================

# 认证头
_auth_header = "Basic " + base64.b64encode(
    f"{TDENGINE_USER}:{TDENGINE_PASSWORD}".encode()
).decode()

_print_lock = threading.Lock()
_progress_lock = threading.Lock()
_stats = {"sql_count": 0, "record_count": 0, "error_count": 0}


def log(msg):
    """线程安全的日志输出"""
    with _print_lock:
        ts = datetime.datetime.now().strftime("%H:%M:%S")
        print(f"[{ts}] {msg}")
        sys.stdout.flush()


def make_session():
    """为每个线程创建独立的 HTTP Session，避免连接池竞争和端口耗尽"""
    session = requests.Session()
    session.headers.update({
        "Authorization": _auth_header,
        "Content-Type": "application/json",
    })
    # 单线程只需1个连接，pool_maxsize=1 避免创建多余连接
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=1,
        pool_maxsize=1,
        max_retries=0,
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def execute_sql(session, sql, desc=""):
    """执行SQL并返回JSON结果，失败重试
    
    使用 with 上下文管理器确保响应体被消费、连接回池。
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with session.post(
                TDENGINE_URL,
                data=sql.encode("utf-8"),
                timeout=(REQUEST_TIMEOUT, REQUEST_TIMEOUT),  # (connect_timeout, read_timeout)
                # 禁止 keep-alive 让每个请求独立连接/关闭
                # 避免 TDengine REST 服务端关闭连接造成的 TIME_WAIT
                headers={"Connection": "close"},
            ) as resp:
                # 先消费完整响应体，确保连接可回池
                body = resp.content
                # 只有明确的数据错误才走 SQL 错误分支
                if resp.status_code == 200:
                    try:
                        result = json.loads(body)
                    except json.JSONDecodeError:
                        log(f"  [ERR] 非JSON响应: {body[:200]}")
                        with _progress_lock:
                            _stats["error_count"] += 1
                        return None
                else:
                    result = {"code": -1, "desc": f"HTTP {resp.status_code}: {body[:200]}"}
                if result.get("code", 0) != 0:
                    err_desc = result.get("desc", "unknown")
                    if attempt < MAX_RETRIES:
                        log(f"  [WARN] SQL出错 (尝试 {attempt}/{MAX_RETRIES}): {err_desc}")
                        time.sleep(RETRY_DELAY_SEC)
                        continue
                    else:
                        log(f"  [ERR] SQL失败: {err_desc}")
                        with _progress_lock:
                            _stats["error_count"] += 1
                        return None
            return result
        except requests.exceptions.Timeout:
            if attempt < MAX_RETRIES:
                log(f"  [WARN] 请求超时 (尝试 {attempt}/{MAX_RETRIES})")
                time.sleep(RETRY_DELAY_SEC)
            else:
                log(f"  [ERR] 请求超时，放弃")
                with _progress_lock:
                    _stats["error_count"] += 1
                return None
        except Exception as e:
            if attempt < MAX_RETRIES:
                log(f"  [WARN] 请求异常: {e} (尝试 {attempt}/{MAX_RETRIES})")
                time.sleep(RETRY_DELAY_SEC)
            else:
                log(f"  [ERR] 请求异常: {e}")
                with _progress_lock:
                    _stats["error_count"] += 1
                return None
    return None


def step_create_database(session):
    """第1步：创建数据库"""
    log(f"[1/4] 创建数据库 [{DATABASE}]，保留 {KEEP_DAYS} 天...")
    sql = f"CREATE DATABASE IF NOT EXISTS {DATABASE} KEEP {KEEP_DAYS};"
    result = execute_sql(session, sql, "CREATE DATABASE")
    if result is not None:
        log(f"  [OK] 数据库创建成功")
    return result is not None


def step_create_super_tables(session):
    """第2步：创建超级表"""
    log(f"[2/4] 创建超级表...")
    for st in SUPER_TABLES:
        sql = (
            f"CREATE STABLE IF NOT EXISTS {DATABASE}.{st['name']} "
            f"(ts TIMESTAMP, `value` INT) TAGS(dev VARCHAR(30));"
        )
        result = execute_sql(session, sql, f"CREATE STABLE {st['name']}")
        if result is None:
            log(f"  [ERR] 创建超级表 {st['name']} 失败")
            return False
        log(f"  [OK] 超级表 {st['name']} 创建成功")
    return True


def build_insert_for_batch(stable_name, prefix, child_ids, start_epoch_ms, record_count):
    """
    为一批子表构建一条 SQL INSERT 语句。
    每个子表插入 record_count 条数据，间隔 WRITE_INTERVAL_SECONDS 秒。

    Args:
        stable_name: 超级表名 (如 ST001)
        prefix: 子表前缀 (如 T1)
        child_ids: 当前批次的子表ID列表
        start_epoch_ms: 该批数据起始 epoch 毫秒
        record_count: 每个子表插入的记录数

    Returns:
        SQL 字符串, 预期影响行数
    """
    lines = []
    for cid in child_ids:
        child_name = f"{DATABASE}.{prefix}{cid:05d}"
        dev_value = f"dev{cid // 5000}"
        vals = []
        for i in range(record_count):
            ts_ms = start_epoch_ms + i * WRITE_INTERVAL_SECONDS * 1000
            val = (ts_ms // 1000) % 100000
            vals.append(f"({ts_ms},{val})")
        lines.append(
            f"{child_name} USING {DATABASE}.{stable_name} TAGS('{dev_value}') "
            f"(ts,`value`) VALUES {','.join(vals)}"
        )
    sql = "INSERT INTO " + " ".join(lines) + ";"
    expected_rows = len(child_ids) * record_count
    return sql, expected_rows


def worker_generate(stable_name, prefix, child_ids, start_epoch_ms, total_records,
                    worker_id):
    """
    单个工作线程：负责生成指定子表列表的全部数据。

    Args:
        stable_name: 超级表名
        prefix: 子表前缀
        child_ids: 该线程负责的子表ID列表
        start_epoch_ms: 起始epoch毫秒
        total_records: 每个子表的总记录数
        worker_id: 线程编号（仅用于日志）
    """
    if not child_ids:
        return 0

    # 每个工作线程创建独立的 Session
    session = make_session()
    
    # 每子表每次插入记录数，保证每条SQL总记录数不超过 BATCH_SIZE
    records_per_sql = max(1, BATCH_SIZE // TABLES_PER_SQL)
    local_records = 0

    # 将子表按 TABLES_PER_SQL 分批
    for batch_start in range(0, len(child_ids), TABLES_PER_SQL):
        batch_ids = child_ids[batch_start:batch_start + TABLES_PER_SQL]

        # 将记录数分批
        for rec_offset in range(0, total_records, records_per_sql):
            actual_records = min(records_per_sql, total_records - rec_offset)

            sql, expected = build_insert_for_batch(
                stable_name, prefix, batch_ids,
                start_epoch_ms + rec_offset * WRITE_INTERVAL_SECONDS * 1000,
                actual_records,
            )

            result = execute_sql(session, sql, f"W{worker_id} insert")
            if result is not None:
                rows = result.get("rows", 0)
                local_records += rows
                with _progress_lock:
                    _stats["record_count"] += rows
                    _stats["sql_count"] += 1
            else:
                with _progress_lock:
                    _stats["error_count"] += 1

    return local_records


def step_generate_stable(stable_info, dry_run=False):
    """
    第3步：为一个超级表生成所有子表数据（多线程并行）
    
    Args:
        stable_info: 超级表信息
        dry_run: 如果为True，只统计不实际插入
    """
    st_name = stable_info["name"]
    prefix = stable_info["child_prefix"]
    child_count = stable_info["child_count"]

    log(f"")
    log(f"[3/4] 生成超级表 {st_name} 数据 ({child_count} 个子表)...")

    # 解析时间范围
    fmt = "%Y-%m-%d %H:%M:%S"
    start_dt = datetime.datetime.strptime(DATA_START_TIME, fmt)
    end_dt = datetime.datetime.strptime(DATA_END_TIME, fmt)
    total_seconds = int((end_dt - start_dt).total_seconds())

    if total_seconds <= 0:
        log(f"  [ERR] 时间范围无效: start={DATA_START_TIME}, end={DATA_END_TIME}")
        return False

    start_epoch_ms = int(start_dt.timestamp() * 1000)
    total_records = total_seconds // WRITE_INTERVAL_SECONDS  # 每个子表的实际记录数

    records_per_sql = max(1, BATCH_SIZE // TABLES_PER_SQL)

    log(f"    时间范围: {DATA_START_TIME} ~ {DATA_END_TIME} ({total_seconds} 秒)")
    log(f"    写入间隔: {WRITE_INTERVAL_SECONDS} 秒/条")
    log(f"    批模式: {TABLES_PER_SQL}子表/SQL x {records_per_sql}条/子表 = {TABLES_PER_SQL * records_per_sql}条/SQL")
    rec_per_child = total_records
    rec_total = child_count * rec_per_child
    records_per_sql = max(1, BATCH_SIZE // TABLES_PER_SQL)
    sql_total = ceil(child_count / TABLES_PER_SQL) * ceil(total_records / records_per_sql)
    log(f"    估算: {child_count}子表 × {rec_per_child}条/子表 = {rec_total:,} 条")
    log(f"    估算SQL请求: ~{sql_total:,} 条")
    log(f"    工作线程: {WORKER_THREADS}")
    
    if dry_run:
        log(f"  [DRY RUN] 模式，不实际写入数据")
        return True

    # 分配子表给各线程
    all_ids = list(range(1, child_count + 1))
    chunk_size = ceil(len(all_ids) / WORKER_THREADS)
    chunks = [all_ids[i:i + chunk_size] for i in range(0, len(all_ids), chunk_size)]

    log(f"    启动 {len(chunks)} 个线程...")

    start_wall = time.time()
    deadline = start_wall + WORKER_TIMEOUT
    with ThreadPoolExecutor(max_workers=WORKER_THREADS) as executor:
        futures = {}
        for wid, chunk in enumerate(chunks):
            if not chunk:
                continue
            future = executor.submit(
                worker_generate,
                st_name, prefix, chunk,
                start_epoch_ms, total_records, wid,
            )
            futures[future] = (wid, len(chunk))

        completed = 0
        total_chunks = len(futures)
        try:
            for future in as_completed(futures, timeout=WORKER_TIMEOUT):
                wid, chunk_size = futures[future]
                completed += 1
                try:
                    records = future.result()
                    elapsed = time.time() - start_wall
                    rate = _stats["record_count"] / elapsed if elapsed > 0 else 0
                    with _progress_lock:
                        cur_records = _stats["record_count"]
                        cur_errors = _stats["error_count"]
                        cur_sql = _stats["sql_count"]
                    log(f"    [OK] W{wid} 完成 {records} 条 | "
                        f"总计 {cur_records:,} 条, "
                        f"SQL {cur_sql}, 错误 {cur_errors}, "
                        f"速率 {rate:,.0f} 条/秒")
                except Exception as e:
                    log(f"    [ERR] W{wid} 异常: {e}")
                    with _progress_lock:
                        _stats["error_count"] += 1
        except concurrent.futures.TimeoutError:
            log(f"  [WARN] 工作线程超时 ({WORKER_TIMEOUT}秒)，正在取消剩余线程...")
            # 取消所有未完成的 future
            for future in futures:
                future.cancel()
            executor.shutdown(wait=False)
            log(f"  [WARN] 已取消 {total_chunks - completed} 个未完成线程")
            # 标记未完成部分为错误
            with _progress_lock:
                _stats["error_count"] += total_chunks - completed

    elapsed = time.time() - start_wall
    total = _stats["record_count"]
    log(f"  [OK] 超级表 {st_name} 数据生成完成:")
    log(f"    总记录数: {total:,}")
    log(f"    总耗时: {elapsed:.1f} 秒")
    log(f"    平均速率: {total/elapsed:,.0f} 条/秒")
    log(f"    SQL请求: {_stats['sql_count']}, 错误: {_stats['error_count']}")

    return _stats["error_count"] == 0


def step_print_stats():
    """第4步：输出统计信息"""
    log(f"")
    log(f"[4/4] 数据生成概要")
    for st in SUPER_TABLES:
        log(f"  - 超级表: {st['name']} ({st['child_count']} 个子表)")
    log(f"  - 时间范围: {DATA_START_TIME} ~ {DATA_END_TIME}")
    log(f"  - 总记录数: {_stats['record_count']:,}")
    log(f"  - SQL请求: {_stats['sql_count']}")
    log(f"  - 错误数: {_stats['error_count']}")
    log(f"")
    log(f"完成!")


def main():
    parser = argparse.ArgumentParser(description="TDengine 测试数据生成工具")
    parser.add_argument("--dry-run", action="store_true",
                        help="预览模式：只统计不实际写入数据")
    args = parser.parse_args()

    log("=" * 60)
    log("TDengine 测试数据生成工具")
    if args.dry_run:
        log(" [DRY RUN 模式]")
    log("=" * 60)
    log(f"数据库: {DATABASE}")
    log(f"超级表: {[st['name'] for st in SUPER_TABLES]}")
    log(f"时间范围: {DATA_START_TIME} ~ {DATA_END_TIME}")
    log(f"工作线程: {WORKER_THREADS}")
    log("=" * 60)
    log("")

    # 主线程的 Session（用于建库建表）
    setup_session = make_session()

    # 步骤1: 创建数据库
    if not args.dry_run:
        if not step_create_database(setup_session):
            log("创建数据库失败，退出")
            sys.exit(1)
    else:
        log(f"[1/4] [DRY RUN] 跳过创建数据库 [{DATABASE}]")

    # 步骤2: 创建超级表
    if not args.dry_run:
        if not step_create_super_tables(setup_session):
            log("创建超级表失败，退出")
            sys.exit(1)
    else:
        log(f"[2/4] [DRY RUN] 跳过创建超级表")

    # 步骤3: 生成数据
    total_success = True
    for st in SUPER_TABLES:
        ok = step_generate_stable(st, dry_run=args.dry_run)
        if not ok:
            total_success = False
            log(f"  [WARN] 超级表 {st['name']} 数据生成过程中出现错误")

    # 步骤4: 输出统计
    step_print_stats()

    if not total_success:
        log("[WARN] 过程中出现错误，请检查日志")
        sys.exit(1)


if __name__ == "__main__":
    main()
