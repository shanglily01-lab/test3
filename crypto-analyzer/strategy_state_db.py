"""
strategy_state_db.py
策略状态持久化到 MySQL，替代 JSON 文件方案。
两个策略共用同一张表，通过 (strategy, symbol, stype) 唯一定位一行。
"""
import logging
from decimal import Decimal as _Dec

import pymysql.err

log = logging.getLogger(__name__)

# done_time 默认 0 时，若状态为 DONE，表达式 (now - done_time) > cd 恒成立，冷却形同虚设。
_COOLDOWN_DONE_EPOCH_MIN = 946684800.0  # 2000-01-01 之后视为有效 Unix 秒


def _norm(row: dict) -> dict:
    """将 MySQL DECIMAL 字段统一转 float，避免与 Python float 做运算报 TypeError。"""
    return {k: float(v) if isinstance(v, _Dec) else v for k, v in row.items()}

_CREATE_SQL = """
CREATE TABLE IF NOT EXISTS strategy_state (
  id           BIGINT PRIMARY KEY AUTO_INCREMENT,
  strategy     VARCHAR(32)    NOT NULL,
  symbol       VARCHAR(32)    NOT NULL,
  stype        VARCHAR(32)    NOT NULL,
  state        VARCHAR(16)    NOT NULL DEFAULT 'IDLE',
  pid          BIGINT,
  order_id     VARCHAR(64),
  entry_p      DECIMAL(18,8),
  entry_time   DOUBLE         DEFAULT 0,
  done_time    DOUBLE         DEFAULT 0,
  tp_pct       DECIMAL(6,4)   DEFAULT 0,
  peak         DECIMAL(18,8),
  pump_pct     DECIMAL(6,4),
  peak_pnl_pct DOUBLE         NOT NULL DEFAULT 0,
  entry_ts     BIGINT,
  side         VARCHAR(16),
  last_reason  VARCHAR(32),
  created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_ssym (strategy, symbol, stype),
  KEY idx_state (state),
  KEY idx_pid   (pid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

_ALL_FIELDS = (
    'state', 'pid', 'order_id', 'entry_p', 'entry_time', 'done_time',
    'tp_pct', 'peak', 'pump_pct', 'peak_pnl_pct', 'entry_ts', 'side', 'last_reason',
)


def ensure_table(conn) -> None:
    """建表（幂等，启动时调一次）；旧库补列 peak_pnl_pct。"""
    cur = conn.cursor()
    cur.execute(_CREATE_SQL)
    try:
        cur.execute(
            "ALTER TABLE strategy_state ADD COLUMN peak_pnl_pct DOUBLE NOT NULL DEFAULT 0"
        )
    except pymysql.err.OperationalError as e:
        if e.args[0] != 1060:
            raise
    conn.commit()
    cur.close()


def get_or_create(conn, strategy: str, symbol: str, stype: str, defaults: dict) -> dict:
    """
    读取行，不存在则按 defaults 插入。
    返回完整行 dict（含所有字段）。
    """
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM strategy_state WHERE strategy=%s AND symbol=%s AND stype=%s",
        (strategy, symbol, stype),
    )
    row = cur.fetchone()
    if row:
        cur.close()
        return _norm(row)

    # 插入默认值
    fields = {**{'state': 'IDLE', 'pid': None, 'order_id': None,
                 'entry_p': 0.0, 'entry_time': 0.0, 'done_time': 0.0,
                 'tp_pct': 0.0, 'peak': None, 'pump_pct': None, 'peak_pnl_pct': 0.0,
                 'entry_ts': None, 'side': None, 'last_reason': None},
              **defaults}
    cols = ', '.join(['strategy', 'symbol', 'stype'] + list(fields.keys()))
    vals = ', '.join(['%s'] * (3 + len(fields)))
    cur.execute(
        f"INSERT IGNORE INTO strategy_state ({cols}) VALUES ({vals})",
        [strategy, symbol, stype] + list(fields.values()),
    )
    conn.commit()
    cur.execute(
        "SELECT * FROM strategy_state WHERE strategy=%s AND symbol=%s AND stype=%s",
        (strategy, symbol, stype),
    )
    row = cur.fetchone()
    cur.close()
    return _norm(row) if row else {**fields, 'strategy': strategy, 'symbol': symbol, 'stype': stype}


def update_state(conn, strategy: str, symbol: str, stype: str, **fields) -> None:
    """更新指定字段（只传变化的字段即可）"""
    if not fields:
        return
    allowed = {k: v for k, v in fields.items() if k in _ALL_FIELDS}
    if not allowed:
        log.warning("update_state: 无有效字段 %s", fields)
        return
    set_clause = ', '.join(f"{k}=%s" for k in allowed)
    cur = conn.cursor()
    cur.execute(
        f"UPDATE strategy_state SET {set_clause} WHERE strategy=%s AND symbol=%s AND stype=%s",
        list(allowed.values()) + [strategy, symbol, stype],
    )
    conn.commit()
    cur.close()


def ensure_cooldown_anchor_epoch(
    conn, strategy: str, symbol: str, stype: str, row: dict, now_s_val: float
) -> float:
    """
    平仓冷却计时的起点（Unix 秒）。done_time 无效时用当前时刻写入 DB，避免 now-0 误判已冷却。
    """
    raw = row.get('done_time')
    try:
        t = float(raw) if raw is not None else 0.0
    except (TypeError, ValueError):
        t = 0.0
    if t > _COOLDOWN_DONE_EPOCH_MIN:
        return t
    update_state(conn, strategy, symbol, stype, done_time=now_s_val)
    return now_s_val


def delete_state(conn, strategy: str, symbol: str, stype: str) -> None:
    """删除一行（例如不再需要该标的的状态行时）"""
    cur = conn.cursor()
    cur.execute(
        "DELETE FROM strategy_state WHERE strategy=%s AND symbol=%s AND stype=%s",
        (strategy, symbol, stype),
    )
    conn.commit()
    cur.close()


def list_active(conn, strategy: str, stype: str = None) -> list:
    """查所有非 IDLE 状态行，stype 为 None 时不过滤子类型"""
    cur = conn.cursor()
    if stype:
        cur.execute(
            "SELECT * FROM strategy_state WHERE strategy=%s AND stype=%s AND state!='IDLE'",
            (strategy, stype),
        )
    else:
        cur.execute(
            "SELECT * FROM strategy_state WHERE strategy=%s AND state!='IDLE'",
            (strategy,),
        )
    rows = cur.fetchall()
    cur.close()
    return [_norm(r) for r in rows]


def list_all_stype(conn, strategy: str, stype: str) -> list:
    """查指定 stype 的所有行（含 IDLE），用于汇总展示"""
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM strategy_state WHERE strategy=%s AND stype=%s",
        (strategy, stype),
    )
    rows = cur.fetchall()
    cur.close()
    return [_norm(r) for r in rows]
