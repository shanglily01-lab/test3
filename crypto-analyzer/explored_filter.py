#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
explored_filter.py
==================
供 auto_explore_alien*.py 统一使用的 "已探索策略过滤器"。

机制（用户指令 2026-04-17）：
  下次不再跑已经跑过的策略 —— 以 `strategy_params` 表的 `strategy_name`
  作为权威记录。凡是表中已登记的名字，视为"已通过四阶段验证并部署"，
  不再重复送入 validate_4stage。

用法：
    from explored_filter import load_deployed_names, filter_new_strategies

    deployed = load_deployed_names()
    strategies = theme_fn()
    strategies = filter_new_strategies(strategies, deployed)

    # 或者 CLI 带 --force 时绕过：
    strategies = filter_new_strategies(strategies, deployed, force=args.force)
"""

from __future__ import annotations

import os
from typing import Iterable

import pymysql
from dotenv import load_dotenv

load_dotenv()

_DB_CFG = dict(
    host=os.getenv("DB_HOST"),
    port=int(os.getenv("DB_PORT", "3306")),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    database=os.getenv("DB_NAME"),
    charset="utf8mb4",
)


def load_deployed_names() -> set[str]:
    """返回 strategy_params 表里全部 strategy_name 的集合。

    查询失败返回空集（等价于不过滤，脚本正常运行）。
    """
    try:
        conn = pymysql.connect(**_DB_CFG)
        try:
            with conn.cursor() as c:
                c.execute("SELECT strategy_name FROM strategy_params")
                return {r[0] for r in c.fetchall() if r and r[0]}
        finally:
            conn.close()
    except Exception as e:
        print(f"  [WARN] load_deployed_names failed: {e}  (skip filter)")
        return set()


def filter_new_strategies(
    strategies: list[dict],
    deployed: Iterable[str] | None = None,
    force: bool = False,
) -> list[dict]:
    """过滤掉名字已在 deployed 集合里的策略。

    - force=True 时直接返回原列表（配合 CLI --force）。
    - deployed 为 None 时自动调用 load_deployed_names()。
    - 返回新的 list，原列表不修改。
    """
    if force:
        return list(strategies)
    if deployed is None:
        deployed = load_deployed_names()
    deployed_set = set(deployed)
    return [s for s in strategies if s.get("name") not in deployed_set]


def report_skip(theme_name: str, orig_n: int, filtered_n: int) -> None:
    """统一的跳过信息输出。"""
    skipped = orig_n - filtered_n
    if skipped <= 0:
        print(f"  主题: {theme_name}  ({orig_n} 个候选策略)")
    else:
        print(
            f"  主题: {theme_name}  ({filtered_n} 个待跑，"
            f"跳过 {skipped} 个已在 strategy_params)"
        )
