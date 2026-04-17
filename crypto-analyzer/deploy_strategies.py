#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
deploy_strategies.py
====================
读取 auto_explore.py 产出的 _passed.csv，把通过四阶段验证的策略自动写入
dimension_trader.py，然后重启 dimension_trader 进程。

用法:
  .venv/Scripts/python.exe deploy_strategies.py                  # 读最新 CSV
  .venv/Scripts/python.exe deploy_strategies.py --csv logs/xxx.csv
  .venv/Scripts/python.exe deploy_strategies.py --dry-run        # 只预览不写文件
"""

import argparse
import csv
import os
import re
import signal
import subprocess
import sys
import textwrap
import time
from datetime import datetime
from pathlib import Path

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

ROOT = Path(__file__).parent
TRADER_FILE = ROOT / "dimension_trader.py"

# 已存在策略的参数集合，避免重复部署（DB 家族）
EXISTING_DB_PARAMS = {
    # (h_n, mac_n, hist_th, f_min, amp1_max, amp4_min)
    (6,  4, 0.002, 0.53, 0.030, None),   # E16
    (8,  4, 0.002, 0.53, None,  None),   # E17
    (6,  4, 0.002, 0.53, 0.018, None),   # E18
    (4,  4, 0.002, 0.53, None,  None),   # E19
    (5,  4, 0.002, 0.53, None,  None),   # E20
    (7,  4, 0.002, 0.53, None,  None),   # E21
    (10, 4, 0.002, 0.53, None,  None),   # E22
    (6,  4, 0.002, 0.53, 0.022, None),   # E23
    (8,  4, 0.002, 0.53, 0.018, None),   # E24
    (6,  4, 0.002, 0.53, None,  0.020),  # E25
    (6,  3, 0.002, 0.53, None,  None),   # E26
    (3,  4, 0.002, 0.53, None,  None),   # E27
    (12, 4, 0.002, 0.53, None,  None),   # E28
    (14, 4, 0.002, 0.53, None,  None),   # E29
    (7,  4, 0.004, 0.53, None,  None),   # E30
}


# ─── 策略名称解析 ──────────────────────────────────────────────────────────────

def parse_db(name: str) -> dict | None:
    """DB_h15 / DB_h6_f55 / DB_h7_mac5 / DB_h6_ht3 等"""
    m = re.match(r"DB_h(\d+)", name)
    if not m:
        return None
    params = {"h_n": int(m.group(1)), "mac_n": 4, "hist_th": 0.002,
              "f_min": 0.53, "amp1_max": None, "amp4_min": None}
    for tok in name.split("_")[2:]:
        if tok.startswith("f") and tok[1:].isdigit():
            params["f_min"] = int(tok[1:]) / 100
        elif tok.startswith("mac") and tok[3:].isdigit():
            params["mac_n"] = int(tok[3:])
        elif tok.startswith("ht") and tok[2:].isdigit():
            params["hist_th"] = int(tok[2:]) / 1000
    return params


def parse_fluxaccel(name: str) -> dict | None:
    """FluxAccel_h6_mac3_f51"""
    m = re.match(r"FluxAccel_h(\d+)_mac(\d+)_f(\d+)", name)
    if not m:
        return None
    return {
        "hist_n": int(m.group(1)),
        "mac_th": int(m.group(2)) / 1000,
        "f_abs":  int(m.group(3)) / 100,
    }


def parse_ovrsold(name: str) -> dict | None:
    """OvrSold_h6_d10_f55"""
    m = re.match(r"OvrSold_h(\d+)_d(\d+)_f(\d+)", name)
    if not m:
        return None
    return {
        "h_n":      int(m.group(1)),
        "depth_th": int(m.group(2)) / 1000,
        "f_min":    int(m.group(3)) / 100,
    }


def parse_btclead(name: str) -> dict | None:
    """BTCLead_b7_h6_f52"""
    m = re.match(r"BTCLead_b(\d+)_h(\d+)_f(\d+)", name)
    if not m:
        return None
    return {
        "btc_mac_th":  int(m.group(1)) / 1000,
        "alt_hist_n":  int(m.group(2)),
        "f_min":       int(m.group(3)) / 100,
    }


def parse_volcomp(name: str) -> dict | None:
    """VolComp_c7_bg5_m2"""
    m = re.match(r"VolComp_c(\d+)_bg(\d+)_m(\d+)", name)
    if not m:
        return None
    return {
        "compress_ratio": int(m.group(1)) / 10,
        "breakout_g":     int(m.group(2)) / 1000,
        "mac_th":         int(m.group(3)) / 1000,
    }


def strategy_type(name: str) -> str:
    if name.startswith("DB_"):         return "db"
    if name.startswith("FluxAccel_"):  return "fluxaccel"
    if name.startswith("OvrSold_"):    return "ovrsold"
    if name.startswith("BTCLead_"):    return "btclead"
    if name.startswith("VolComp_"):    return "volcomp"
    return "unknown"


# ─── 代码生成 ──────────────────────────────────────────────────────────────────

def gen_db_func(fn_name: str, strat_name: str, p: dict,
                train_wr: float, test_wr: float, s3_n: int, test_n: int) -> str:
    key = (p["h_n"], p["mac_n"], p["hist_th"], p["f_min"], p["amp1_max"], p["amp4_min"])
    if key in EXISTING_DB_PARAMS:
        return ""   # 已存在，跳过

    args = [f"h_n={p['h_n']}"]
    if p["mac_n"] != 4:      args.append(f"mac_n={p['mac_n']}")
    if p["hist_th"] != 0.002: args.append(f"hist_th={p['hist_th']}")
    if p["f_min"] != 0.53:   args.append(f"f_min={p['f_min']}")
    if p["amp1_max"] is not None: args.append(f"amp1_max={p['amp1_max']}")
    if p["amp4_min"] is not None: args.append(f"amp4_min={p['amp4_min']}")

    return textwrap.dedent(f"""\
        def {fn_name}(cs1h: list, cs4h: list) -> bool:
            \"\"\"{fn_name}-{strat_name}
            train={train_wr:.1f}% / test={test_wr:.1f}%  n={s3_n}+{test_n}  mtf_self\"\"\"
            return _db_long(cs1h, cs4h, {', '.join(args)})
        """)


def gen_fluxaccel_func(fn_name: str, strat_name: str, p: dict,
                        train_wr: float, test_wr: float, s3_n: int, test_n: int) -> str:
    min1 = max(p["hist_n"] + 3, 10)
    return textwrap.dedent(f"""\
        def {fn_name}(cs1h: list, cs4h: list) -> bool:
            \"\"\"{fn_name}-{strat_name}: flux acceleration LONG
            train={train_wr:.1f}% / test={test_wr:.1f}%  n={s3_n}+{test_n}  mtf_self\"\"\"
            if len(cs1h) < {min1} or len(cs4h) < 8: return False
            if gradient(cs4h, 4) <= {p['mac_th']}: return False
            if gradient(cs1h, {p['hist_n']}) >= -0.001: return False
            f2 = flux(cs1h, 2); f4 = flux(cs1h, 4); f8 = flux(cs1h, 8)
            if not (f2 > f4 * 0.97 and f4 > f8 * 0.97): return False
            if f2 <= {p['f_abs']}: return False
            return True
        """)


def gen_ovrsold_func(fn_name: str, strat_name: str, p: dict,
                      train_wr: float, test_wr: float, s3_n: int, test_n: int) -> str:
    min1 = max(p["h_n"] + 3, 10)
    return textwrap.dedent(f"""\
        def {fn_name}(cs1h: list, cs4h: list) -> bool:
            \"\"\"{fn_name}-{strat_name}: oversold deep bounce LONG
            train={train_wr:.1f}% / test={test_wr:.1f}%  n={s3_n}+{test_n}  mtf_self\"\"\"
            if len(cs1h) < {min1} or len(cs4h) < 8: return False
            if gradient(cs4h, 4) <= 0.003: return False
            if gradient(cs1h, {p['h_n']}) >= -{p['depth_th']}: return False
            if gradient(cs1h, 2) <= 0: return False
            if flux(cs1h, 2) <= {p['f_min']}: return False
            return True
        """)


def gen_btclead_func(fn_name: str, strat_name: str, p: dict,
                      train_wr: float, test_wr: float, s3_n: int, test_n: int) -> str:
    min1 = max(p["alt_hist_n"] + 3, 10)
    return textwrap.dedent(f"""\
        def {fn_name}(cs1h: list, cs4h_btc: list) -> bool:
            \"\"\"{fn_name}-{strat_name}: BTC lead alt LONG
            train={train_wr:.1f}% / test={test_wr:.1f}%  n={s3_n}+{test_n}  mtf_btc\"\"\"
            if len(cs1h) < {min1} or len(cs4h_btc) < 8: return False
            if gradient(cs4h_btc, 4) <= {p['btc_mac_th']}: return False
            if gradient(cs1h, {p['alt_hist_n']}) >= -0.002: return False
            if gradient(cs1h, 2) <= 0: return False
            if flux(cs1h, 2) <= {p['f_min']}: return False
            return True
        """)


def gen_volcomp_func(fn_name: str, strat_name: str, p: dict,
                      train_wr: float, test_wr: float, s3_n: int, test_n: int) -> str:
    return textwrap.dedent(f"""\
        def {fn_name}(cs1h: list, cs4h: list) -> bool:
            \"\"\"{fn_name}-{strat_name}: vol compression breakout LONG
            train={train_wr:.1f}% / test={test_wr:.1f}%  n={s3_n}+{test_n}  mtf_self\"\"\"
            if len(cs1h) < 14 or len(cs4h) < 8: return False
            if gradient(cs4h, 4) <= {p['mac_th']}: return False
            amp_recent = amplitude(cs1h, 2); amp_hist = amplitude(cs1h, 8)
            if amp_hist <= 0 or amp_recent >= amp_hist * {p['compress_ratio']}: return False
            if gradient(cs1h, 2) <= {p['breakout_g']}: return False
            if flux(cs1h, 2) <= {p['f_min']}: return False
            return True
        """)


# ─── compute_signal 插入点 ─────────────────────────────────────────────────────

BTCLEAD_MARKER  = "    # E3-AltDipRecovery"
DBCHECKS_MARKER = "        _db_checks = ["
DBCHECKS_END    = "        for fn, name in _db_checks:"


def find_next_e_number(code: str) -> int:
    """扫描代码里最大的 sig_EXX 编号，返回下一个。"""
    nums = [int(m) for m in re.findall(r"def sig_E(\d+)\(", code)]
    return max(nums, default=30) + 1


def inject_functions(code: str, new_funcs: list[dict]) -> str:
    """
    在 compute_signal 定义行之前插入所有新函数。
    new_funcs: [{"fn_name": ..., "code": ..., "mode": ..., "test_wr": ..., "fn": ...}]
    """
    marker = "\n# -- 信号计算 "
    idx = code.find(marker)
    if idx == -1:
        # 备用：找 compute_signal 定义行
        idx = code.find("\ndef compute_signal(")
    if idx == -1:
        raise RuntimeError("找不到 compute_signal 插入位置")

    func_block = "\n# -- 自动部署策略（deploy_strategies.py）-----------------\n\n"
    for s in new_funcs:
        if s["code"]:
            func_block += s["code"] + "\n"

    return code[:idx] + "\n" + func_block + code[idx:]


def inject_db_checks(code: str, new_db: list[dict]) -> str:
    """把新的 DB/FluxAccel/OvrSold/VolComp mtf_self 策略插入 _db_checks 列表。"""
    if not new_db:
        return code

    start = code.find(DBCHECKS_MARKER)
    end   = code.find(DBCHECKS_END)
    if start == -1 or end == -1:
        print("WARNING: 找不到 _db_checks 列表，mtf_self 策略未注册到 compute_signal")
        return code

    # 提取现有列表内容
    block = code[start:end]

    # 生成新行（按 test_wr 排序已由调用方保证）
    new_lines = []
    for s in sorted(new_db, key=lambda x: -x["test_wr"]):
        new_lines.append(
            f'            ({s["fn_name"]}, "{s["fn_name"]}-{s["strat_name"]}"),  '
            f'# test {s["test_wr"]:.1f}%'
        )

    # 在 _db_checks 列表末尾（]之前）插入
    close_bracket = block.rfind("]")
    if close_bracket == -1:
        print("WARNING: _db_checks 列表末尾 ] 未找到")
        return code

    abs_close = start + close_bracket
    insert_str = "\n" + "\n".join(new_lines) + "\n        "
    return code[:abs_close] + insert_str + code[abs_close:]


def inject_btclead_checks(code: str, new_btc: list[dict]) -> str:
    """在 E3-AltDipRecovery 之前插入 BTCLead LONG 检查块。"""
    if not new_btc:
        return code

    marker_idx = code.find(BTCLEAD_MARKER)
    if marker_idx == -1:
        print("WARNING: 找不到 BTCLead 插入位置（E3 marker）")
        return code

    checks = "\n".join(
        f'    if cs_btc4h and symbol in _long_set and {s["fn_name"]}(cs1h, cs_btc4h):\n'
        f'        sl_pct, tp_pct = sl_tp_from(cs1h)\n'
        f'        return _build_sig("{s["fn_name"]}-{s["strat_name"]}", "LONG", price, sl_pct, tp_pct, cs1h)'
        for s in sorted(new_btc, key=lambda x: -x["test_wr"])
    )
    insert = checks + "\n\n"
    return code[:marker_idx] + insert + code[marker_idx:]


# ─── 进程管理 ──────────────────────────────────────────────────────────────────

def restart_dimension_trader():
    """杀掉旧 dimension_trader 进程，启动新进程。"""
    # 找 PID
    result = subprocess.run(
        ["wmic", "process", "where",
         "CommandLine like '%dimension_trader%' and name='python.exe'",
         "get", "ProcessId"],
        capture_output=True, text=True
    )
    pids = [int(x) for x in re.findall(r"\d+", result.stdout) if int(x) > 4]
    if pids:
        for pid in pids:
            print(f"  Killing dimension_trader PID {pid}...")
            try:
                os.kill(pid, signal.SIGTERM)
            except Exception:
                subprocess.run(["taskkill", "/F", "/PID", str(pid)],
                               capture_output=True)
        time.sleep(2)
    else:
        print("  No running dimension_trader found.")

    print("  Starting dimension_trader...")
    log_dir = ROOT / "logs"
    log_dir.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    log_path = log_dir / f"dimension_trader_{ts}.log"
    python = ROOT / ".venv" / "Scripts" / "python.exe"
    with open(log_path, "w") as lf:
        subprocess.Popen(
            [str(python), str(ROOT / "dimension_trader.py")],
            stdout=lf, stderr=lf,
            creationflags=subprocess.DETACHED_PROCESS if sys.platform == "win32" else 0
        )
    print(f"  Started. Log: {log_path}")


# ─── 主流程 ────────────────────────────────────────────────────────────────────

def find_latest_csv() -> Path | None:
    log_dir = ROOT / "logs"
    csvs = sorted(log_dir.glob("explore_*_passed.csv"), key=lambda p: p.stat().st_mtime)
    return csvs[-1] if csvs else None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv",     default=None, help="指定 _passed.csv 路径")
    parser.add_argument("--dry-run", action="store_true", help="只预览，不修改文件")
    parser.add_argument("--min-test-wr", type=float, default=57.0,
                        help="最低测试胜率 %% (默认 57.0)")
    parser.add_argument("--min-test-n",  type=int,   default=10,
                        help="最低测试信号数 (默认 10)")
    args = parser.parse_args()

    # 找 CSV
    if args.csv:
        csv_path = Path(args.csv)
    else:
        csv_path = find_latest_csv()

    if not csv_path or not csv_path.exists():
        print("ERROR: 找不到 _passed.csv，请先运行 auto_explore.py")
        sys.exit(1)

    print(f"Reading: {csv_path}")

    # 读 CSV
    rows = []
    with open(csv_path, encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            test_wr = float(r["test_wr_pct"])
            test_n  = int(r["test_n"])
            if test_wr >= args.min_test_wr and test_n >= args.min_test_n:
                rows.append(r)

    print(f"Qualifying strategies (test_wr>={args.min_test_wr}%, n>={args.min_test_n}): {len(rows)}")
    if not rows:
        print("No strategies to deploy.")
        return

    # 读 dimension_trader.py
    code = TRADER_FILE.read_text(encoding="utf-8")
    next_e = find_next_e_number(code)
    print(f"Next E number: E{next_e}")

    new_funcs     = []  # all generated info dicts
    new_db_self   = []  # mtf_self strategies for _db_checks
    new_btclead   = []  # mtf_btc strategies for btclead block
    skipped       = []

    for row in sorted(rows, key=lambda r: -float(r["test_wr_pct"])):
        name     = row["name"]
        mode     = row["mode"]
        test_wr  = float(row["test_wr_pct"])
        s3_wr    = float(row["s3_wr_pct"])
        s3_n     = int(row["s3_n"])
        test_n   = int(row["test_n"])
        stype    = strategy_type(name)

        fn_name = f"sig_E{next_e}"
        code_str = ""

        if stype == "db":
            p = parse_db(name)
            if p is None:
                skipped.append((name, "parse error"))
                continue
            key = (p["h_n"], p["mac_n"], p["hist_th"], p["f_min"], p["amp1_max"], p["amp4_min"])
            if key in EXISTING_DB_PARAMS:
                skipped.append((name, "already exists"))
                continue
            code_str = gen_db_func(fn_name, name, p, s3_wr, test_wr, s3_n, test_n)
            EXISTING_DB_PARAMS.add(key)
        elif stype == "fluxaccel":
            p = parse_fluxaccel(name)
            if p is None:
                skipped.append((name, "parse error"))
                continue
            code_str = gen_fluxaccel_func(fn_name, name, p, s3_wr, test_wr, s3_n, test_n)
        elif stype == "ovrsold":
            p = parse_ovrsold(name)
            if p is None:
                skipped.append((name, "parse error"))
                continue
            code_str = gen_ovrsold_func(fn_name, name, p, s3_wr, test_wr, s3_n, test_n)
        elif stype == "btclead":
            p = parse_btclead(name)
            if p is None:
                skipped.append((name, "parse error"))
                continue
            code_str = gen_btclead_func(fn_name, name, p, s3_wr, test_wr, s3_n, test_n)
        elif stype == "volcomp":
            p = parse_volcomp(name)
            if p is None:
                skipped.append((name, "parse error"))
                continue
            code_str = gen_volcomp_func(fn_name, name, p, s3_wr, test_wr, s3_n, test_n)
        else:
            skipped.append((name, "unknown type"))
            continue

        if not code_str:
            skipped.append((name, "already exists (duplicate params)"))
            continue

        entry = {
            "fn_name":    fn_name,
            "strat_name": name,
            "mode":       mode,
            "test_wr":    test_wr,
            "s3_wr":      s3_wr,
            "code":       code_str,
        }
        new_funcs.append(entry)
        if mode == "mtf_btc" and stype == "btclead":
            new_btclead.append(entry)
        elif mode in ("mtf_self", "mtf_btc"):
            new_db_self.append(entry)

        next_e += 1

        print(f"  [E{next_e-1}] {name:50s}  test={test_wr:.1f}%  n={test_n}")

    print()
    if skipped:
        print(f"Skipped {len(skipped)}:")
        for n, r in skipped:
            print(f"  {n}: {r}")
        print()

    if not new_funcs:
        print("Nothing new to deploy.")
        return

    print(f"Deploying {len(new_funcs)} new strategies...")

    if args.dry_run:
        print("[dry-run] Would write the following functions:")
        for s in new_funcs:
            print(f"\n--- {s['fn_name']} ---")
            print(s["code"])
        print("[dry-run] No files modified.")
        return

    # 备份原文件
    backup = TRADER_FILE.with_suffix(".py.bak")
    backup.write_text(code, encoding="utf-8")
    print(f"Backup: {backup}")

    # 注入函数体
    new_code = inject_functions(code, new_funcs)

    # 注入 _db_checks（mtf_self）
    new_code = inject_db_checks(new_code, new_db_self)

    # 注入 btclead 块
    new_code = inject_btclead_checks(new_code, new_btclead)

    # 写回
    TRADER_FILE.write_text(new_code, encoding="utf-8")
    print(f"Updated: {TRADER_FILE}")

    # 重启
    print("\nRestarting dimension_trader...")
    restart_dimension_trader()

    print("\nDone.")
    print(f"Deployed {len(new_funcs)} new strategies:")
    for s in new_funcs:
        print(f"  {s['fn_name']}  {s['strat_name']}  test={s['test_wr']:.1f}%")


if __name__ == "__main__":
    main()
