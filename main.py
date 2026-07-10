# main.py
# ==============================================================================
# NASIRPREDICT - ETHIO LOTTERY (FAST KENO) RESEARCH TELEGRAM BOT
# ==============================================================================
# A complete single-file application that:
#   * Runs as a Telegram Bot with inline (in-chat) menus
#   * Logs into https://ethiolottery.et using Playwright (headless for cloud)
#   * Navigates directly to the Fast Keno game URL to save RAM
#   * Scrapes history and monitors live draws
#   * Stores all data in SQLite
#   * Predicts using 14 statistical scoring methods and self-learning weights
#   * Runs automatic backtesting
#
# Author: NasirPredict Build
# ==============================================================================

import os
import sys
import json
import time
import shutil
import sqlite3
import random
import threading
import statistics
import traceback
import asyncio
from datetime import datetime, timedelta
from collections import defaultdict, Counter
from typing import List, Dict, Tuple, Optional, Any

# Third-party libraries
try:
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError, Page, Browser, BrowserContext
    from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
    from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes
    from telegram.constants import ParseMode
except ImportError as e:
    print(f"Missing dependency: {e.name}. Please install requirements.")
    sys.exit(1)

# ==============================================================================
# CONFIGURATION
# ==============================================================================
APP_NAME = "NasirPredict"
APP_VERSION = "3.0 (Optimized Cloud Edition)"
DB_PATH = "nasirpredict.db"
BACKUP_DIR = "backups"
EXPORT_DIR = "exports"
WEBSITE_URL = "https://ethiolottery.et"
GAME_URL = "https://www.ethiolottery.et/en/virtualGames?game=cmka4xvdkytm6p25jaoep9ywu"
KENO_MIN = 1
KENO_MAX = 80
NUMBERS_PER_DRAW = 20
MONITOR_INTERVAL_SEC = 5
PREDICTION_PROMPT_TIMEOUT = 10
DEFAULT_PREDICTION_COUNT = 20
MAX_HISTORY_ON_FIRST_RUN = 5000
SCROLL_PAUSE = 0.8
PAGE_TIMEOUT = 30000
DB_RETRIES = 5
DB_RETRY_DELAY = 0.5

# ==============================================================================
# UTILITY FUNCTIONS
# ==============================================================================
def now_str() -> str: return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
def today_str() -> str: return datetime.now().strftime("%Y-%m-%d")
def safe_int(value, default: int = 0) -> int:
    try: return int(str(value).strip())
    except (ValueError, TypeError): return default
def clamp(value: int, lo: int, hi: int) -> int: return max(lo, min(hi, value))
def pause(seconds: float) -> None: time.sleep(seconds)

# ==============================================================================
# DATABASE LAYER
# ==============================================================================
class Database:
    def __init__(self, path: str = DB_PATH):
        self.path = path
        self.conn: Optional[sqlite3.Connection] = None
        self._connect()
        self._create_schema()

    def _connect(self) -> None:
        for attempt in range(DB_RETRIES):
            try:
                self.conn = sqlite3.connect(self.path, timeout=30, isolation_level=None, check_same_thread=False)
                self.conn.row_factory = sqlite3.Row
                self.conn.execute("PRAGMA journal_mode=WAL;")
                self.conn.execute("PRAGMA foreign_keys=ON;")
                self.conn.execute("PRAGMA busy_timeout=30000;")
                return
            except sqlite3.OperationalError as e:
                time.sleep(DB_RETRY_DELAY)
        raise RuntimeError("Could not connect to database after retries.")

    def _ensure(self) -> sqlite3.Connection:
        try:
            self.conn.execute("SELECT 1;")
            return self.conn
        except Exception:
            self._connect()
            return self.conn

    def execute(self, sql: str, params: Tuple = ()) -> sqlite3.Cursor:
        for attempt in range(DB_RETRIES):
            try:
                return self._ensure().execute(sql, params)
            except sqlite3.OperationalError as e:
                if "locked" in str(e).lower() or "busy" in str(e).lower():
                    time.sleep(DB_RETRY_DELAY)
                    continue
                raise
        raise RuntimeError("DB execute failed after retries.")

    def fetchone(self, sql: str, params: Tuple = ()) -> Optional[sqlite3.Row]:
        return self.execute(sql, params).fetchone()

    def fetchall(self, sql: str, params: Tuple = ()) -> List[sqlite3.Row]:
        return self.execute(sql, params).fetchall()

    def _create_schema(self) -> None:
        stmts = [
            "CREATE TABLE IF NOT EXISTS draws (draw_id INTEGER PRIMARY KEY, draw_date TEXT, draw_time TEXT, numbers TEXT, created_at TEXT)",
            "CREATE TABLE IF NOT EXISTS predictions (id INTEGER PRIMARY KEY AUTOINCREMENT, predicted_at TEXT, target_draw_id INTEGER, count INTEGER, numbers TEXT, hits INTEGER DEFAULT 0, hit_numbers TEXT, actual_draw_id INTEGER, actual_numbers TEXT, resolved INTEGER DEFAULT 0, resolved_at TEXT)",
            "CREATE TABLE IF NOT EXISTS prediction_details (id INTEGER PRIMARY KEY AUTOINCREMENT, prediction_id INTEGER, number INTEGER, frequency_score REAL, trend_score REAL, gap_score REAL, overdue_score REAL, pair_score REAL, triple_score REAL, group_score REAL, odd_even_score REAL, high_low_score REAL, pattern_score REAL, repeat_score REAL, moving_avg_score REAL, rolling_score REAL, similarity_score REAL, final_score REAL, hit INTEGER DEFAULT 0, FOREIGN KEY(prediction_id) REFERENCES predictions(id))",
            "CREATE TABLE IF NOT EXISTS learning_data (method TEXT PRIMARY KEY, total_predictions INTEGER DEFAULT 0, total_hits INTEGER DEFAULT 0, weight REAL DEFAULT 1.0, last_updated TEXT)",
            "CREATE TABLE IF NOT EXISTS machine_state (key TEXT PRIMARY KEY, value TEXT)",
            "CREATE TABLE IF NOT EXISTS scoring_weights (method TEXT PRIMARY KEY, weight REAL, updated_at TEXT)",
            "CREATE TABLE IF NOT EXISTS backtests (id INTEGER PRIMARY KEY AUTOINCREMENT, run_at TEXT, description TEXT, weights_json TEXT, avg_hits REAL, best_hits INTEGER, worst_hits INTEGER, samples INTEGER, selected INTEGER DEFAULT 0)",
            "CREATE INDEX IF NOT EXISTS idx_draws_date ON draws(draw_date);",
            "CREATE INDEX IF NOT EXISTS idx_predictions_target ON predictions(target_draw_id);",
        ]
        for s in stmts: self.execute(s)

        default_methods = ["frequency", "trend", "gap", "overdue", "pair", "triple", "group", "odd_even", "high_low", "pattern", "repeat", "moving_avg", "rolling", "similarity"]
        for m in default_methods:
            self.execute("INSERT OR IGNORE INTO learning_data(method, total_predictions, total_hits, weight, last_updated) VALUES (?, 0, 0, 1.0, ?)", (m, now_str()))
            self.execute("INSERT OR IGNORE INTO scoring_weights(method, weight, updated_at) VALUES (?, 1.0, ?)", (m, now_str()))

        defaults = {"running": "0", "last_draw_id": "0", "predictions_made": "0", "first_run_done": "0", "ethio_user": "", "ethio_pass": ""}
        for k, v in defaults.items():
            self.execute("INSERT OR IGNORE INTO machine_state(key, value) VALUES (?, ?)", (k, v))

    def insert_draw(self, draw_id: int, draw_date: str, draw_time: str, numbers: List[int]) -> bool:
        if draw_id <= 0: return False
        if self.fetchone("SELECT draw_id FROM draws WHERE draw_id=?", (draw_id,)): return False
        self.execute("INSERT INTO draws(draw_id, draw_date, draw_time, numbers, created_at) VALUES (?, ?, ?, ?, ?)", (draw_id, draw_date, draw_time, json.dumps(numbers), now_str()))
        return True

    def get_highest_draw_id(self) -> int:
        row = self.fetchone("SELECT MAX(draw_id) AS m FROM draws")
        return int(row["m"]) if row and row["m"] is not None else 0

    def get_draw_count(self) -> int:
        row = self.fetchone("SELECT COUNT(*) AS c FROM draws")
        return int(row["c"]) if row else 0

    def get_recent_draws(self, limit: int = 500) -> List[Dict[str, Any]]:
        rows = self.fetchall("SELECT draw_id, draw_date, draw_time, numbers FROM draws ORDER BY draw_id DESC LIMIT ?", (limit,))
        return [{"draw_id": int(r["draw_id"]), "date": r["draw_date"], "time": r["draw_time"], "numbers": json.loads(r["numbers"])} for r in rows]

    def get_all_draws(self) -> List[Dict[str, Any]]: return self.get_recent_draws(limit=10_000_000)

    def save_prediction(self, target_draw_id: int, count: int, numbers: List[int], details: List[Dict[str, Any]]) -> int:
        cur = self.execute("INSERT INTO predictions(predicted_at, target_draw_id, count, numbers, hits, resolved) VALUES (?, ?, ?, ?, 0, 0)", (now_str(), target_draw_id, count, json.dumps(numbers)))
        pred_id = cur.lastrowid
        for d in details:
            self.execute("INSERT INTO prediction_details(prediction_id, number, frequency_score, trend_score, gap_score, overdue_score, pair_score, triple_score, group_score, odd_even_score, high_low_score, pattern_score, repeat_score, moving_avg_score, rolling_score, similarity_score, final_score, hit) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (pred_id, d["number"], d.get("frequency",0), d.get("trend",0), d.get("gap",0), d.get("overdue",0), d.get("pair",0), d.get("triple",0), d.get("group",0), d.get("odd_even",0), d.get("high_low",0), d.get("pattern",0), d.get("repeat",0), d.get("moving_avg",0), d.get("rolling",0), d.get("similarity",0), d.get("final",0), 0))
        self.set_state("predictions_made", str(self.get_state_int("predictions_made", 0) + 1))
        return pred_id

    def resolve_prediction(self, prediction_id: int, actual_draw_id: int, actual_numbers: List[int], hits: int, hit_numbers: List[int]) -> None:
        self.execute("UPDATE predictions SET resolved=1, resolved_at=?, actual_draw_id=?, actual_numbers=?, hits=?, hit_numbers=? WHERE id=?", (now_str(), actual_draw_id, json.dumps(actual_numbers), hits, json.dumps(hit_numbers), prediction_id))
        if hit_numbers:
            self.execute(f"UPDATE prediction_details SET hit=1 WHERE prediction_id=? AND number IN ({','.join('?' * len(hit_numbers))})", [prediction_id] + hit_numbers)

    def get_unresolved_predictions(self) -> List[sqlite3.Row]: return self.fetchall("SELECT * FROM predictions WHERE resolved=0 ORDER BY id ASC")
    def get_prediction_details(self, prediction_id: int) -> List[sqlite3.Row]: return self.fetchall("SELECT * FROM prediction_details WHERE prediction_id=?", (prediction_id,))
    def get_prediction_history(self, limit: int = 20) -> List[sqlite3.Row]: return self.fetchall("SELECT * FROM predictions ORDER BY id DESC LIMIT ?", (limit,))

    def update_learning(self, method: str, predicted_count: int, hit_count: int) -> None:
        row = self.fetchone("SELECT * FROM learning_data WHERE method=?", (method,))
        if not row:
            self.execute("INSERT INTO learning_data(method, total_predictions, total_hits, weight, last_updated) VALUES (?, ?, ?, 1.0, ?)", (method, predicted_count, hit_count, now_str()))
            return
        new_total = int(row["total_predictions"]) + predicted_count
        new_hits = int(row["total_hits"]) + hit_count
        hit_rate = new_hits / max(1, new_total)
        weight = clamp(0.5 + (hit_rate * 5.0), 0.1, 5.0)
        self.execute("UPDATE learning_data SET total_predictions=?, total_hits=?, weight=?, last_updated=? WHERE method=?", (new_total, new_hits, weight, now_str(), method))
        self.execute("UPDATE scoring_weights SET weight=?, updated_at=? WHERE method=?", (weight, now_str(), method))

    def get_weights(self) -> Dict[str, float]:
        return {r["method"]: float(r["weight"]) for r in self.fetchall("SELECT method, weight FROM scoring_weights")}

    def set_weight(self, method: str, weight: float) -> None:
        weight = clamp(weight, 0.0, 10.0)
        self.execute("INSERT INTO scoring_weights(method, weight, updated_at) VALUES (?, ?, ?) ON CONFLICT(method) DO UPDATE SET weight=excluded.weight, updated_at=excluded.updated_at", (method, weight, now_str()))
        self.execute("UPDATE learning_data SET weight=? WHERE method=?", (weight, method))

    def get_learning_progress(self) -> List[Dict[str, Any]]:
        return [{"method": r["method"], "predictions": int(r["total_predictions"]), "hits": int(r["total_hits"]), "hit_rate": (int(r["total_hits"]) / int(r["total_predictions"])) if int(r["total_predictions"]) else 0.0, "weight": float(r["weight"])} for r in self.fetchall("SELECT method, total_predictions, total_hits, weight, last_updated FROM learning_data ORDER BY weight DESC")]

    def set_state(self, key: str, value: str) -> None: self.execute("INSERT INTO machine_state(key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value))
    def get_state(self, key: str, default: str = "") -> str:
        row = self.fetchone("SELECT value FROM machine_state WHERE key=?", (key,))
        return row["value"] if row else default
    def get_state_int(self, key: str, default: int = 0) -> int: return safe_int(self.get_state(key, str(default)), default)

    def save_backtest(self, description: str, weights: Dict[str, float], avg_hits: float, best_hits: int, worst_hits: int, samples: int, selected: int = 0) -> int:
        cur = self.execute("INSERT INTO backtests(run_at, description, weights_json, avg_hits, best_hits, worst_hits, samples, selected) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", (now_str(), description, json.dumps(weights), avg_hits, best_hits, worst_hits, samples, selected))
        return cur.lastrowid
    def get_backtests(self, limit: int = 20) -> List[sqlite3.Row]: return self.fetchall("SELECT * FROM backtests ORDER BY id DESC LIMIT ?", (limit,))

    def backup(self) -> str:
        os.makedirs(BACKUP_DIR, exist_ok=True)
        fname = os.path.join(BACKUP_DIR, f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db")
        try: self.conn.commit()
        except: pass
        shutil.copy2(self.path, fname)
        return fname

    def restore(self, backup_path: str) -> bool:
        if not os.path.exists(backup_path): return False
        try: self.conn.close()
        except: pass
        shutil.copy2(backup_path, self.path)
        self._connect()
        self._create_schema()
        return True

    def export_predictions(self, path: str) -> int:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        rows = self.fetchall("SELECT * FROM predictions ORDER BY id ASC")
        with open(path, "w", encoding="utf-8") as f:
            f.write("id,predicted_at,target_draw_id,count,numbers,hits,hit_numbers,actual_draw_id,actual_numbers,resolved,resolved_at\n")
            for r in rows:
                f.write(",".join(str(x) for x in [r["id"], r["predicted_at"], r["target_draw_id"], r["count"], r["numbers"], r["hits"], r["hit_numbers"], r["actual_draw_id"], r["actual_numbers"], r["resolved"], r["resolved_at"]]) + "\n")
        return len(rows)

    def export_draws(self, path: str) -> int:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        rows = self.fetchall("SELECT * FROM draws ORDER BY draw_id ASC")
        with open(path, "w", encoding="utf-8") as f:
            f.write("draw_id,draw_date,draw_time,numbers,created_at\n")
            for r in rows: f.write(f'{r["draw_id"]},{r["draw_date"]},{r["draw_time"]},{r["numbers"]},{r["created_at"]}\n')
        return len(rows)

    def reset_statistics(self) -> None:
        self.execute("DELETE FROM prediction_details;")
        self.execute("DELETE FROM predictions;")
        self.execute("DELETE FROM backtests;")
        self.execute("UPDATE learning_data SET total_predictions=0, total_hits=0, weight=1.0, last_updated=?;", (now_str(),))
        self.execute("UPDATE scoring_weights SET weight=1.0, updated_at=?;", (now_str(),))
        self.set_state("predictions_made", "0")

    def clear_cache(self) -> None:
        for k in ("running", "last_draw_id", "predictions_made"): self.set_state(k, "0")

# ==============================================================================
# ANALYSIS ENGINE
# ==============================================================================
class AnalysisEngine:
    def __init__(self, db: Database): self.db = db

    @staticmethod
    def _ensure_full(history: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return sorted(history, key=lambda d: d["draw_id"])

    def score_frequency(self, history: List[Dict[str, Any]]) -> Dict[int, float]:
        counts = Counter(n for d in history for n in d["numbers"])
        total = sum(counts.values()) or 1
        return {n: (counts.get(n, 0) / total) for n in range(KENO_MIN, KENO_MAX + 1)}

    def score_trend(self, history: List[Dict[str, Any]], window: int = 30) -> Dict[int, float]:
        recent = history[-window:] if len(history) >= window else history
        counts = Counter(n for d in recent for n in d["numbers"])
        total = sum(counts.values()) or 1
        return {n: (counts.get(n, 0) / total) for n in range(KENO_MIN, KENO_MAX + 1)}

    def score_gap(self, history: List[Dict[str, Any]]) -> Dict[int, float]:
        last_seen = {n: None for n in range(KENO_MIN, KENO_MAX + 1)}
        gaps = defaultdict(list)
        for idx, d in enumerate(history):
            for n in d["numbers"]:
                if last_seen[n] is not None: gaps[n].append(idx - last_seen[n])
                last_seen[n] = idx
        return {n: (1.0 / (1.0 + statistics.mean(gaps[n])) if gaps[n] else 0.0) for n in range(KENO_MIN, KENO_MAX + 1)}

    def score_overdue(self, history: List[Dict[str, Any]]) -> Dict[int, float]:
        last_seen = {n: -1 for n in range(KENO_MIN, KENO_MAX + 1)}
        for idx, d in enumerate(history):
            for n in d["numbers"]: last_seen[n] = idx
        total = len(history)
        return {n: ((total - 1 - last_seen[n]) if last_seen[n] >= 0 else total) / max(1, total) for n in range(KENO_MIN, KENO_MAX + 1)}

    def score_pairs(self, history: List[Dict[str, Any]]) -> Dict[int, float]:
        if not history: return {n: 0.0 for n in range(KENO_MIN, KENO_MAX + 1)}
        pair_counts = defaultdict(int)
        for d in history:
            nums = d["numbers"]
            for i in range(len(nums)):
                for j in range(i + 1, len(nums)):
                    pair_counts[(nums[i], nums[j])] += 1
                    pair_counts[(nums[j], nums[i])] += 1
        last_draw = set(history[-1]["numbers"]) if history else set()
        out = {n: 0.0 for n in range(KENO_MIN, KENO_MAX + 1)}
        for n in range(KENO_MIN, KENO_MAX + 1):
            if n in last_draw: continue
            out[n] = sum(pair_counts.get((n, m), 0) for m in last_draw)
        mx = max(out.values()) or 1.0
        return {n: (out[n] / mx) for n in out}

    def score_triples(self, history: List[Dict[str, Any]]) -> Dict[int, float]:
        if len(history) < 2: return {n: 0.0 for n in range(KENO_MIN, KENO_MAX + 1)}
        triple_counts = defaultdict(int)
        for d in history:
            nums = d["numbers"]
            for i in range(len(nums)):
                for j in range(i + 1, len(nums)):
                    for k in range(j + 1, len(nums)):
                        triple_counts[tuple(sorted((nums[i], nums[j], nums[k])))] += 1
        a, b = set(history[-1]["numbers"]), set(history[-2]["numbers"])
        common = a & b
        out = {n: 0.0 for n in range(KENO_MIN, KENO_MAX + 1)}
        for n in range(KENO_MIN, KENO_MAX + 1):
            if n in a or n in b: continue
            out[n] = sum(triple_counts.get(tuple(sorted((n, m1, m2))), 0) for m1 in common for m2 in common if m1 < m2)
        mx = max(out.values()) or 1.0
        return {n: (out[n] / mx) for n in out}

    def score_group_balance(self, history: List[Dict[str, Any]]) -> Dict[int, float]:
        if not history: return {n: 0.0 for n in range(KENO_MIN, KENO_MAX + 1)}
        recent = history[-50:]
        group_counts = [0, 0, 0, 0]
        for d in recent:
            for n in d["numbers"]: group_counts[(n - 1) // 20] += 1
        avg = sum(group_counts) / 4 or 1
        group_score = [max(0.0, (avg - c) / avg) for c in group_counts]
        return {n: group_score[(n - 1) // 20] for n in range(KENO_MIN, KENO_MAX + 1)}

    def score_odd_even(self, history: List[Dict[str, Any]]) -> Dict[int, float]:
        if not history: return {n: 0.0 for n in range(KENO_MIN, KENO_MAX + 1)}
        recent = history[-20:]
        odd = sum(1 for d in recent for n in d["numbers"] if n % 2 == 1)
        even = sum(1 for d in recent for n in d["numbers"] if n % 2 == 0)
        total = odd + even or 1
        return {n: (even / total if n % 2 == 1 else odd / total) for n in range(KENO_MIN, KENO_MAX + 1)}

    def score_high_low(self, history: List[Dict[str, Any]]) -> Dict[int, float]:
        if not history: return {n: 0.0 for n in range(KENO_MIN, KENO_MAX + 1)}
        recent = history[-20:]
        low = sum(1 for d in recent for n in d["numbers"] if n <= 40)
        high = sum(1 for d in recent for n in d["numbers"] if n > 40)
        total = low + high or 1
        return {n: (high / total if n <= 40 else low / total) for n in range(KENO_MIN, KENO_MAX + 1)}

    def score_pattern(self, history: List[Dict[str, Any]]) -> Dict[int, float]:
        if len(history) < 5: return {n: 0.0 for n in range(KENO_MIN, KENO_MAX + 1)}
        last_set = set(history[-1]["numbers"])
        next_counts = Counter()
        for i in range(len(history) - 1):
            if len(last_set & set(history[i]["numbers"])) >= 10:
                for n in history[i + 1]["numbers"]: next_counts[n] += 1
        mx = max(next_counts.values()) if next_counts else 1
        return {n: (next_counts.get(n, 0) / mx) for n in range(KENO_MIN, KENO_MAX + 1)}

    def score_repeat(self, history: List[Dict[str, Any]]) -> Dict[int, float]:
        if len(history) < 2: return {n: 0.0 for n in range(KENO_MIN, KENO_MAX + 1)}
        repeat_counts = Counter()
        for i in range(1, len(history)):
            for n in (set(history[i - 1]["numbers"]) & set(history[i]["numbers"])): repeat_counts[n] += 1
        last_set = set(history[-1]["numbers"])
        mx = max(repeat_counts.values()) if repeat_counts else 1
        return {n: ((repeat_counts.get(n, 0) / mx) if n in last_set else 0.0) for n in range(KENO_MIN, KENO_MAX + 1)}

    def score_moving_avg(self, history: List[Dict[str, Any]], window: int = 10) -> Dict[int, float]:
        if len(history) < window * 2: return self.score_frequency(history)
        recent = self.score_frequency(history[-window:])
        longer = self.score_frequency(history[-window * 4:])
        return {n: max(0.0, recent[n] - longer[n]) for n in range(KENO_MIN, KENO_MAX + 1)}

    def score_rolling(self, history: List[Dict[str, Any]]) -> Dict[int, float]:
        if len(history) < 30: return {n: 0.0 for n in range(KENO_MIN, KENO_MAX + 1)}
        w1 = self.score_frequency(history[-10:])
        w2 = self.score_frequency(history[-20:-10])
        w3 = self.score_frequency(history[-30:-20])
        return {n: (w1[n] - w3[n]) for n in range(KENO_MIN, KENO_MAX + 1)}

    def score_similarity(self, history: List[Dict[str, Any]]) -> Dict[int, float]:
        if len(history) < 6: return {n: 0.0 for n in range(KENO_MIN, KENO_MAX + 1)}
        recent_seq = [set(d["numbers"]) for d in history[-3:]]
        best_matches = []
        for i in range(len(history) - 4):
            seq = [set(history[j]["numbers"]) for j in range(i, i + 3)]
            sim = sum(len(a & b) for a, b in zip(recent_seq, seq)) / 3.0
            best_matches.append((sim, i))
        best_matches.sort(reverse=True)
        next_counts = Counter()
        for sim, i in best_matches[:10]:
            if i + 3 < len(history):
                for n in history[i + 3]["numbers"]: next_counts[n] += sim
        mx = max(next_counts.values()) if next_counts else 1
        return {n: (next_counts.get(n, 0) / mx) for n in range(KENO_MIN, KENO_MAX + 1)}

    def compute_scores(self, history: List[Dict[str, Any]]) -> Tuple[Dict[int, float], Dict[int, Dict[str, float]]]:
        history = self._ensure_full(history)
        weights = self.db.get_weights()
        sub_scores = {
            "frequency": self.score_frequency(history), "trend": self.score_trend(history),
            "gap": self.score_gap(history), "overdue": self.score_overdue(history),
            "pair": self.score_pairs(history), "triple": self.score_triples(history),
            "group": self.score_group_balance(history), "odd_even": self.score_odd_even(history),
            "high_low": self.score_high_low(history), "pattern": self.score_pattern(history),
            "repeat": self.score_repeat(history), "moving_avg": self.score_moving_avg(history),
            "rolling": self.score_rolling(history), "similarity": self.score_similarity(history),
        }
        final, details = {}, {}
        for n in range(KENO_MIN, KENO_MAX + 1):
            weighted_sum, total_weight, d = 0.0, 0.0, {}
            for method, scores in sub_scores.items():
                s = float(scores.get(n, 0.0))
                w = float(weights.get(method, 1.0))
                d[method] = s
                weighted_sum += s * w
                total_weight += w
            d["final"] = weighted_sum / max(0.0001, total_weight)
            final[n] = d["final"]
            details[n] = d
        return final, details

    def predict(self, count: int, history: List[Dict[str, Any]]) -> Tuple[List[int], List[Dict[str, Any]]]:
        count = clamp(count, 1, 20)
        final, details = self.compute_scores(history)
        ranked = sorted(range(KENO_MIN, KENO_MAX + 1), key=lambda n: final[n], reverse=True)
        chosen = []
        if count >= 4:
            groups = {0: [], 1: [], 2: [], 3: []}
            for n in ranked: groups[(n - 1) // 20].append(n)
            for g in range(4):
                if groups[g]: chosen.append(groups[g][0])
            for n in ranked:
                if n not in chosen and len(chosen) < count: chosen.append(n)
                if len(chosen) >= count: break
        else: chosen = ranked[:count]

        odds, evens = [n for n in chosen if n % 2 == 1], [n for n in chosen if n % 2 == 0]
        if len(chosen) >= 6 and (len(odds) == 0 or len(evens) == 0):
            chosen_sorted = sorted(chosen, key=lambda n: final[n])
            replace = chosen_sorted[0]
            parity_need = 0 if replace % 2 == 1 else 1
            for n in ranked:
                if n not in chosen and (n % 2 == parity_need):
                    chosen.remove(replace)
                    chosen.append(n)
                    break
        chosen.sort()
        return chosen, [{"number": n, **details[n]} for n in chosen]

# ==============================================================================
# BACKTESTING ENGINE
# ==============================================================================
class Backtester:
    def __init__(self, db: Database, engine: AnalysisEngine):
        self.db = db
        self.engine = engine

    @staticmethod
    def _evaluate(history: List[Dict[str, Any]], weights: Dict[str, float], test_size: int = 100, predict_count: int = 10) -> Tuple[float, int, int]:
        if len(history) < test_size + 50: return 0.0, 0, 0
        all_hits = []
        for i in range(len(history) - test_size, len(history)):
            train = history[:i]
            if len(train) < 50: continue
            sub_scores = {
                "frequency": AnalysisEngine.score_frequency(train), "trend": AnalysisEngine.score_trend(train),
                "gap": AnalysisEngine.score_gap(train), "overdue": AnalysisEngine.score_overdue(train),
                "pair": AnalysisEngine.score_pairs(train), "triple": AnalysisEngine.score_triples(train),
                "group": AnalysisEngine.score_group_balance(train), "odd_even": AnalysisEngine.score_odd_even(train),
                "high_low": AnalysisEngine.score_high_low(train), "pattern": AnalysisEngine.score_pattern(train),
                "repeat": AnalysisEngine.score_repeat(train), "moving_avg": AnalysisEngine.score_moving_avg(train),
                "rolling": AnalysisEngine.score_rolling(train), "similarity": AnalysisEngine.score_similarity(train),
            }
            final = {}
            for n in range(KENO_MIN, KENO_MAX + 1):
                ws, tw = 0.0, 0.0
                for m, sc in sub_scores.items():
                    w = float(weights.get(m, 1.0))
                    ws += sc.get(n, 0.0) * w
                    tw += w
                final[n] = ws / max(0.0001, tw)
            ranked = sorted(range(KENO_MIN, KENO_MAX + 1), key=lambda n: final[n], reverse=True)
            predicted = set(ranked[:predict_count])
            actual = set(history[i]["numbers"])
            all_hits.append(len(predicted & actual))
        if not all_hits: return 0.0, 0, 0
        return statistics.mean(all_hits), max(all_hits), min(all_hits)

    def run(self, predict_count: int = 10, test_size: int = 80) -> Dict[str, Any]:
        history = self.db.get_all_draws()
        if len(history) < test_size + 50: return {"status": "insufficient_history", "samples": len(history)}
        current_weights = self.db.get_weights()
        candidates = [
            ("current_weights", current_weights),
            ("equal_weights", {k: 1.0 for k in current_weights}),
            ("frequency_heavy", {**{k: 1.0 for k in current_weights}, "frequency": 3.0, "trend": 2.0}),
            ("overdue_heavy", {**{k: 1.0 for k in current_weights}, "overdue": 3.0, "gap": 2.0}),
            ("pattern_heavy", {**{k: 1.0 for k in current_weights}, "pattern": 3.0, "similarity": 2.0}),
            ("repeat_heavy", {**{k: 1.0 for k in current_weights}, "repeat": 3.0, "pair": 2.0}),
            ("trend_heavy", {**{k: 1.0 for k in current_weights}, "trend": 3.0, "rolling": 2.0, "moving_avg": 2.0}),
        ]
        results = []
        for desc, w in candidates:
            avg, best, worst = self._evaluate(history, w, test_size=test_size, predict_count=predict_count)
            results.append({"desc": desc, "weights": w, "avg": avg, "best": best, "worst": worst})
            self.db.save_backtest(desc, w, avg, best, worst, test_size, selected=0)
        best_result = max(results, key=lambda r: r["avg"])
        for m, v in best_result["weights"].items(): self.db.set_weight(m, float(v))
        self.db.execute("UPDATE backtests SET selected=1 WHERE id=(SELECT MAX(id) FROM backtests WHERE description=?)", (best_result["desc"],))
        return {"status": "ok", "best": best_result["desc"], "avg": best_result["avg"]}

# ==============================================================================
# PLAYWRIGHT BROWSER AUTOMATION
# ==============================================================================
class BrowserBot:
    def __init__(self, username: str, password: str, headless: bool = True):
        self.username = username
        self.password = password
        self.headless = headless
        self.playwright = None
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None

    def start(self) -> bool:
        try:
            self.playwright = sync_playwright().start()
            # Cloud-optimized launch flags to prevent RAM crashes
            self.browser = self.playwright.chromium.launch(
                headless=self.headless,
                args=[
                    "--no-sandbox", 
                    "--disable-dev-shm-usage", 
                    "--disable-gpu",
                    "--single-process",
                    "--disable-extensions"
                ]
            )
            self.context = self.browser.new_context(
                viewport={"width": 1280, "height": 800},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
            )
            self.page = self.context.new_page()
            
            # BLOCK IMAGES, FONTS & CSS to save massive amounts of RAM on Railway
            def block_heavy_resources(route):
                if route.request.resource_type in ["image", "media", "font", "stylesheet"]:
                    route.abort()
                else:
                    route.continue_()
            self.page.route("**/*", block_heavy_resources)
            
            self.page.set_default_timeout(PAGE_TIMEOUT)
            return True
        except Exception as e:
            print(f"Browser start failed: {e}")
            return False

    def close(self) -> None:
        try:
            if self.context: self.context.close()
            if self.browser: self.browser.close()
            if self.playwright: self.playwright.stop()
        except: pass

    def _retry(self, fn, retries=3, delay=1.5):
        for attempt in range(retries):
            try: return fn()
            except Exception as e:
                time.sleep(delay)
        raise RuntimeError(f"Failed after {retries} retries")

    def goto_home(self) -> bool:
        try:
            self._retry(lambda: self.page.goto(WEBSITE_URL, wait_until="domcontentloaded"))
            self.page.wait_for_timeout(2000)
            return True
        except Exception as e:
            print(f"Could not load {WEBSITE_URL}: {e}")
            return False

    def login(self) -> bool:
        if not self.goto_home(): return False
        try:
            user_selectors = ['input[type="email"]', 'input[name="email"]', 'input[name="username"]', 'input[placeholder*="email" i]', 'input[placeholder*="user" i]', 'input[id*="email" i]', 'input[id*="user" i]']
            user_field = None
            for sel in user_selectors:
                try:
                    user_field = self.page.wait_for_selector(sel, timeout=4000)
                    if user_field: break
                except: continue

            if not user_field:
                for sel in ['button:has-text("Login")', 'a:has-text("Login")', 'button:has-text("Sign In")', 'a:has-text("Sign In")', 'text=Login', 'text=Sign In']:
                    try:
                        el = self.page.wait_for_selector(sel, timeout=3000)
                        if el:
                            el.click()
                            self.page.wait_for_timeout(1500)
                            break
                    except: continue
                for sel in user_selectors:
                    try:
                        user_field = self.page.wait_for_selector(sel, timeout=4000)
                        if user_field: break
                    except: continue

            if not user_field: return False
            user_field.fill(self.username)
            self.page.wait_for_timeout(500)

            pass_field = None
            for sel in ['input[type="password"]', 'input[name="password"]', 'input[id*="pass" i]']:
                try:
                    pass_field = self.page.wait_for_selector(sel, timeout=4000)
                    if pass_field: break
                except: continue
            if not pass_field: return False
            pass_field.fill(self.password)
            self.page.wait_for_timeout(500)

            submitted = False
            for sel in ['button[type="submit"]', 'button:has-text("Login")', 'button:has-text("Sign In")', 'button:has-text("Log in")', 'input[type="submit"]']:
                try:
                    btn = self.page.wait_for_selector(sel, timeout=3000)
                    if btn:
                        btn.click()
                        submitted = True
                        break
                except: continue
            if not submitted: pass_field.press("Enter")

            self.page.wait_for_timeout(4000)
            body_text = self.page.inner_text("body").lower()
            failure_markers = ["invalid", "incorrect", "wrong password", "login failed", "authentication failed", "no such user", "credentials"]
            for marker in failure_markers:
                if marker in body_text:
                    try:
                        err_el = self.page.query_selector('[class*="error" i], [class*="alert" i], [role="alert"]')
                        if err_el and marker in err_el.inner_text().lower(): return False
                    except: pass

            current_url = self.page.url.lower()
            success_markers = ["dashboard", "profile", "account", "wallet", "balance", "deposit", "logout", "sign out"]
            if any(m in current_url for m in success_markers) or any(m in body_text for m in success_markers): return True
            if "login" in current_url or "signin" in current_url: return False
            return True
        except Exception as e:
            print(f"Login exception: {e}")
            return False

    def open_shamo_game(self) -> bool:
        """
        Navigates directly to the Fast Keno game URL.
        Because the bot logs in first, the session is active and the URL will work perfectly.
        """
        try:
            print("Navigating directly to Fast Keno game URL...")
            self.page.goto(GAME_URL, wait_until="domcontentloaded")
            self.page.wait_for_timeout(4000) 
            return True
        except Exception as e:
            print(f"Failed to open Fast Keno URL: {e}")
            return False

    def open_results_tab(self) -> bool:
        try:
            for sel in ['button:has-text("RESULTS")', 'a:has-text("RESULTS")', 'div:has-text("RESULTS")', 'span:has-text("RESULTS")', 'text=RESULTS']:
                try:
                    el = self.page.wait_for_selector(sel, timeout=4000)
                    if el:
                        el.click()
                        self.page.wait_for_timeout(2000)
                        return True
                except: continue
            try:
                for el in self.page.query_selector_all('button, a, div[role="tab"], li'):
                    try:
                        if "result" in (el.inner_text() or "").strip().lower():
                            el.click()
                            self.page.wait_for_timeout(2000)
                            return True
                    except: continue
            except: pass
            return False
        except: return False

    @staticmethod
    def _parse_draw_numbers(text: str) -> List[int]:
        nums = []
        for tok in text.replace("\n", " ").replace(",", " ").split():
            try:
                n = int(tok)
                if KENO_MIN <= n <= KENO_MAX and n not in nums: nums.append(n)
            except: continue
            if len(nums) >= NUMBERS_PER_DRAW: break
        return nums

    @staticmethod
    def _parse_draw_id(text: str) -> Optional[int]:
        import re
        for cand in re.findall(r"\b(\d{5,9})\b", text):
            n = int(cand)
            if 10000 <= n <= 9999999: return n
        return None

    @staticmethod
    def _parse_date_time(text: str) -> Tuple[str, str]:
        import re
        date_str, time_str = "", ""
        d1 = re.search(r"(\d{4})[-/](\d{1,2})[-/](\d{1,2})", text)
        d2 = re.search(r"(\d{1,2})[-/](\d{1,2})[-/](\d{4})", text)
        if d1: date_str = f"{d1.group(1)}-{int(d1.group(2)):02d}-{int(d1.group(3)):02d}"
        elif d2: date_str = f"{d2.group(3)}-{int(d2.group(2)):02d}-{int(d2.group(1)):02d}"
        t = re.search(r"(\d{1,2}):(\d{2})(?::(\d{2}))?", text)
        if t: time_str = f"{int(t.group(1)):02d}:{t.group(2)}"
        return date_str, time_str

    def _scrape_visible_results(self) -> List[Dict[str, Any]]:
        results = []
        try:
            rows = self.page.query_selector_all('div[class*="result" i], div[class*="draw" i], li[class*="result" i], li[class*="draw" i], tr, div[class*="row" i]')
            seen_ids = set()
            for row in rows:
                try: txt = row.inner_text() or ""
                except: continue
                if not txt.strip(): continue
                draw_id = self._parse_draw_id(txt)
                if not draw_id or draw_id in seen_ids: continue
                nums = self._parse_draw_numbers(txt)
                if len(nums) < 5: continue
                d, t = self._parse_date_time(txt)
                if not d: d = today_str()
                seen_ids.add(draw_id)
                results.append({"draw_id": draw_id, "date": d, "time": t, "numbers": nums[:NUMBERS_PER_DRAW]})
        except: pass
        return results

    def scrape_full_history(self, max_items: int = MAX_HISTORY_ON_FIRST_RUN) -> List[Dict[str, Any]]:
        all_results: Dict[int, Dict[str, Any]] = {}
        try:
            self.page.mouse.wheel(0, -5000)
            self.page.wait_for_timeout(1000)
        except: pass

        no_change_count, last_count, scroll_attempts = 0, 0, 0
        while scroll_attempts < 200 and len(all_results) < max_items:
            visible = self._scrape_visible_results()
            for r in visible:
                if r["draw_id"] not in all_results: all_results[r["draw_id"]] = r
            if len(all_results) == last_count:
                no_change_count += 1
                if no_change_count >= 5: break
            else: no_change_count = 0
            last_count = len(all_results)

            try:
                scroll_container = self.page.query_selector('div[class*="result" i][style*="overflow"], div[class*="scroll" i], div[class*="history" i]')
                if scroll_container: scroll_container.evaluate("e => e.scrollTop = e.scrollHeight")
                else: self.page.mouse.wheel(0, 2000)
            except: self.page.mouse.wheel(0, 2000)

            self.page.wait_for_timeout(SCROLL_PAUSE * 1000)
            scroll_attempts += 1
        return list(all_results.values())

    def scrape_latest_draw(self) -> Optional[Dict[str, Any]]:
        try:
            self.page.mouse.wheel(0, -3000)
            self.page.wait_for_timeout(800)
            visible = self._scrape_visible_results()
            if not visible: return None
            visible.sort(key=lambda r: r["draw_id"], reverse=True)
            return visible[0]
        except: return None

# ==============================================================================
# MACHINE CONTROLLER
# ==============================================================================
class Machine:
    def __init__(self):
        self.db = Database()
        self.engine = AnalysisEngine(self.db)
        self.backtester = Backtester(self.db, self.engine)
        self.browser_bot: Optional[BrowserBot] = None
        self.running = False
        self.monitor_thread: Optional[threading.Thread] = None
        self.stop_event = threading.Event()
        self.current_draw_id = 0
        self.last_prediction: Optional[Dict[str, Any]] = None
        self.current_prediction: Optional[Dict[str, Any]] = None
        self.bot_app = None
        self.loop = None
        self.chat_id = None
        self.awaiting_prediction = False
        self.prediction_event = threading.Event()
        self.prediction_count = 20
        self._restore_last_prediction()

    def _restore_last_prediction(self):
        rows = self.db.get_prediction_history(limit=1)
        if rows:
            r = rows[0]
            self.last_prediction = {
                "id": r["id"], "target_draw_id": r["target_draw_id"], "count": r["count"],
                "numbers": json.loads(r["numbers"]) if r["numbers"] else [],
                "hits": r["hits"], "resolved": bool(r["resolved"]),
            }

    def is_running(self) -> bool: return self.running
    def status_text(self) -> str: return "RUNNING" if self.running else "OFFLINE"

    def get_state_summary(self) -> Dict[str, Any]:
        today, week_ago = today_str(), (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        today_preds = self.db.fetchall("SELECT * FROM predictions WHERE resolved=1 AND date(resolved_at)=?", (today,))
        today_acc = sum(r["hits"] for r in today_preds) / sum(r["count"] for r in today_preds) if today_preds else 0.0
        week_preds = self.db.fetchall("SELECT * FROM predictions WHERE resolved=1 AND date(resolved_at)>=?", (week_ago,))
        week_acc = sum(r["hits"] for r in week_preds) / sum(r["count"] for r in week_preds) if week_preds else 0.0
        return {
            "status": self.status_text(),
            "current_draw_id": self.current_draw_id or self.db.get_highest_draw_id(),
            "db_count": self.db.get_draw_count(),
            "predictions_made": self.db.get_state_int("predictions_made", 0),
            "today_accuracy": today_acc,
            "weekly_accuracy": week_acc,
            "last_prediction": self.last_prediction,
            "current_prediction": self.current_prediction,
        }

    def send_telegram(self, text: str):
        if self.bot_app and self.loop and self.chat_id:
            try:
                asyncio.run_coroutine_threadsafe(
                    self.bot_app.bot.send_message(chat_id=self.chat_id, text=text, parse_mode="HTML"),
                    self.loop
                )
            except: pass

    def setup_browser(self, username: str, password: str) -> bool:
        self.browser_bot = BrowserBot(username, password, headless=True)
        if not self.browser_bot.start(): return False
        if not self.browser_bot.login(): return False
        if not self.browser_bot.open_shamo_game(): return False
        if not self.browser_bot.open_results_tab(): return False
        return True

    def ensure_results_open(self) -> bool:
        if not self.browser_bot or not self.browser_bot.page: return False
        try:
            _ = self.browser_bot.page.title()
        except:
            self.send_telegram("⚠️ Browser session lost. Reconnecting...")
            if not self.setup_browser(self.browser_bot.username, self.browser_bot.password): return False
        try:
            if not self.browser_bot.page.query_selector_all('text=RESULTS'):
                if not self.browser_bot.open_shamo_game(): return False
                if not self.browser_bot.open_results_tab(): return False
            return True
        except: return False

    def catch_up_history(self) -> int:
        if not self.ensure_results_open(): return 0
        highest = self.db.get_highest_draw_id()
        scraped = self.browser_bot.scrape_full_history(max_items=500)
        inserted = 0
        for r in scraped:
            if r["draw_id"] > highest:
                if self.db.insert_draw(r["draw_id"], r["date"], r["time"], r["numbers"]): inserted += 1
        if inserted: self.db.set_state("last_draw_id", str(self.db.get_highest_draw_id()))
        return inserted

    def first_run_scrape(self) -> int:
        if not self.ensure_results_open(): return 0
        scraped = self.browser_bot.scrape_full_history()
        inserted = 0
        for r in scraped:
            if self.db.insert_draw(r["draw_id"], r["date"], r["time"], r["numbers"]): inserted += 1
        self.db.set_state("first_run_done", "1")
        self.db.set_state("last_draw_id", str(self.db.get_highest_draw_id()))
        return inserted

    def make_prediction(self, count: Optional[int] = None) -> Optional[Dict[str, Any]]:
        history = self.db.get_all_draws()
        if len(history) < 5:
            self.send_telegram("⚠️ Not enough history to predict.")
            return None
        if count is None:
            self.send_telegram("🔮 <b>Prediction Requested</b>\nHow many numbers? (1-20)\n<i>Reply within 10 seconds (Default: 20).</i>")
            self.awaiting_prediction = True
            self.prediction_event.clear()
            event_set = self.prediction_event.wait(timeout=10)
            self.awaiting_prediction = False
            if event_set and self.prediction_count:
                count = self.prediction_count
            else:
                count = 20
                self.send_telegram("⏰ <b>Timeout</b> — predicting 20 numbers.")

        numbers, details = self.engine.predict(count, history)
        target_draw_id = self.db.get_highest_draw_id() + 1
        pred_id = self.db.save_prediction(target_draw_id, count, numbers, details)
        self.current_prediction = {"id": pred_id, "target_draw_id": target_draw_id, "count": count, "numbers": numbers, "details": details, "created_at": now_str()}
        self.send_telegram(f"✅ <b>Prediction for Draw {target_draw_id}</b>\nNumbers: <code>{numbers}</code>")
        return self.current_prediction

    def resolve_pending_predictions(self, actual_draw_id: int, actual_numbers: List[int]) -> None:
        pending = self.db.get_unresolved_predictions()
        for p in pending:
            if p["target_draw_id"] <= actual_draw_id:
                predicted = set(json.loads(p["numbers"]))
                actual = set(actual_numbers)
                hits = len(predicted & actual)
                hit_numbers = sorted(predicted & actual)
                self.db.resolve_prediction(p["id"], actual_draw_id, actual_numbers, hits, hit_numbers)
                self.send_telegram(f"📊 <b>Prediction #{p['id']} Resolved</b>\nTarget: Draw {p['target_draw_id']}\nHits: {hits}/{p['count']} ({hit_numbers})")
                self._update_learning_for_prediction(p["id"], hits, p["count"])
                self.last_prediction = {"id": p["id"], "target_draw_id": p["target_draw_id"], "count": p["count"], "numbers": json.loads(p["numbers"]), "hits": hits, "resolved": True}
        if pending and random.random() < 0.15:
            try: self.backtester.run(predict_count=10, test_size=60)
            except: pass

    def _update_learning_for_prediction(self, prediction_id: int, hits: int, count: int) -> None:
        details = self.db.get_prediction_details(prediction_id)
        for method in ["frequency", "trend", "gap", "overdue", "pair", "triple", "group", "odd_even", "high_low", "pattern", "repeat", "moving_avg", "rolling", "similarity"]:
            col = method + "_score"
            scored = [(d, d[col]) for d in details]
            scored.sort(key=lambda x: x[1], reverse=True)
            top_half = scored[: max(1, len(scored) // 2)]
            hit_count = sum(1 for d, _ in top_half if d["hit"])
            self.db.update_learning(method, max(1, len(top_half)), hit_count)

    def _monitor_loop(self) -> None:
        last_known = self.db.get_highest_draw_id()
        self.current_draw_id = last_known
        consecutive_errors = 0
        while not self.stop_event.is_set():
            try:
                if not self.ensure_results_open():
                    time.sleep(MONITOR_INTERVAL_SEC * 2)
                    continue
                latest = self.browser_bot.scrape_latest_draw()
                if not latest:
                    time.sleep(MONITOR_INTERVAL_SEC)
                    continue
                if latest["draw_id"] > last_known:
                    self.send_telegram(f"🆕 <b>New draw detected!</b>\nDraw ID: {latest['draw_id']}\nNumbers: <code>{latest['numbers']}</code>")
                    self.db.insert_draw(latest["draw_id"], latest["date"], latest["time"], latest["numbers"])
                    self.current_draw_id = latest["draw_id"]
                    self.db.set_state("last_draw_id", str(latest["draw_id"]))
                    self.resolve_pending_predictions(latest["draw_id"], latest["numbers"])
                    self.make_prediction()
                    last_known = latest["draw_id"]
                consecutive_errors = 0
            except Exception as e:
                consecutive_errors += 1
                if consecutive_errors > 10:
                    time.sleep(30)
                    consecutive_errors = 0
            for _ in range(MONITOR_INTERVAL_SEC * 2):
                if self.stop_event.is_set(): break
                time.sleep(0.5)

    def start(self, username: str, password: str) -> bool:
        if self.running: return True
        self.send_telegram("⚙️ <b>Starting NasirPredict...</b>\nInitializing browser and logging in. This may take a minute.")
        first_run = self.db.get_state_int("first_run_done", 0) == 0
        if not self.browser_bot or not self.browser_bot.page:
            if not self.setup_browser(username, password):
                self.send_telegram("❌ <b>Login Failed</b>\nCould not log in. Please check your credentials and try again.")
                return False
        if first_run:
            self.send_telegram("📊 <b>First Start</b>\nScraping full history. This will take several minutes. Please wait...")
            self.first_run_scrape()
            self.send_telegram(f"✅ History scrape complete. Imported {self.db.get_draw_count()} draws.")
        else:
            self.send_telegram("🔄 Catching up on missing history...")
            imported = self.catch_up_history()
            self.send_telegram(f"✅ Catch-up complete. Imported {imported} new draws.")
        if not self.db.get_unresolved_predictions(): self.make_prediction()
        self.running = True
        self.stop_event.clear()
        self.db.set_state("running", "1")
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
        self.send_telegram("🟢 <b>Machine STARTED</b>\nLive monitoring is active.")
        return True

    def stop(self) -> None:
        if not self.running: return
        self.stop_event.set()
        self.running = False
        self.db.set_state("running", "0")
        if self.monitor_thread: self.monitor_thread.join(timeout=10)
        self.send_telegram("🔴 <b>Machine STOPPED</b>\nUse /start to resume.")

# ==============================================================================
# TELEGRAM BOT INTERFACE
# ==============================================================================
class TelegramBot:
    def __init__(self, token: str, machine: Machine):
        self.token = token
        self.machine = machine
        self.app = Application.builder().token(token).post_init(self.post_init).build()
        self.chat_id = None
        self.state = "MAIN"
        self.weight_method = ""

        self.app.add_handler(CommandHandler("start", self.cmd_start))
        self.app.add_handler(CallbackQueryHandler(self.on_callback))
        self.app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.on_message))
        self.app.add_handler(MessageHandler(filters.Document.ALL, self.on_document))

    async def post_init(self, app):
        self.machine.loop = asyncio.get_running_loop()
        self.machine.bot_app = app

    def main_menu_keyboard(self):
        kb = [
            [InlineKeyboardButton("🔄 Refresh Status", callback_data="cb_status")],
            [InlineKeyboardButton("🔴 Stop Machine" if self.machine.is_running() else "🟢 Start Machine", callback_data="cb_stop" if self.machine.is_running() else "cb_start")],
            [InlineKeyboardButton("🔮 Predict Now", callback_data="cb_predict")],
            [InlineKeyboardButton("⚙️ Settings", callback_data="cb_settings")]
        ]
        return InlineKeyboardMarkup(kb)

    def settings_keyboard(self):
        kb = [
            [InlineKeyboardButton("🚪 Logout", callback_data="cb_logout"), InlineKeyboardButton("🗑 Reset Stats", callback_data="cb_reset_stats")],
            [InlineKeyboardButton("📅 Today", callback_data="cb_perf_today"), InlineKeyboardButton("🗓 Weekly", callback_data="cb_perf_week"), InlineKeyboardButton("📆 Monthly", callback_data="cb_perf_month")],
            [InlineKeyboardButton("📜 Pred History", callback_data="cb_pred_history"), InlineKeyboardButton("📊 Pred Stats", callback_data="cb_pred_stats")],
            [InlineKeyboardButton("📈 Current Scores", callback_data="cb_scores"), InlineKeyboardButton("🗄 DB Stats", callback_data="cb_db_stats")],
            [InlineKeyboardButton("🧠 Learning", callback_data="cb_learning"), InlineKeyboardButton("🔬 Backtests", callback_data="cb_backtest")],
            [InlineKeyboardButton("⚖️ Change Weights", callback_data="cb_weights")],
            [InlineKeyboardButton("💾 Backup DB", callback_data="cb_backup"), InlineKeyboardButton("📥 Restore DB", callback_data="cb_restore")],
            [InlineKeyboardButton("📤 Export Preds", callback_data="cb_export_preds"), InlineKeyboardButton("📤 Export Draws", callback_data="cb_export_draws")],
            [InlineKeyboardButton("🧹 Clear Cache", callback_data="cb_clear_cache")],
            [InlineKeyboardButton("⬅️ Back", callback_data="cb_main")]
        ]
        return InlineKeyboardMarkup(kb)

    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        self.chat_id = update.effective_chat.id
        self.machine.chat_id = self.chat_id
        user = self.machine.db.get_state("ethio_user")
        pwd = self.machine.db.get_state("ethio_pass")
        if not user or not pwd:
            self.state = "AWAITING_USERNAME"
            await context.bot.send_message(chat_id=self.chat_id, text="👋 Welcome to NasirPredict!\n\nPlease enter your Ethio Lottery <b>Username</b>:", parse_mode="HTML")
        else:
            self.state = "MAIN"
            await self.show_main_menu(update, context)

    async def show_main_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        s = self.machine.get_state_summary()
        text = (
            f"🤖 <b>NASIRPREDICT BOT</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 <b>Status</b>: {'🟢 RUNNING' if s['status'] == 'RUNNING' else '🔴 OFFLINE'}\n"
            f"🎲 <b>Current Draw</b>: {s['current_draw_id']}\n"
            f"🗄 <b>DB Draws</b>: {s['db_count']}\n"
            f"🎯 <b>Predictions</b>: {s['predictions_made']}\n"
            f"📅 <b>Today Acc</b>: {s['today_accuracy']*100:.2f}%\n"
            f"🗓 <b>Weekly Acc</b>: {s['weekly_accuracy']*100:.2f}%\n"
        )
        if s['last_prediction']:
            lp = s['last_prediction']
            text += f"📉 <b>Last Pred</b>: {lp['hits']}/{lp['count']} hits\n"
        if s['current_prediction']:
            cp = s['current_prediction']
            text += f"📈 <b>Current Pred</b>: <code>{cp['numbers']}</code>\n"
        kb = self.main_menu_keyboard()
        if update.callback_query:
            try: await update.callback_query.edit_message_text(text, reply_markup=kb, parse_mode="HTML")
            except: await context.bot.send_message(chat_id=self.chat_id, text=text, reply_markup=kb, parse_mode="HTML")
        else: await context.bot.send_message(chat_id=self.chat_id, text=text, reply_markup=kb, parse_mode="HTML")

    async def on_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        data = query.data
        self.chat_id = query.message.chat_id
        self.machine.chat_id = self.chat_id

        if data == "cb_main" or data == "cb_status": await self.show_main_menu(update, context)
        elif data == "cb_settings": await query.edit_message_text("⚙️ <b>SETTINGS</b>\nSelect an option below:", reply_markup=self.settings_keyboard(), parse_mode="HTML")
        elif data == "cb_start":
            user = self.machine.db.get_state("ethio_user")
            pwd = self.machine.db.get_state("ethio_pass")
            if not user or not pwd:
                await query.edit_message_text("⚠️ Please login first. Send /start to enter credentials.")
                return
            await query.edit_message_text("🚀 Starting machine...", reply_markup=None)
            threading.Thread(target=self.machine.start, args=(user, pwd), daemon=True).start()
        elif data == "cb_stop":
            self.machine.stop()
            await self.show_main_menu(update, context)
        elif data == "cb_predict":
            if self.machine.db.get_draw_count() < 5:
                await query.edit_message_text("⚠️ Not enough history. Start machine first.", reply_markup=self.main_menu_keyboard())
                return
            threading.Thread(target=self.machine.make_prediction, daemon=True).start()
            await query.edit_message_text("🔮 Prediction requested...", reply_markup=self.main_menu_keyboard())
        elif data == "cb_logout":
            self.machine.db.set_state("ethio_user", "")
            self.machine.db.set_state("ethio_pass", "")
            self.state = "AWAITING_USERNAME"
            await query.edit_message_text("🚪 Logged out. Please send your Username.")
        elif data == "cb_perf_today": await self.cb_perf(query, "today")
        elif data == "cb_perf_week": await self.cb_perf(query, "week")
        elif data == "cb_perf_month": await self.cb_perf(query, "month")
        elif data == "cb_pred_history": await self.cb_pred_history(query)
        elif data == "cb_pred_stats": await self.cb_pred_stats(query)
        elif data == "cb_scores": await self.cb_scores(query)
        elif data == "cb_db_stats": await self.cb_db_stats(query)
        elif data == "cb_learning": await self.cb_learning(query)
        elif data == "cb_backtest": await self.cb_backtest(query)
        elif data == "cb_weights":
            rows = self.machine.db.fetchall("SELECT method, weight FROM scoring_weights ORDER BY method")
            text = "⚖️ <b>Change Analysis Weights</b>\n\nCurrent weights:\n"
            for r in rows: text += f"  {r['method']}: {float(r['weight']):.3f}\n"
            text += "\nSend the method name to change (e.g., 'frequency')."
            await query.edit_message_text(text, parse_mode="HTML")
            self.state = "AWAITING_WEIGHT_METHOD"
        elif data == "cb_backup":
            path = self.machine.db.backup()
            with open(path, 'rb') as f:
                await context.bot.send_document(chat_id=self.chat_id, document=f, filename=os.path.basename(path))
            await query.edit_message_text("💾 Backup sent!", reply_markup=self.settings_keyboard())
        elif data == "cb_restore":
            self.state = "AWAITING_RESTORE"
            await query.edit_message_text("📥 <b>Restore Database</b>\nPlease upload the .db backup file now.", parse_mode="HTML")
        elif data == "cb_export_preds":
            os.makedirs(EXPORT_DIR, exist_ok=True)
            path = os.path.join(EXPORT_DIR, f"preds_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
            self.machine.db.export_predictions(path)
            with open(path, 'rb') as f:
                await context.bot.send_document(chat_id=self.chat_id, document=f, filename=os.path.basename(path))
            await query.edit_message_text("📤 Export sent!", reply_markup=self.settings_keyboard())
        elif data == "cb_export_draws":
            os.makedirs(EXPORT_DIR, exist_ok=True)
            path = os.path.join(EXPORT_DIR, f"draws_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
            self.machine.db.export_draws(path)
            with open(path, 'rb') as f:
                await context.bot.send_document(chat_id=self.chat_id, document=f, filename=os.path.basename(path))
            await query.edit_message_text("📤 Export sent!", reply_markup=self.settings_keyboard())
        elif data == "cb_reset_stats":
            await query.edit_message_text("⚠️ <b>Reset Statistics</b>\nThis will DELETE all predictions and learning stats. Draws preserved.\n\nAre you sure?", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✅ Yes, Reset", callback_data="cb_reset_confirm")], [InlineKeyboardButton("❌ No, Cancel", callback_data="cb_settings")]]), parse_mode="HTML")
        elif data == "cb_reset_confirm":
            self.machine.db.reset_statistics()
            await query.edit_message_text("✅ Statistics reset.", reply_markup=self.settings_keyboard())
        elif data == "cb_clear_cache":
            self.machine.db.clear_cache()
            await query.edit_message_text("✅ Cache cleared.", reply_markup=self.settings_keyboard())

    async def cb_perf(self, query, period: str):
        if period == "today": start, label = today_str(), "Today's"
        elif period == "week": start, label = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d"), "Weekly"
        else: start, label = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"), "Monthly"
        rows = self.machine.db.fetchall("SELECT * FROM predictions WHERE resolved=1 AND date(resolved_at)>=? ORDER BY id DESC", (start,))
        if not rows:
            await query.edit_message_text(f"ℹ️ No resolved predictions in this {label.lower()} period.", reply_markup=self.settings_keyboard())
            return
        total_hits = sum(r["hits"] for r in rows)
        total_numbers = sum(r["count"] for r in rows)
        acc = (total_hits / total_numbers) if total_numbers else 0.0
        text = f"📅 <b>{label.upper()} PERFORMANCE</b>\nPredictions: {len(rows)}\nTotal Hits: {total_hits}/{total_numbers}\nAccuracy: {acc*100:.2f}%\nAvg Hits/Pred: {total_hits/len(rows):.3f}"
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=self.settings_keyboard())

    async def cb_pred_history(self, query):
        rows = self.machine.db.get_prediction_history(limit=15)
        if not rows:
            await query.edit_message_text("ℹ️ No predictions yet.", reply_markup=self.settings_keyboard())
            return
        text = "📜 <b>PREDICTION HISTORY (last 15)</b>\n\n"
        for r in rows:
            nums = json.loads(r["numbers"]) if r["numbers"] else []
            status = "✓" if r["resolved"] else "⏳"
            text += f"[{status}] #{r['id']} → Draw {r['target_draw_id']} | hits={r['hits']}/{r['count']}\n  <code>{nums}</code>\n"
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=self.settings_keyboard())

    async def cb_pred_stats(self, query):
        row = self.machine.db.fetchone("SELECT COUNT(*) AS c, SUM(count) AS tn, SUM(hits) AS th FROM predictions WHERE resolved=1")
        if not row or row["c"] == 0:
            await query.edit_message_text("ℹ️ No resolved predictions yet.", reply_markup=self.settings_keyboard())
            return
        c, tn, th = int(row["c"]), int(row["tn"]) if row["tn"] else 0, int(row["th"]) if row["th"] else 0
        acc = (th / tn) if tn else 0.0
        text = f"📊 <b>PREDICTION STATISTICS</b>\nTotal Resolved: {c}\nTotal Numbers: {tn}\nTotal Hits: {th}\nAccuracy: {acc*100:.2f}%\nAvg Hits/Pred: {th/c:.3f}"
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=self.settings_keyboard())

    async def cb_scores(self, query):
        history = self.machine.db.get_all_draws()
        if len(history) < 5:
            await query.edit_message_text("ℹ️ Not enough history.", reply_markup=self.settings_keyboard())
            return
        final, details = self.machine.engine.compute_scores(history)
        ranked = sorted(range(KENO_MIN, KENO_MAX + 1), key=lambda n: final[n], reverse=True)
        text = "📈 <b>TOP 20 ANALYSIS SCORES</b>\n\n"
        text += f"{'Rk':>3} {'Num':>4} {'Final':>7} | {'freq':>5} {'trnd':>5} {'gap':>5}\n"
        text += "─" * 35 + "\n"
        for rank, n in enumerate(ranked[:20], 1):
            d = details[n]
            text += f"{rank:>3} {n:>4} {d['final']:>7.4f} | {d['frequency']:>5.2f} {d['trend']:>5.2f} {d['gap']:>5.2f}\n"
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=self.settings_keyboard())

    async def cb_db_stats(self, query):
        preds = self.machine.db.fetchone("SELECT COUNT(*) AS c FROM predictions")
        text = (f"🗄 <b>DATABASE STATISTICS</b>\n"
                f"Total Draws: {self.machine.db.get_draw_count()}\n"
                f"Highest Draw ID: {self.machine.db.get_highest_draw_id()}\n"
                f"Total Predictions: {preds['c'] if preds else 0}\n")
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=self.settings_keyboard())

    async def cb_learning(self, query):
        rows = self.machine.db.get_learning_progress()
        text = "🧠 <b>LEARNING PROGRESS</b>\n\n"
        text += f"{'Method':<12} {'Preds':>6} {'Hits':>5} {'Rate':>6} {'Wgt':>5}\n"
        text += "─" * 35 + "\n"
        for r in rows:
            text += f"{r['method']:<12} {r['predictions']:>6} {r['hits']:>5} {r['hit_rate']*100:>5.1f}% {r['weight']:>5.2f}\n"
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=self.settings_keyboard())

    async def cb_backtest(self, query):
        rows = self.machine.db.get_backtests(limit=10)
        if not rows:
            await query.edit_message_text("ℹ️ No backtests yet.", reply_markup=self.settings_keyboard())
            return
        text = "🔬 <b>BACKTESTING RESULTS</b>\n\n"
        for r in rows:
            text += f"#{r['id']} {r['description']}\n  Avg: {r['avg_hits']:.3f} Best: {r['best_hits']} Worst: {r['worst_hits']} {'⭐' if r['selected'] else ''}\n"
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=self.settings_keyboard())

    async def on_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        text = update.message.text
        if not text: return
        if self.machine.awaiting_prediction:
            try:
                n = int(text)
                n = clamp(n, 1, 20)
                self.machine.prediction_count = n
                self.machine.prediction_event.set()
                await update.message.reply_text(f"✓ Will predict {n} numbers.")
            except ValueError:
                await update.message.reply_text("⚠ Invalid number. Using default 20.")
                self.machine.prediction_count = 20
                self.machine.prediction_event.set()
            return

        if self.state == "AWAITING_USERNAME":
            self.machine.db.set_state("ethio_user", text)
            self.state = "AWAITING_PASSWORD"
            await update.message.reply_text("🔐 Enter Password:")
        elif self.state == "AWAITING_PASSWORD":
            self.machine.db.set_state("ethio_pass", text)
            self.state = "MAIN"
            await self.cmd_start(update, context)
        elif self.state == "AWAITING_WEIGHT_METHOD":
            self.weight_method = text.lower()
            self.state = "AWAITING_WEIGHT_VALUE"
            await update.message.reply_text(f"Enter new weight for {self.weight_method} (0.0 - 10.0):")
        elif self.state == "AWAITING_WEIGHT_VALUE":
            try:
                w = float(text)
                self.machine.db.set_weight(self.weight_method, w)
                await update.message.reply_text(f"✅ Weight for {self.weight_method} set to {w:.3f}", reply_markup=self.settings_keyboard())
            except ValueError:
                await update.message.reply_text("⚠ Invalid number.")
            self.state = "MAIN"
        else:
            await update.message.reply_text("Use the menu buttons or /start to begin.")

    async def on_document(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if self.state == "AWAITING_RESTORE":
            doc = update.message.document
            if doc.file_name.endswith(".db"):
                await update.message.reply_text("📥 Downloading file...")
                file = await context.bot.get_file(doc.file_id)
                path = "restored.db"
                await file.download_to_drive(path)
                if self.machine.is_running(): self.machine.stop()
                if self.machine.db.restore(path):
                    await update.message.reply_text("✅ Database restored successfully!", reply_markup=self.settings_keyboard())
                else:
                    await update.message.reply_text("❌ Restore failed.", reply_markup=self.settings_keyboard())
            else:
                await update.message.reply_text("⚠ Please upload a .db file.")
            self.state = "MAIN"

# ==============================================================================
# MAIN ENTRY POINT
# ==============================================================================
def main():
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    if not token:
        print("ERROR: TELEGRAM_BOT_TOKEN environment variable is missing.")
        sys.exit(1)
    machine = Machine()
    bot = TelegramBot(token, machine)
    print("Starting NasirPredict Telegram Bot...")
    bot.app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
