# -*- coding: utf-8 -*-
"""SQLite data logger for long-running acquisitions."""

import os
import queue
import sqlite3
import threading
import time
from typing import List, Optional


class DataLogger:
    def __init__(self, base_dir: str, batch_size: int = 200, flush_interval: float = 0.5):
        self.base_dir = base_dir
        self.batch_size = max(1, int(batch_size))
        self.flush_interval = max(0.05, float(flush_interval))

        self.db_path: str = ""
        self.channel_names: List[str] = []

        self._queue: "queue.Queue[object]" = queue.Queue()
        self._thread: Optional[threading.Thread] = None
        self._running = False
        self._stop_token = object()

        self._flush_request = False
        self._flush_event = threading.Event()
        self._lock = threading.Lock()
        self._row_count = 0
        self._last_error: Optional[str] = None

    @property
    def row_count(self) -> int:
        with self._lock:
            return int(self._row_count)

    @property
    def last_error(self) -> Optional[str]:
        with self._lock:
            return self._last_error

    def start_session(self, channel_names: List[str]) -> str:
        self.stop()
        self.channel_names = list(channel_names or [])

        os.makedirs(self.base_dir, exist_ok=True)
        ts = time.strftime("%Y%m%d_%H%M%S", time.localtime())
        self.db_path = os.path.join(self.base_dir, f"data_log_{ts}.sqlite")

        # Initialize schema (main thread)
        conn = sqlite3.connect(self.db_path)
        try:
            self._apply_pragmas(conn)
            conn.execute("CREATE TABLE IF NOT EXISTS channels (idx INTEGER PRIMARY KEY, name TEXT)")
            # clear and insert channel names for this session
            conn.execute("DELETE FROM channels")
            for idx, name in enumerate(self.channel_names):
                conn.execute("INSERT INTO channels (idx, name) VALUES (?, ?)", (int(idx), str(name)))

            col_defs = ", ".join([f"ch{idx} REAL" for idx in range(len(self.channel_names))])
            if col_defs:
                conn.execute(
                    f"CREATE TABLE IF NOT EXISTS data (id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL, {col_defs})"
                )
            else:
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS data (id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL)"
                )
            conn.commit()
        finally:
            conn.close()

        self._queue = queue.Queue()
        self._flush_event.clear()
        self._flush_request = False
        self._running = True
        self._thread = threading.Thread(target=self._writer_loop, daemon=True)
        self._thread.start()
        return self.db_path

    def append(self, ts: float, row: dict):
        if not self._running:
            return
        if not self.channel_names:
            return
        try:
            ts_val = float(ts)
        except Exception:
            ts_val = time.time()

        vals = []
        for name in self.channel_names:
            v = row.get(name, None)
            if v is None:
                vals.append(None)
                continue
            try:
                fv = float(v)
            except Exception:
                vals.append(None)
                continue
            # Filter non-finite values
            if fv != fv or fv == float("inf") or fv == float("-inf"):
                vals.append(None)
            else:
                vals.append(fv)

        self._queue.put((ts_val, vals))

    def flush(self, wait: bool = True, timeout: float = 5.0):
        if not self._running:
            return
        self._flush_request = True
        if not wait:
            return
        self._flush_event.wait(timeout=timeout)
        self._flush_event.clear()

    def stop(self):
        if not self._running:
            return
        self._running = False
        try:
            self._queue.put(self._stop_token)
        except Exception:
            pass
        if self._thread is not None:
            try:
                self._thread.join(timeout=3.0)
            except Exception:
                pass
        self._thread = None

    def _apply_pragmas(self, conn: sqlite3.Connection):
        try:
            conn.execute("PRAGMA journal_mode=WAL")
        except Exception:
            pass
        try:
            conn.execute("PRAGMA synchronous=NORMAL")
        except Exception:
            pass
        try:
            conn.execute("PRAGMA temp_store=MEMORY")
        except Exception:
            pass

    def _writer_loop(self):
        conn = sqlite3.connect(self.db_path, timeout=30.0, check_same_thread=False)
        try:
            self._apply_pragmas(conn)
            cur = conn.cursor()
            col_names = [f"ch{idx}" for idx in range(len(self.channel_names))]
            cols_sql = ", ".join(["ts"] + col_names)
            placeholders = ", ".join(["?"] * (1 + len(col_names)))
            insert_sql = f"INSERT INTO data ({cols_sql}) VALUES ({placeholders})"

            batch = []
            last_flush = time.monotonic()

            while True:
                item = None
                try:
                    item = self._queue.get(timeout=0.2)
                except Exception:
                    item = None

                if item is self._stop_token:
                    # flush remaining and exit
                    if batch:
                        self._flush_batch(cur, conn, batch, insert_sql)
                        batch = []
                    conn.commit()
                    break

                if item is not None:
                    ts_val, vals = item
                    batch.append((ts_val, *vals))

                now = time.monotonic()
                time_due = (now - last_flush) >= self.flush_interval
                size_due = len(batch) >= self.batch_size
                force_due = self._flush_request

                if batch and (time_due or size_due or force_due):
                    self._flush_batch(cur, conn, batch, insert_sql)
                    batch = []
                    last_flush = now

                if force_due:
                    # mark flush completed
                    self._flush_request = False
                    self._flush_event.set()
        except Exception as e:
            with self._lock:
                self._last_error = str(e)
        finally:
            try:
                conn.commit()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def _flush_batch(self, cur, conn, batch, insert_sql: str):
        if not batch:
            return
        try:
            cur.executemany(insert_sql, batch)
            conn.commit()
            with self._lock:
                self._row_count += len(batch)
        except Exception as e:
            with self._lock:
                self._last_error = str(e)
