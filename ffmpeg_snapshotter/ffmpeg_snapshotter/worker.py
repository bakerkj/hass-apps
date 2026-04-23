# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

"""Per-stream Worker: grabs one snapshot per ``interval_seconds`` via ffmpeg."""

from __future__ import annotations

import re
import shlex
import signal
import subprocess
import threading
import time
from collections.abc import Callable
from pathlib import Path

from .config import StreamCfg
from .stats import SnapshotStats
from .util import log, redact_url, set_latest_symlink


class Worker:
    def __init__(
        self,
        cfg: StreamCfg,
        ffmpeg_cfg: dict[str, str],
        log_level: str,
        start_offset_seconds: float = 0.0,
        stats: SnapshotStats | None = None,
        image_sink: Callable[[str, bytes], None] | None = None,
    ):
        self.cfg = cfg
        self.ffmpeg_cfg = ffmpeg_cfg
        self.thread: threading.Thread | None = None
        self.stop_event = threading.Event()
        self.backoff = 1.0
        self.log_level = log_level
        self.next_due = 0.0
        self.start_offset_seconds = float(start_offset_seconds)
        self.stats = stats
        # Called with (camera_name, jpeg_bytes) after each successful
        # snapshot.  The sink must be fast and thread-safe; exceptions
        # are swallowed so an MQTT outage can't break snapshotting.
        self.image_sink = image_sink

    def start(self) -> None:
        if self.thread and self.thread.is_alive():
            return
        self.cfg.output_dir.mkdir(parents=True, exist_ok=True)
        tmp_dir = self.cfg.output_dir / ".tmp"
        if tmp_dir.exists():
            for f in tmp_dir.iterdir():
                try:
                    f.unlink()
                except OSError:
                    pass
        log("INFO", f"[{self.cfg.name}] Starting worker")
        self.stop_event.clear()
        self.backoff = 1.0
        # Attempt to align the first snapshot to evenly distribute load across streams
        # sharing the same interval. This aligns to the next wall-clock interval boundary
        # plus this stream's assigned offset within that interval.
        now_wall = time.time()
        interval = max(1, int(self.cfg.interval_seconds))
        offset = float(self.start_offset_seconds) % interval
        align_delay = (offset - (now_wall % interval)) % interval
        self.next_due = time.monotonic() + align_delay
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def poll(self) -> int | None:
        if not self.thread:
            return None
        return None if self.thread.is_alive() else 0

    def stop(self, sig=signal.SIGTERM) -> None:
        try:
            self.stop_event.set()
        except Exception:
            pass
        if self.thread and self.thread.is_alive():
            try:
                self.thread.join(timeout=2.0)
            except Exception:
                pass

    def _is_vaapi(self) -> bool:
        hwaccel_args = self.ffmpeg_cfg.get("global_hwaccel_args", "") or ""
        return bool(re.search(r"(^|\s)-hwaccel\s+vaapi(\s|$)", hwaccel_args))

    def _tmp_snapshot_path(self) -> Path:
        tmp_dir = self.cfg.output_dir / ".tmp"
        tmp_dir.mkdir(parents=True, exist_ok=True)
        return tmp_dir / f"{self.cfg.name}-{time.monotonic_ns()}.jpg"

    def _final_snapshot_path(self, now_ts: float) -> Path:
        date_dir = time.strftime(self.cfg.date_dir_format, time.localtime(now_ts))
        out_dir = self.cfg.output_dir / date_dir
        out_dir.mkdir(parents=True, exist_ok=True)
        filename = time.strftime(self.cfg.filename_format, time.localtime(now_ts))
        return out_dir / filename

    def _build_ffmpeg_cmd(self, out_path: Path) -> list[str]:
        cmd: list[str] = ["ffmpeg", "-nostdin"]

        def extend_args(arg_str: str) -> None:
            if arg_str:
                cmd.extend(shlex.split(arg_str))

        extend_args(self.ffmpeg_cfg.get("global_hwaccel_args", "") or "")
        extend_args(self.ffmpeg_cfg.get("global_input_args", "") or "")
        extend_args(self.cfg.extra_input_args)
        cmd.extend(["-i", self.cfg.url])

        # Grab a single frame.
        if self._is_vaapi():
            cmd.extend(["-vf", "hwdownload,format=nv12"])
        cmd.extend(["-an", "-frames:v", "1"])
        extend_args(self.ffmpeg_cfg.get("global_output_args", "") or "")
        extend_args(self.cfg.extra_output_args)
        cmd.extend(["-atomic_writing", "1"])
        cmd.extend(["-update", "1"])
        cmd.extend(["-y", str(out_path)])
        return cmd

    def _run_one_snapshot(self) -> int:
        rc = 1
        tmp_path = self._tmp_snapshot_path()
        now_ts = time.time()
        final_path = self._final_snapshot_path(now_ts)
        cmd = self._build_ffmpeg_cmd(tmp_path)

        if self.log_level == "DEBUG":
            redacted = " ".join(shlex.quote(redact_url(x)) for x in cmd)
            log("DEBUG", f"[{self.cfg.name}] cmd: {redacted}")

        proc: subprocess.Popen | None = None
        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            out, err = proc.communicate(
                timeout=max(5, min(120, int(self.cfg.interval_seconds)))
            )
            rc = int(proc.returncode or 0)
            out_level = "WARNING" if rc != 0 else "DEBUG"
            if self.log_level == "DEBUG" or rc != 0:
                if out:
                    for line in out.splitlines():
                        if line:
                            log(out_level, f"[{self.cfg.name}] [stdout] {line}")
                if err:
                    for line in err.splitlines():
                        if line:
                            log(out_level, f"[{self.cfg.name}] [stderr] {line}")
            if rc != 0:
                if self.stats is not None:
                    self.stats.record_error()
                return rc

            # Capture size before the rename so we can feed it to stats; a
            # stat on final_path after rename would also work but incurs a
            # second inode lookup for no benefit.
            try:
                file_bytes = tmp_path.stat().st_size
            except OSError:
                file_bytes = 0
            tmp_path.replace(final_path)
            latest_path = self.cfg.output_dir / self.cfg.latest_name
            set_latest_symlink(final_path, latest_path)
            if self.stats is not None:
                self.stats.record_success(file_bytes)
            if self.image_sink is not None:
                try:
                    self.image_sink(self.cfg.name, final_path.read_bytes())
                except Exception as e:
                    log("WARNING", f"[{self.cfg.name}] image publish failed: {e}")
            return 0
        except subprocess.TimeoutExpired:
            if proc is not None:
                try:
                    proc.kill()
                except Exception:
                    pass
            log("WARNING", f"[{self.cfg.name}] ffmpeg timed out")
            if self.stats is not None:
                self.stats.record_error()
            return 124
        except Exception as e:
            log("WARNING", f"[{self.cfg.name}] snapshot failed: {e}")
            if self.stats is not None:
                self.stats.record_error()
            return rc
        finally:
            try:
                if tmp_path.exists():
                    tmp_path.unlink()
            except Exception:
                pass

    def _run(self) -> None:
        interval = max(1, int(self.cfg.interval_seconds))
        while not self.stop_event.is_set():
            now = time.monotonic()
            delay = self.next_due - now
            if delay > 0:
                if self.stop_event.wait(delay):
                    break

            # Resync if we fell behind significantly.
            now = time.monotonic()
            if (now - self.next_due) > (interval * 1.5):
                self.next_due = now

            rc = self._run_one_snapshot()
            if rc == 0:
                self.backoff = 1.0
                self.next_due += interval
                continue

            backoff_delay = min(self.backoff, 60.0)
            log(
                "WARNING",
                f"[{self.cfg.name}] snapshot failed (rc={rc}). Backing off {backoff_delay:.1f}s",
            )
            self.backoff = min(self.backoff * 2.0, 60.0)
            self.next_due = time.monotonic() + backoff_delay
