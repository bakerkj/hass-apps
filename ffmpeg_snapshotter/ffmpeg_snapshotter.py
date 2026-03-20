# Copyright (c) 2026 Kenneth Baker <bakerkj@umich.edu>
# All rights reserved.

import argparse
import json
import os
import re
import shlex
import signal
import threading
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple


def log(level: str, msg: str) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"{ts} [{level}] {msg}", flush=True)


def ensure_media_path(output_dir: str) -> Path:
    p = Path(output_dir)
    if not str(p).startswith("/media/"):
        p = Path("/media") / p
    return p


def set_latest_symlink(target: Path, latest_path: Path) -> None:
    # Best-effort symlink update. If symlinks aren't supported on /media, we log once per attempt.
    try:
        tmp_link = latest_path.with_suffix(".jpg.tmp")
        if tmp_link.exists() or tmp_link.is_symlink():
            tmp_link.unlink()
        rel = os.path.relpath(str(target), str(latest_path.parent))
        tmp_link.symlink_to(rel)
        tmp_link.replace(latest_path)
    except Exception as e:
        log("WARNING", f"Failed to update symlink {latest_path} -> {target}: {e}")


def apply_retention_days(dir_path: Path, retain_days: int, latest_name: str) -> None:
    if retain_days <= 0:
        return
    if not dir_path.exists():
        return
    # Use find for efficiency (no Python directory walks needed)
    # -mtime +N matches strictly greater than N days. We want older than retain_days, so +retain_days.
    # Exclude the latest symlink by name.
    try:
        subprocess.run(
            [
                "find",
                str(dir_path),
                "-type",
                "f",
                "-name",
                "*.jpg",
                "!",
                "-name",
                latest_name,
                "-mtime",
                f"+{retain_days}",
                "-delete",
            ],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except Exception as e:
        log("WARNING", f"Retention (days) failed for {dir_path}: {e}")


def apply_retention_count(dir_path: Path, retain_count: int, latest_name: str) -> None:
    if retain_count <= 0:
        return
    if not dir_path.exists():
        return

    # Count retention requires listing/sorting. Do it only on the retention interval.
    files: List[Tuple[float, Path]] = []
    try:
        for p in dir_path.iterdir():
            if not p.is_file():
                continue
            if p.suffix.lower() != ".jpg":
                continue
            if p.name == latest_name:
                continue
            try:
                files.append((p.stat().st_mtime, p))
            except FileNotFoundError:
                pass
        files.sort(key=lambda t: t[0], reverse=True)
        for _, p in files[retain_count:]:
            try:
                p.unlink(missing_ok=True)
            except Exception:
                pass
    except Exception as e:
        log("WARNING", f"Retention (count) failed for {dir_path}: {e}")


@dataclass
class StreamCfg:
    name: str
    url: str
    interval_seconds: int
    output_dir: Path
    filename_format: str
    date_dir_format: str
    latest_name: str
    retain_count: int
    retain_days: int
    extra_input_args: str
    extra_output_args: str


class Worker:
    def __init__(
        self,
        cfg: StreamCfg,
        ffmpeg_cfg: Dict[str, str],
        log_level: str,
        start_offset_seconds: float = 0.0,
    ):
        self.cfg = cfg
        self.ffmpeg_cfg = ffmpeg_cfg
        self.thread: Optional[threading.Thread] = None
        self.stop_event = threading.Event()
        self.backoff = 1.0
        self.log_level = log_level
        self.next_due = 0.0
        self.start_offset_seconds = float(start_offset_seconds)

    def start(self) -> None:
        if self.thread and self.thread.is_alive():
            return
        self.cfg.output_dir.mkdir(parents=True, exist_ok=True)
        tmp_dir = self.cfg.output_dir / ".tmp"
        if tmp_dir.exists():
            for f in tmp_dir.iterdir():
                try:
                    f.unlink()
                except Exception:
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

    def poll(self) -> Optional[int]:
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

    def _build_ffmpeg_cmd(self, out_path: Path) -> List[str]:
        cmd: List[str] = ["ffmpeg", "-nostdin"]

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
            log(
                "DEBUG",
                f"[{self.cfg.name}] cmd: {' '.join(shlex.quote(x) for x in cmd)}",
            )

        proc: Optional[subprocess.Popen] = None
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
                return rc

            tmp_path.replace(final_path)
            latest_path = self.cfg.output_dir / self.cfg.latest_name
            set_latest_symlink(final_path, latest_path)
            return 0
        except subprocess.TimeoutExpired:
            if proc is not None:
                try:
                    proc.kill()
                except Exception:
                    pass
            log("WARNING", f"[{self.cfg.name}] ffmpeg timed out")
            return 124
        except Exception as e:
            log("WARNING", f"[{self.cfg.name}] snapshot failed: {e}")
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


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--options", required=True, help="Path to HA add-on options.json")
    args = ap.parse_args()

    with open(args.options, "r", encoding="utf-8") as f:
        opts = json.load(f)

    log_level = (opts.get("log_level") or "INFO").upper()

    streams = opts.get("streams") or []
    if not streams:
        log("WARNING", "No streams configured. Exiting.")
        return 0

    ff = opts.get("ffmpeg") or {}
    ffmpeg_cfg: Dict[str, str] = {
        "global_input_args": ff.get("global_input_args", "") or "",
        "global_hwaccel_args": ff.get("global_hwaccel_args", "") or "",
        "global_output_args": ff.get("global_output_args", "") or "",
    }

    hk = opts.get("housekeeping") or {}
    retention_interval = int(hk.get("retention_interval_seconds", 60) or 60)
    retention_interval = max(5, min(3600, retention_interval))
    last_retention = 0.0

    workers: Dict[str, Worker] = {}
    cfgs: Dict[str, StreamCfg] = {}

    for s in streams:
        cfg = StreamCfg(
            name=s["name"],
            url=s["url"],
            interval_seconds=int(s["interval_seconds"]),
            output_dir=ensure_media_path(s["output_dir"]),
            filename_format=s.get("filename_format") or "%Y%m%d-%H%M%S.jpg",
            date_dir_format=s.get("date_dir_format") or "%Y/%m/%d",
            latest_name=s.get("latest_name") or "latest.jpg",
            retain_count=int(s.get("retain_count") or 0),
            retain_days=int(s.get("retain_days") or 0),
            extra_input_args=s.get("extra_input_args") or "",
            extra_output_args=s.get("extra_output_args") or "",
        )
        cfgs[cfg.name] = cfg

    # Evenly distribute start offsets for streams that share the same interval.
    # For example, if 6 streams have interval 60, they will start at 0,10,20,30,40,50 seconds.
    offsets: Dict[str, float] = {}
    groups: Dict[int, list] = {}
    for s in streams:
        interval = int(s["interval_seconds"])
        groups.setdefault(interval, []).append(s["name"])

    for interval, names in groups.items():
        n = max(1, len(names))
        for i, name in enumerate(names):
            offsets[name] = (float(i) * float(interval)) / float(n)

    for name, cfg in cfgs.items():
        workers[name] = Worker(
            cfg,
            ffmpeg_cfg,
            log_level=log_level,
            start_offset_seconds=offsets.get(name, 0.0),
        )

    stopping = False

    def handle_sig(sig, frame):
        nonlocal stopping
        stopping = True
        log("INFO", f"Received signal {sig}, stopping workers...")
        for w in workers.values():
            w.stop(signal.SIGTERM)

    signal.signal(signal.SIGTERM, handle_sig)
    signal.signal(signal.SIGINT, handle_sig)

    for w in workers.values():
        try:
            w.start()
        except Exception as e:
            log("ERROR", f"[{w.cfg.name}] failed to start: {e}")
            return 1

    while True:
        now = time.time()
        if (now - last_retention) >= retention_interval:
            last_retention = now
            for cfg in cfgs.values():
                try:
                    apply_retention_days(
                        cfg.output_dir, cfg.retain_days, cfg.latest_name
                    )
                    apply_retention_count(
                        cfg.output_dir, cfg.retain_count, cfg.latest_name
                    )
                except Exception as e:
                    log("WARNING", f"[{cfg.name}] retention error: {e}")

        if stopping:
            time.sleep(1.0)
            return 0

        time.sleep(0.1)


if __name__ == "__main__":
    raise SystemExit(main())
