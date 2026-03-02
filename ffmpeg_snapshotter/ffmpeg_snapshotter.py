#!/usr/bin/env python3
import argparse
import json
import os
import re
import shlex
import signal
import selectors
import threading
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

OPENING_RE = re.compile(r"Opening '([^']+\.jpg)' for writing")

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

def apply_retention_days(dir_path: Path, retain_days: int) -> None:
    if retain_days <= 0:
        return
    if not dir_path.exists():
        return
    # Use find for efficiency (no Python directory walks needed)
    # -mtime +N matches strictly greater than N days. We want older than retain_days, so +retain_days.
    # Exclude latest.jpg.
    try:
        subprocess.run(
            ["find", str(dir_path), "-type", "f", "-name", "*.jpg", "!", "-name", "latest.jpg",
             "-mtime", f"+{retain_days}", "-delete"],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except Exception as e:
        log("WARNING", f"Retention (days) failed for {dir_path}: {e}")

def apply_retention_count(dir_path: Path, retain_count: int) -> None:
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
            if p.name == "latest.jpg":
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
    latest_name: str
    retain_count: int
    retain_days: int
    extra_input_args: str
    extra_output_args: str

class Worker:
    def __init__(self, cfg: StreamCfg, ffmpeg_cmd: List[str], log_level: str):
        self.cfg = cfg
        self.ffmpeg_cmd = ffmpeg_cmd
        self.proc: Optional[subprocess.Popen] = None
        self.reader_thread: Optional[threading.Thread] = None
        self.stop_event = threading.Event()
        self.backoff = 1.0
        self.log_level = log_level

    def start(self) -> None:
        if self.reader_thread and self.reader_thread.is_alive():
            try:
                self.stop_event.set()
                self.reader_thread.join(timeout=1.0)
            except Exception:
                pass
        self.cfg.output_dir.mkdir(parents=True, exist_ok=True)
        log("INFO", f"[{self.cfg.name}] Starting ffmpeg worker")
        log("INFO", f"[{self.cfg.name}] cmd: {' '.join(shlex.quote(x) for x in self.ffmpeg_cmd)}")

        self.proc = subprocess.Popen(
            self.ffmpeg_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        self.stop_event.clear()
        self.reader_thread = threading.Thread(target=self._io_reader, daemon=True)
        self.reader_thread.start()
        self.backoff = 1.0

    def poll(self) -> Optional[int]:
        if not self.proc:
            return None
        return self.proc.poll()

    def stop(self, sig=signal.SIGTERM) -> None:
        if not self.proc:
            return
        try:
            self.stop_event.set()
        except Exception:
            pass
        try:
            self.proc.send_signal(sig)
        except Exception:
            pass
        if self.reader_thread and self.reader_thread.is_alive():
            try:
                self.reader_thread.join(timeout=1.0)
            except Exception:
                pass

    def _io_reader(self) -> None:
        if not self.proc:
            return
        sel = selectors.DefaultSelector()
        try:
            if self.proc.stdout:
                sel.register(self.proc.stdout, selectors.EVENT_READ, data="stdout")
            if self.proc.stderr:
                sel.register(self.proc.stderr, selectors.EVENT_READ, data="stderr")
        except Exception:
            return

        try:
            while not self.stop_event.is_set():
                events = sel.select(timeout=0.5)
                if not events:
                    # If the process exited, stop draining.
                    if self.proc.poll() is not None:
                        break
                    continue
                for key, _mask in events:
                    try:
                        line = key.fileobj.readline()
                    except Exception:
                        line = ""
                    if not line:
                        try:
                            sel.unregister(key.fileobj)
                        except Exception:
                            pass
                        continue
                    line = line.rstrip()
                    if not line:
                        continue

                    # Parse path directly from ffmpeg log
                    m = OPENING_RE.search(line)
                    if m:
                        path = m.group(1)
                        try:
                            p = Path(path)
                            latest_path = self.cfg.output_dir / self.cfg.latest_name
                            set_latest_symlink(p, latest_path)
                        except Exception as e:
                            log("WARNING", f"[{self.cfg.name}] latest update failed: {e}")

                    log("INFO", f"[{self.cfg.name}] {line}")
        except Exception:
            pass
        finally:
            try:
                sel.close()
            except Exception:
                pass

    def restart_with_backoff(self) -> None:
        delay = min(self.backoff, 60.0)
        log("WARNING", f"[{self.cfg.name}] ffmpeg exited. Restarting in {delay:.1f}s")
        time.sleep(delay)
        self.backoff = min(self.backoff * 2.0, 60.0)
        self.start()

def build_ffmpeg_cmd(
    url: str,
    interval_seconds: int,
    output_path_pattern: str,
    global_input_args: str,
    global_hwaccel_args: str,
    extra_input_args: str,
    global_output_args: str,
    extra_output_args: str,
) -> List[str]:
    cmd: List[str] = ["ffmpeg", "-nostdin"]

    def extend_args(arg_str: str) -> None:
        if arg_str:
            cmd.extend(shlex.split(arg_str))

    extend_args(global_hwaccel_args)
    extend_args(global_input_args)
    extend_args(extra_input_args)

    cmd.extend(["-i", url])

    # emit one frame per interval
    vf = f"fps=1/{interval_seconds}"
    # If using VAAPI hardware-accelerated decode, frames may remain in GPU memory.
    # Download to system memory and normalize pixel format for JPEG encoding.
    if re.search(r"(^|\s)-hwaccel\s+vaapi(\s|$)", global_hwaccel_args):
         vf = vf + ",hwdownload,format=nv12"
    cmd.extend(["-vf", vf, "-an"])

    extend_args(global_output_args)
    extend_args(extra_output_args)

    cmd.extend(["-f", "image2", "-atomic_writing", "1", "-strftime", "1", output_path_pattern])
    return cmd

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
    global_input_args = ff.get("global_input_args", "") or ""
    global_hwaccel_args = ff.get("global_hwaccel_args", "") or ""
    global_output_args = ff.get("global_output_args", "") or ""

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
            latest_name=s.get("latest_name") or "latest.jpg",
            retain_count=int(s.get("retain_count") or 0),
            retain_days=int(s.get("retain_days") or 0),
            extra_input_args=s.get("extra_input_args") or "",
            extra_output_args=s.get("extra_output_args") or "",
        )

        out_pattern = str(cfg.output_dir / cfg.filename_format)
        cmd = build_ffmpeg_cmd(
            url=cfg.url,
            interval_seconds=cfg.interval_seconds,
            output_path_pattern=out_pattern,
            global_input_args=global_input_args,
            global_hwaccel_args=global_hwaccel_args,
            extra_input_args=cfg.extra_input_args,
            global_output_args=global_output_args,
            extra_output_args=cfg.extra_output_args,
        )

        workers[cfg.name] = Worker(cfg, cmd, log_level=log_level)
        cfgs[cfg.name] = cfg

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
        for w in workers.values():
            rc = w.poll()
            if rc is not None and not stopping:
                w.restart_with_backoff()

        now = time.time()
        if (now - last_retention) >= retention_interval:
            last_retention = now
            for cfg in cfgs.values():
                try:
                    apply_retention_days(cfg.output_dir, cfg.retain_days)
                    apply_retention_count(cfg.output_dir, cfg.retain_count)
                except Exception as e:
                    log("WARNING", f"[{cfg.name}] retention error: {e}")

        if stopping:
            time.sleep(1.0)
            for w in workers.values():
                if w.poll() is None:
                    w.stop(signal.SIGKILL)
            return 0

        time.sleep(0.1)

if __name__ == "__main__":
    raise SystemExit(main())
