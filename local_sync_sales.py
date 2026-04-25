#!/usr/bin/env python3
import subprocess
import sys
from pathlib import Path


def main() -> int:
    root = Path(__file__).resolve().parent
    cmd = [sys.executable, str(root / "local_ecostok2_sync.py"), "--target", "sales", *sys.argv[1:]]
    return subprocess.run(cmd).returncode


if __name__ == "__main__":
    raise SystemExit(main())
