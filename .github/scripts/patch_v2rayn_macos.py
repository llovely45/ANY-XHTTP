#!/usr/bin/env python3
"""Patch the upstream macOS tray tooltip to tolerate its initial null binding."""

from pathlib import Path
import sys


path = Path(sys.argv[1])
source = path.read_text(encoding="utf-8")
old = 'ToolTipText="{Binding RunningServerToolTipText}"'
new = 'ToolTipText="{Binding RunningServerToolTipText, TargetNullValue=\'\'}"'

if source.count(old) != 1:
    raise SystemExit(f"Expected one tray tooltip binding in {path}; found {source.count(old)}")

path.write_text(source.replace(old, new), encoding="utf-8")
