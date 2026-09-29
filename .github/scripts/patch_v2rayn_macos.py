#!/usr/bin/env python3
"""Patch the upstream macOS tray tooltip with a safe startup binding fallback."""

from pathlib import Path
import sys


path = Path(sys.argv[1])
source = path.read_text(encoding="utf-8")
old = 'ToolTipText="{Binding RunningServerToolTipText}"'
new = (
    'ToolTipText="{Binding RunningServerToolTipText, '
    "FallbackValue='v2rayN', TargetNullValue='v2rayN'}\""
)

if source.count(old) != 1:
    raise SystemExit(f"Expected one tray tooltip binding in {path}; found {source.count(old)}")

patched = source.replace(old, new)
if patched.count(new) != 1:
    raise SystemExit(f"Failed to apply tray tooltip fallback in {path}")

path.write_text(patched, encoding="utf-8")
