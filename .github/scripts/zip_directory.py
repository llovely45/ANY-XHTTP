#!/usr/bin/env python3
"""Create a ZIP that includes the source directory as its top-level folder."""

from __future__ import annotations

import pathlib
import stat
import sys
import zipfile


def main() -> int:
    if len(sys.argv) != 3:
        print("usage: zip_directory.py SOURCE_DIR OUTPUT_ZIP", file=sys.stderr)
        return 2

    source = pathlib.Path(sys.argv[1]).resolve()
    output = pathlib.Path(sys.argv[2]).resolve()
    if not source.is_dir():
        raise SystemExit(f"source directory does not exist: {source}")

    output.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as archive:
        for path in sorted(source.rglob("*")):
            relative = pathlib.Path(source.name) / path.relative_to(source)
            if path.is_dir():
                entry = zipfile.ZipInfo(relative.as_posix() + "/")
                entry.external_attr = (stat.S_IFDIR | 0o755) << 16
                archive.writestr(entry, b"")
            else:
                archive.write(path, relative.as_posix())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
