"""Refuse a release whose version is not declared everywhere it has to be.

`make release-check` runs this before tagging; release.yml runs it again with
the tag it is building. 0.2.1 shipped with `__version__` still reading "0.2.0"
because the bump touched pyproject.toml alone, and nothing noticed.

Deliberately dependency-free and stdlib-only, so it runs on any Python this
project supports, including 3.10 without tomllib.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def declared_version() -> str:
    """The one `version = "..."` line pyproject.toml is allowed to have."""
    text = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    found = re.findall(r'^version = "(.+?)"$', text, re.MULTILINE)
    if len(found) != 1:
        raise SystemExit(f"pyproject.toml has {len(found)} version lines; expected exactly one")
    return found[0]


def problems(tag: str | None) -> list[str]:
    version = declared_version()
    pyproject = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    changelog = (ROOT / "CHANGELOG.md").read_text(encoding="utf-8")
    found = []

    if tag is not None and tag != f"v{version}":
        found.append(f"tag {tag} does not match pyproject version {version}")

    if re.search(r'^\s*"License :: ', pyproject, re.MULTILINE):
        # PEP 639: PyPI rejects an upload carrying both. `twine check` does not catch it.
        found.append("drop the 'License :: ...' classifiers; they cannot accompany License-Expression")

    if f"## [{version}]" not in changelog:
        found.append(f"CHANGELOG.md has no '## [{version}]' section")

    unreleased = re.search(r"^## \[Unreleased\]\s*\n(.*?)(?=^## |\Z)", changelog, re.MULTILINE | re.DOTALL)
    if unreleased is not None and unreleased.group(1).strip():
        found.append(f"CHANGELOG.md still has entries under [Unreleased]; they belong in [{version}]")

    return found


def main(argv: list[str]) -> int:
    found = problems(argv[0] if argv else None)
    for problem in found:
        print(f"release check: {problem}", file=sys.stderr)
    if found:
        return 1
    print(f"release check: {declared_version()} is consistent")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
