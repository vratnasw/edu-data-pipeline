"""Load config/config.yaml + .env."""
from __future__ import annotations

from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]


def load_config(path: Path | None = None) -> dict:
    p = Path(path) if path else REPO_ROOT / "config" / "config.yaml"
    return yaml.safe_load(p.read_text(encoding="utf-8"))


def load_dotenv(path: Path | None = None) -> dict:
    """Load .env into os.environ (won't override existing values)."""
    import os
    p = Path(path) if path else REPO_ROOT / ".env"
    if not p.exists():
        return {}
    loaded: dict = {}
    for raw in p.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip(); v = v.strip().strip('"').strip("'")
        if k and not os.environ.get(k):
            os.environ[k] = v
        loaded[k] = v
    return loaded
