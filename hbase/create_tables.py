"""Create the AEGIS HBase tables (idempotent) and print their column families.

    python hbase/create_tables.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from aegis.config import get_config  # noqa: E402
from aegis.storage.hbase_repository import TABLES, HBaseRepository, HBaseUnavailable  # noqa: E402


def main() -> int:
    cfg = get_config()
    repo = HBaseRepository.from_config(cfg)
    try:
        result = repo.ensure_tables()
        out = {"endpoint": f"{repo.host}:{repo.port}", "tables": {}}
        for name in TABLES:
            out["tables"][name] = {"status": result[name], "row_key": TABLES[name]["row_key"],
                                   "families": repo.describe(name)}
        print(json.dumps(out, indent=2))
        return 0
    except HBaseUnavailable as exc:
        print(json.dumps({"error": str(exc), "hint": "start HBase: docker compose up -d hbase"}, indent=2))
        return 2
    finally:
        repo.close()


if __name__ == "__main__":
    raise SystemExit(main())
