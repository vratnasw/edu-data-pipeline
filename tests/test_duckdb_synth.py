"""DuckDB query interface against a local synthetic parquet."""
from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd


def test_duckdb_can_query_local_parquet(tmp_path):
    df = pd.DataFrame({"cds": ["a", "b", "c"], "math": [10.0, 20.0, 30.0],
                          "year_num": [2020, 2021, 2022]})
    p = tmp_path / "panel.parquet"
    df.to_parquet(p, index=False)
    con = duckdb.connect()
    result = con.execute(f"""
        SELECT year_num, AVG(math) AS avg_math
        FROM '{p}'
        GROUP BY year_num
        ORDER BY year_num
    """).df()
    assert len(result) == 3
    assert abs(result["avg_math"].sum() - 60.0) < 1e-6


def test_duckdb_httpfs_extension_loads():
    """Confirm DuckDB has httpfs available — needed for R2 reads."""
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    # No actual http call here — just verify the load succeeded
    assert True
