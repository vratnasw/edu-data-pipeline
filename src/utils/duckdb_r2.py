"""Query parquet files on R2 directly via DuckDB httpfs — no download.

Usage (from outside the pipeline; no R2 creds needed if bucket is public):
    from utils.duckdb_r2 import query, EXAMPLE_QUERIES
    df = query(EXAMPLE_QUERIES['state_summary'])

Auth-mode: if R2 creds are in env, configure DuckDB's S3 endpoint so it
can read private parquet too.
"""
from __future__ import annotations

import logging
import os

import duckdb
import pandas as pd

log = logging.getLogger(__name__)


def _con():
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    # If R2 creds are set, configure DuckDB's S3 driver
    if all(os.environ.get(k) for k in
              ("R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_ENDPOINT_URL")):
        endpoint = os.environ["R2_ENDPOINT_URL"].replace("https://", "").rstrip("/")
        con.execute(f"""
            SET s3_endpoint='{endpoint}';
            SET s3_access_key_id='{os.environ["R2_ACCESS_KEY_ID"]}';
            SET s3_secret_access_key='{os.environ["R2_SECRET_ACCESS_KEY"]}';
            SET s3_url_style='path';
        """)
    return con


def query(sql: str) -> pd.DataFrame:
    """Run an arbitrary SQL query and return a pandas dataframe.

    Reference parquet files via either:
      - public URL: 'https://pub-<account>.r2.dev/path/to.parquet'
      - s3://<bucket>/path/to.parquet (requires creds)
    """
    return _con().execute(sql).df()


# --------------------------------------------------------------------------- #
# Example queries — paths are placeholders; substitute the real R2 URL of
# your master panel before running.
# --------------------------------------------------------------------------- #

PUBLIC_PANEL = "https://pub-<R2_ACCOUNT>.r2.dev/public/master_panel.parquet"

EXAMPLE_QUERIES = {
    "state_summary": f"""
        SELECT year_num,
                AVG(caaspp_math_met_pct)        AS math_pct,
                AVG(caaspp_ela_met_pct)         AS ela_pct,
                AVG(chronic_absenteeism_rate)   AS absent_rate,
                COUNT(*)                          AS n_districts
        FROM '{PUBLIC_PANEL}'
        GROUP BY year_num
        ORDER BY year_num
    """,

    "district_history": f"""
        SELECT year_num, *
        FROM '{PUBLIC_PANEL}'
        WHERE cds = '38-68478-0000000'
        ORDER BY year_num
    """,

    "high_missingness_districts": f"""
        WITH coverage AS (
            SELECT cds,
                    SUM(CASE WHEN caaspp_math_met_pct IS NULL THEN 1 ELSE 0 END)
                        / COUNT(*)::DOUBLE AS pct_missing_math
            FROM '{PUBLIC_PANEL}'
            GROUP BY cds
        )
        SELECT * FROM coverage WHERE pct_missing_math > 0.30
    """,

    "top_volatility": f"""
        SELECT cds,
                STDDEV(caaspp_math_met_pct) AS math_volatility,
                STDDEV(chronic_absenteeism_rate) AS absent_volatility
        FROM '{PUBLIC_PANEL}'
        GROUP BY cds
        HAVING math_volatility IS NOT NULL
        ORDER BY math_volatility DESC
        LIMIT 10
    """,

    "near_boundary_vs_full": f"""
        -- Compare near-boundary districts (from Layer 6) against the full panel.
        -- The CSV of near-boundary CDS codes can be supplied via local file:
        SELECT m.cds,
                AVG(m.caaspp_math_met_pct) AS math_avg,
                AVG(m.chronic_absenteeism_rate) AS absent_avg
        FROM '{PUBLIC_PANEL}' m
        GROUP BY m.cds
    """,
}


def run_examples() -> dict:
    """Run all examples; useful as a smoke check."""
    out: dict = {}
    for name, sql in EXAMPLE_QUERIES.items():
        try:
            out[name] = {"ok": True, "rows": int(len(query(sql)))}
        except Exception as e:  # noqa: BLE001
            out[name] = {"ok": False, "error": f"{type(e).__name__}: {e}"}
    return out
