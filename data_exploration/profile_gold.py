"""
Gold-layer EDA profiler.

Connects to Postgres (read-only, SELECT only), profiles every column of
gold.trial_features and gold.site_history, and writes a markdown report to
data_exploration/gold_profile.md.

Column type (categorical / boolean / numeric) is determined by querying
information_schema.columns rather than hardcoding a column list, so the report
stays correct if the gold schema evolves.

Run from the project root:

    python -m data_exploration.profile_gold
"""
import os
import warnings

import pandas as pd
import psycopg2

from shared.config import load_dotenv
from shared.db import build_dsn_from_env

OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "gold_profile.md")
TABLES = ["gold.trial_features", "gold.site_history"]
TARGET_COLUMN = "target_velocity"
HIGH_CARDINALITY_THRESHOLD = 20
DOMINANT_VALUE_SHARE = 0.95

NUMERIC_TYPES = {
    "integer", "bigint", "smallint", "numeric", "double precision", "real",
}
BOOLEAN_TYPES = {"boolean"}


def get_columns(cur, schema, table):
    """Return [(column_name, data_type), ...] in declared column order."""
    cur.execute(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """,
        (schema, table),
    )
    return cur.fetchall()


def classify_column(data_type):
    if data_type in BOOLEAN_TYPES:
        return "boolean"
    if data_type in NUMERIC_TYPES:
        return "numeric"
    return "categorical"


def null_stats(series, total_rows):
    null_count = int(series.isna().sum())
    null_pct = (null_count / total_rows * 100) if total_rows else 0.0
    return null_count, null_pct


def profile_numeric(series, total_rows):
    null_count, null_pct = null_stats(series, total_rows)
    non_null = series.dropna()
    flags = []

    if non_null.empty:
        flags.append("ENTIRELY NULL")
        return {
            "null_count": null_count, "null_pct": null_pct,
            "count": 0, "mean": None, "min": None, "max": None,
            "median": None, "p25": None, "p75": None, "p99": None,
        }, flags

    if non_null.nunique() <= 1:
        flags.append(f"CONSTANT (all non-null values = {non_null.iloc[0]})")

    p25, median, p75, p99 = non_null.quantile([0.25, 0.5, 0.75, 0.99])
    stats = {
        "null_count": null_count, "null_pct": null_pct,
        "count": int(non_null.count()), "mean": float(non_null.mean()),
        "min": float(non_null.min()), "max": float(non_null.max()),
        "median": float(median), "p25": float(p25), "p75": float(p75), "p99": float(p99),
    }

    if stats["p99"] and stats["p99"] > 0 and stats["max"] > 10 * stats["p99"]:
        flags.append(
            f"SURPRISING DISTRIBUTION: max ({stats['max']:.2f}) is >10x p99 ({stats['p99']:.2f}) "
            "-- likely extreme outliers"
        )

    return stats, flags


def profile_boolean(series, total_rows):
    null_count, null_pct = null_stats(series, total_rows)
    non_null = series.dropna()
    true_count = int((non_null == True).sum())   # noqa: E712 (pandas bool comparison)
    false_count = int((non_null == False).sum())  # noqa: E712
    flags = []
    if non_null.empty:
        flags.append("ENTIRELY NULL")
    elif true_count == 0 or false_count == 0:
        flags.append("CONSTANT (only one boolean value present)")
    return {
        "null_count": null_count, "null_pct": null_pct,
        "true_count": true_count, "false_count": false_count,
    }, flags


def profile_categorical(series, total_rows):
    null_count, null_pct = null_stats(series, total_rows)
    non_null = series.dropna()
    flags = []
    value_counts = non_null.value_counts()
    nunique = int(value_counts.shape[0])

    if non_null.empty:
        flags.append("ENTIRELY NULL")
    elif nunique <= 1:
        flags.append(f"CONSTANT (all non-null values = {non_null.iloc[0]!r})")
    else:
        top_share = value_counts.iloc[0] / len(non_null)
        if top_share >= DOMINANT_VALUE_SHARE:
            flags.append(
                f"DOMINANT VALUE: {value_counts.index[0]!r} is {top_share * 100:.1f}% "
                "of non-null values"
            )

    high_cardinality = nunique > HIGH_CARDINALITY_THRESHOLD
    if high_cardinality:
        flags.append(f"HIGH CARDINALITY: {nunique} distinct values")

    return {
        "null_count": null_count, "null_pct": null_pct,
        "nunique": nunique, "value_counts": value_counts,
        "high_cardinality": high_cardinality,
    }, flags


def render_numeric_section(lines, col, stats):
    lines.append(f"#### `{col}` (numeric)")
    lines.append("")
    lines.append(
        f"- non-null count: {stats['count']} | null: {stats['null_count']} ({stats['null_pct']:.1f}%)"
    )
    if stats["count"] > 0:
        lines.append(
            f"- mean: {stats['mean']:.4g} | min: {stats['min']:.4g} | max: {stats['max']:.4g}"
        )
        lines.append(
            f"- median (p50): {stats['median']:.4g} | p25: {stats['p25']:.4g} | "
            f"p75: {stats['p75']:.4g} | p99: {stats['p99']:.4g}"
        )
    lines.append("")


def render_boolean_section(lines, col, stats):
    lines.append(f"#### `{col}` (boolean)")
    lines.append("")
    lines.append(
        f"- true: {stats['true_count']} | false: {stats['false_count']} | "
        f"null: {stats['null_count']} ({stats['null_pct']:.1f}%)"
    )
    lines.append("")


def render_categorical_section(lines, col, stats):
    lines.append(f"#### `{col}` (categorical/text)")
    lines.append("")
    lines.append(
        f"- distinct values: {stats['nunique']} | null: {stats['null_count']} ({stats['null_pct']:.1f}%)"
    )
    if stats["high_cardinality"]:
        lines.append(f"- note: high-cardinality column, showing top {HIGH_CARDINALITY_THRESHOLD} values")
    lines.append("")
    lines.append("| value | count |")
    lines.append("|---|---|")
    shown = stats["value_counts"].head(HIGH_CARDINALITY_THRESHOLD if stats["high_cardinality"] else None)
    for value, count in shown.items():
        lines.append(f"| {value} | {count} |")
    if stats["high_cardinality"]:
        remaining = stats["nunique"] - HIGH_CARDINALITY_THRESHOLD
        lines.append(f"| ... ({remaining} more distinct values) | |")
    lines.append("")


def profile_table(df, cur, schema, table, lines, all_flags):
    full_name = f"{schema}.{table}"
    total_rows = len(df)
    columns = get_columns(cur, schema, table)

    lines.append(f"## `{full_name}` ({total_rows} rows)")
    lines.append("")

    for col, data_type in columns:
        kind = classify_column(data_type)
        series = df[col]

        if kind == "numeric":
            stats, flags = profile_numeric(series, total_rows)
            render_numeric_section(lines, col, stats)
        elif kind == "boolean":
            stats, flags = profile_boolean(series, total_rows)
            render_boolean_section(lines, col, stats)
        else:
            stats, flags = profile_categorical(series, total_rows)
            render_categorical_section(lines, col, stats)

        for flag in flags:
            all_flags.append(f"`{full_name}.{col}`: {flag}")


def render_target_section(lines, trial_features_df):
    lines.append("## Target: `target_velocity`")
    lines.append("")
    if TARGET_COLUMN not in trial_features_df.columns:
        lines.append(f"Column `{TARGET_COLUMN}` not found in gold.trial_features.")
        lines.append("")
        return

    total_rows = len(trial_features_df)
    stats, flags = profile_numeric(trial_features_df[TARGET_COLUMN], total_rows)
    render_numeric_section(lines, TARGET_COLUMN, stats)
    if flags:
        lines.append("Notes:")
        for flag in flags:
            lines.append(f"- {flag}")
        lines.append("")


def main():
    load_dotenv()
    dsn = build_dsn_from_env()
    conn = psycopg2.connect(dsn)

    lines = ["# Gold-layer EDA profile", ""]
    all_flags = []

    try:
        with conn.cursor() as cur:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)  # pandas/psycopg2 connection warning
                dataframes = {
                    full_name: pd.read_sql(f"SELECT * FROM {full_name}", conn)
                    for full_name in TABLES
                }

            lines.append("## Row counts")
            lines.append("")
            for full_name, df in dataframes.items():
                lines.append(f"- `{full_name}`: {len(df)} rows")
            lines.append("")

            trial_features_df = dataframes.get("gold.trial_features")
            if trial_features_df is not None:
                render_target_section(lines, trial_features_df)

            for full_name, df in dataframes.items():
                schema, table = full_name.split(".")
                profile_table(df, cur, schema, table, lines, all_flags)
    finally:
        conn.close()

    lines.append("## Flags (model-relevant warnings)")
    lines.append("")
    if all_flags:
        for flag in all_flags:
            lines.append(f"- {flag}")
    else:
        lines.append("None.")
    lines.append("")

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"[INFO]: Wrote report to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
