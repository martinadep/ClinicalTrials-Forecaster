"""
Conditions / MeSH profiler.

Connects to Postgres (read-only, SELECT only) and profiles how trial conditions
are represented across the bronze/silver/gold layers, to support designing a
manual mapping from MeSH terms into ~10-15 therapeutic-area categories.

Run from the project root:

    python -m data_exploration.profile_conditions
"""
import os

import psycopg2

from shared.config import load_dotenv
from shared.db import build_dsn_from_env

OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "conditions_profile.md")
TOP_N = 40
SAMPLE_N = 20


def fetch_one(cur, query, params=None):
    cur.execute(query, params or ())
    row = cur.fetchone()
    return row[0] if row else None


def fetch_all(cur, query, params=None):
    cur.execute(query, params or ())
    return cur.fetchall()


def section_raw_conditions(cur):
    """Profile bronze.trials.conditions -- raw free-text strings, JSONB array per trial."""
    total_trials = fetch_one(cur, "SELECT count(*) FROM bronze.trials")
    samples = fetch_all(
        cur,
        """
        SELECT conditions FROM bronze.trials
        WHERE conditions IS NOT NULL AND conditions != '[]'::jsonb
        LIMIT %s
        """,
        (SAMPLE_N,),
    )

    distinct_raw = fetch_one(
        cur,
        """
        SELECT count(DISTINCT value)
        FROM bronze.trials, jsonb_array_elements_text(conditions) AS value
        """,
    )

    top_raw = fetch_all(
        cur,
        """
        SELECT value, count(*) AS n
        FROM bronze.trials, jsonb_array_elements_text(conditions) AS value
        GROUP BY value
        ORDER BY n DESC
        LIMIT %s
        """,
        (TOP_N,),
    )

    per_trial_stats = fetch_all(
        cur,
        """
        SELECT
            min(n), max(n),
            percentile_cont(0.5) WITHIN GROUP (ORDER BY n),
            percentile_cont(0.99) WITHIN GROUP (ORDER BY n)
        FROM (
            SELECT jsonb_array_length(conditions) AS n
            FROM bronze.trials
            WHERE conditions IS NOT NULL
        ) sub
        """,
    )[0]

    return {
        "total_trials": total_trials,
        "samples": [str(s[0]) for s in samples],
        "distinct_raw": distinct_raw,
        "top_raw": top_raw,
        "per_trial_stats": per_trial_stats,
    }


def section_mesh_conditions(cur):
    """Profile bronze.trials.mesh_conditions -- raw MeSH (id, term) pairs from the API."""
    coverage = fetch_all(
        cur,
        """
        SELECT
            count(*) FILTER (WHERE mesh_conditions IS NOT NULL AND mesh_conditions != '[]'::jsonb),
            count(*)
        FROM bronze.trials
        """,
    )[0]

    samples = fetch_all(
        cur,
        """
        SELECT mesh_conditions FROM bronze.trials
        WHERE mesh_conditions IS NOT NULL AND mesh_conditions != '[]'::jsonb
        LIMIT %s
        """,
        (SAMPLE_N,),
    )

    per_trial_stats = fetch_all(
        cur,
        """
        SELECT
            min(n), max(n),
            percentile_cont(0.5) WITHIN GROUP (ORDER BY n),
            percentile_cont(0.99) WITHIN GROUP (ORDER BY n)
        FROM (
            SELECT jsonb_array_length(mesh_conditions) AS n
            FROM bronze.trials
            WHERE mesh_conditions IS NOT NULL
        ) sub
        """,
    )[0]

    return {
        "with_mesh": coverage[0],
        "total": coverage[1],
        "samples": [str(s[0]) for s in samples],
        "per_trial_stats": per_trial_stats,
    }


def section_dim_mesh_conditions(cur):
    """Profile gold.dim_mesh_conditions -- the deduped (id -> name) lookup table."""
    distinct_count = fetch_one(cur, "SELECT count(*) FROM gold.dim_mesh_conditions")
    top_used = fetch_all(
        cur,
        """
        SELECT d.mesh_condition_id, d.mesh_condition_name, count(*) AS n_trials
        FROM gold.trial_features t, unnest(t.mesh_conditions_ids) AS mesh_id
        JOIN gold.dim_mesh_conditions d ON d.mesh_condition_id = mesh_id
        GROUP BY d.mesh_condition_id, d.mesh_condition_name
        ORDER BY n_trials DESC
        LIMIT %s
        """,
        (TOP_N,),
    )
    return {"distinct_count": distinct_count, "top_used": top_used}


def section_gold_trial_features(cur):
    """Profile gold.trial_features.mesh_conditions_ids -- the array used by the model/pipeline."""
    coverage = fetch_all(
        cur,
        """
        SELECT
            count(*) FILTER (WHERE mesh_conditions_ids IS NOT NULL AND array_length(mesh_conditions_ids, 1) > 0),
            count(*)
        FROM gold.trial_features
        """,
    )[0]

    per_trial_stats = fetch_all(
        cur,
        """
        SELECT
            min(n), max(n),
            percentile_cont(0.5) WITHIN GROUP (ORDER BY n),
            percentile_cont(0.99) WITHIN GROUP (ORDER BY n)
        FROM (
            SELECT array_length(mesh_conditions_ids, 1) AS n
            FROM gold.trial_features
            WHERE mesh_conditions_ids IS NOT NULL
        ) sub
        """,
    )[0]

    return {"with_mesh": coverage[0], "total": coverage[1], "per_trial_stats": per_trial_stats}


def section_missing_mesh(cur):
    """Profile raw condition strings for trials with no MeSH terms at all.

    Useful for the manual bucketing design: distinguishes trials with a real
    diagnosis the API just didn't resolve to MeSH, from trials that have no
    diagnosis to map (healthy volunteers, PK/method studies, etc.).
    """
    total = fetch_one(cur, "SELECT count(*) FROM bronze.trials")
    missing = fetch_one(
        cur,
        "SELECT count(*) FROM bronze.trials WHERE mesh_conditions IS NULL OR mesh_conditions = '[]'::jsonb",
    )
    top_missing = fetch_all(
        cur,
        """
        SELECT value, count(*) AS n
        FROM bronze.trials, jsonb_array_elements_text(conditions) AS value
        WHERE mesh_conditions IS NULL OR mesh_conditions = '[]'::jsonb
        GROUP BY value
        ORDER BY n DESC
        LIMIT %s
        """,
        (TOP_N,),
    )
    return {"total": total, "missing": missing, "top_missing": top_missing}


def section_normalization_issues(cur):
    """Spot-check raw condition strings for case/duplicate/encoding issues."""
    case_variants = fetch_all(
        cur,
        """
        SELECT lower(value) AS normalized, count(DISTINCT value) AS variant_count,
               array_agg(DISTINCT value) AS variants
        FROM bronze.trials, jsonb_array_elements_text(conditions) AS value
        GROUP BY lower(value)
        HAVING count(DISTINCT value) > 1
        ORDER BY variant_count DESC
        LIMIT 15
        """,
    )
    return {"case_variants": case_variants}


def fmt_stats(stats):
    mn, mx, p50, p99 = stats
    return f"min={mn}, median={p50}, p99={p99}, max={mx}"


def write_report(raw, mesh, dim_mesh, gold_tf, missing_mesh, norm):
    lines = []
    lines.append("# Conditions / MeSH profile\n")
    lines.append(
        "Profiles how trial conditions are represented across bronze/silver/gold, "
        "to support designing a manual mapping into ~10-15 therapeutic-area categories.\n"
    )

    lines.append("## Where conditions live\n")
    lines.append(
        "- `bronze.trials.conditions` (JSONB array) -- raw free-text condition strings, "
        "exactly as ClinicalTrials.gov reports them (one trial can list multiple).\n"
        "- `bronze.trials.mesh_conditions` (JSONB array of `{id, term}`) -- MeSH-coded "
        "condition terms from the API's `derivedSection.conditionBrowseModule.meshes`, "
        "backfilled from `bronze.raw_trials.payload`.\n"
        "- `gold.dim_mesh_conditions` (id -> name lookup table) and "
        "`gold.trial_features.mesh_conditions_ids` (TEXT[] of MeSH ids per trial) -- "
        "the MeSH ids actually used downstream (site history, the ML pipeline).\n"
        "- `gold.site_conditions_history` -- (country, city, zip, mesh_condition_id) -> "
        "trial count, i.e. condition history per site.\n"
    )

    lines.append("## Raw conditions (`bronze.trials.conditions`)\n")
    lines.append(f"- Trials profiled: {raw['total_trials']}\n")
    lines.append(f"- Distinct raw condition strings: {raw['distinct_raw']}\n")
    lines.append(f"- Conditions per trial: {fmt_stats(raw['per_trial_stats'])}\n")
    lines.append("\n**Sample raw values (as stored):**\n")
    for s in raw["samples"][:SAMPLE_N]:
        lines.append(f"- `{s}`\n")
    lines.append(f"\n**Top {TOP_N} most frequent raw condition strings:**\n\n")
    lines.append("| condition | count |\n|---|---|\n")
    for value, n in raw["top_raw"]:
        lines.append(f"| {value} | {n} |\n")

    lines.append("\n## MeSH-coded conditions (`bronze.trials.mesh_conditions`)\n")
    pct = (mesh["with_mesh"] / mesh["total"] * 100) if mesh["total"] else 0
    lines.append(f"- Coverage: {mesh['with_mesh']}/{mesh['total']} trials have a MeSH value ({pct:.1f}%)\n")
    lines.append(f"- MeSH terms per trial: {fmt_stats(mesh['per_trial_stats'])}\n")
    lines.append("\n**Sample MeSH values (as stored, `{id, term}` pairs):**\n")
    for s in mesh["samples"][:SAMPLE_N]:
        lines.append(f"- `{s}`\n")

    lines.append("\n## MeSH dimension table (`gold.dim_mesh_conditions`)\n")
    lines.append(f"- Distinct MeSH ids: {dim_mesh['distinct_count']}\n")
    lines.append(f"\n**Top {TOP_N} MeSH conditions by trial count:**\n\n")
    lines.append("| mesh_condition_id | mesh_condition_name | n_trials |\n|---|---|---|\n")
    for mid, name, n in dim_mesh["top_used"]:
        lines.append(f"| {mid} | {name} | {n} |\n")

    lines.append("\n## `gold.trial_features.mesh_conditions_ids` (what the model/pipeline sees)\n")
    pct_tf = (gold_tf["with_mesh"] / gold_tf["total"] * 100) if gold_tf["total"] else 0
    lines.append(f"- Coverage: {gold_tf['with_mesh']}/{gold_tf['total']} trials have at least one MeSH id ({pct_tf:.1f}%)\n")
    lines.append(f"- MeSH ids per trial: {fmt_stats(gold_tf['per_trial_stats'])}\n")

    lines.append("\n## Trials with no MeSH terms at all -- what are their raw conditions?\n")
    pct_missing = (missing_mesh["missing"] / missing_mesh["total"] * 100) if missing_mesh["total"] else 0
    lines.append(
        f"- {missing_mesh['missing']}/{missing_mesh['total']} trials ({pct_missing:.1f}%) have no MeSH "
        "terms at all (`mesh_conditions` null or empty).\n"
    )
    lines.append(
        "- Their raw condition strings split into two distinct groups: (1) **non-diagnosis terms** "
        "that have nothing to map -- healthy-volunteer/PK/method studies (`Healthy`, `Healthy Volunteers`, "
        "`Anesthesia`, `Pharmacokinetics`, `Bioequivalence`, `Surgery`, ...) -- these likely want a "
        "dedicated \"Healthy / no condition\" bucket rather than a keyword rule; and (2) **genuine "
        "diagnoses the API just didn't resolve to MeSH** (`HIV`, `Solid Tumors`, `Plaque Psoriasis`, "
        "`Acute Myocardial Infarction`, `Kidney Transplantation`, ...) -- these are readable and can "
        "still get an ordinary keyword rule.\n"
    )
    lines.append(f"\n**Top {TOP_N} raw condition strings among no-MeSH trials:**\n\n")
    lines.append("| condition | count |\n|---|---|\n")
    for value, n in missing_mesh["top_missing"]:
        lines.append(f"| {value} | {n} |\n")

    lines.append("\n## Normalization issues (case/duplicate variants in raw strings)\n")
    if norm["case_variants"]:
        lines.append("Raw strings that collapse to the same value once lowercased "
                      "(case inconsistency in the source data):\n\n")
        lines.append("| normalized | variant count | variants |\n|---|---|---|\n")
        for normalized, variant_count, variants in norm["case_variants"]:
            lines.append(f"| {normalized} | {variant_count} | {variants} |\n")
    else:
        lines.append("No case-variant duplicates found in the sampled raw strings.\n")
    lines.append(
        "\nNote: MeSH ids sidestep most of this -- the same condition reported with "
        "different free-text casing/spelling/abbreviation (e.g. \"Type 2 Diabetes\" vs "
        "\"T2DM\") generally maps to a single MeSH id, which is the main reason this "
        "project switched from free-text keyword mapping to MeSH ids.\n"
    )

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        f.writelines(lines)


def main():
    load_dotenv()
    dsn = build_dsn_from_env()
    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor() as cur:
            raw = section_raw_conditions(cur)
            mesh = section_mesh_conditions(cur)
            dim_mesh = section_dim_mesh_conditions(cur)
            gold_tf = section_gold_trial_features(cur)
            missing_mesh = section_missing_mesh(cur)
            norm = section_normalization_issues(cur)
    finally:
        conn.close()

    write_report(raw, mesh, dim_mesh, gold_tf, missing_mesh, norm)
    print(f"[INFO]: wrote {OUTPUT_PATH}")
    print(f"[INFO]: distinct raw condition strings = {raw['distinct_raw']}")
    print(f"[INFO]: distinct MeSH ids = {dim_mesh['distinct_count']}")
    print(f"[INFO]: MeSH coverage (bronze) = {mesh['with_mesh']}/{mesh['total']}")


if __name__ == "__main__":
    main()
