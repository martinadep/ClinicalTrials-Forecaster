"""
Rank candidate sites for a planned trial by predicted recruitment velocity
(Spark MLlib). Importable by the API -- no __main__ demo block.

Loads the PipelineModel fitted by train.py once and keeps both the SparkSession
and the model in memory across calls (module-level cache) -- starting a new
SparkSession per prediction would add multi-second JVM startup to every request.

Serving-latency note: a local Spark session still carries ~1-2s of per-job
overhead even when the session itself is reused, since each .transform()/.collect()
is its own Spark job. Accepted for this demo; production serving would export the
model (e.g. ONNX) for low-latency inference. Not implemented here.
"""
import json
import os
import sys

import setuptools  # noqa: F401 -- must be imported before pyspark.ml on Python 3.12+,
# which removed the stdlib `distutils` module that pyspark 3.5.x still references;
# importing setuptools first makes its vendored distutils shim available.

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from models.mesh_area_map import AREAS
from shared.conditions import has_non_diagnostic_condition as _detect_non_diagnostic

ARTIFACTS_DIR = os.path.join(os.path.dirname(__file__), "artifacts")
ARTIFACT_PATH = os.path.join(ARTIFACTS_DIR, "velocity_pipeline")
DEFAULTS_PATH = os.path.join(ARTIFACTS_DIR, "defaults.json")

# Fields the caller must supply -- there's no sensible dataset-wide default for
# what disease the trial treats, how big it is, or whether it's interventional
# vs observational, so we fail fast with a clear error rather than silently
# feeding None into Spark.
REQUIRED_TRIAL_FIELDS = ["study_type", "primary_purpose", "phase", "enrollment_count"]

_spark = None
_model = None
_defaults = None


def _get_spark():
    global _spark
    if _spark is None:
        from pyspark.sql import SparkSession

        _spark = (
            SparkSession.builder.appName("velocity_model_predict")
            .master("local[*]")
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
            .getOrCreate()
        )
        _spark.sparkContext.setLogLevel("ERROR")
    return _spark


def _get_model():
    global _model
    if _model is None:
        from pyspark.ml import PipelineModel

        spark = _get_spark()  # ensures the session exists before loading
        if not os.path.exists(ARTIFACT_PATH):
            raise FileNotFoundError(f"No trained model at {ARTIFACT_PATH}. Run `python -m models.train` first.")
        _model = PipelineModel.load(ARTIFACT_PATH)
    return _model


def _get_defaults():
    """Load the training-set mode/median defaults train.py persisted for optional fields.

    Computed from the actual training data (not hardcoded) so the defaults stay
    representative across retrains instead of silently going stale.
    """
    global _defaults
    if _defaults is None:
        if not os.path.exists(DEFAULTS_PATH):
            raise FileNotFoundError(f"No defaults file at {DEFAULTS_PATH}. Run `python -m models.train` first.")
        with open(DEFAULTS_PATH, encoding="utf-8") as f:
            _defaults = json.load(f)
    return _defaults


def _resolve_trial_params(trial_params, candidate_sites):
    """Validate required fields and fill in optional ones.

    lead_sponsor_class, sex are optional -- missing values fall back to the
    training set's mode, persisted in defaults.json by train.py.

    n_sites is handled separately: it's the user's *planned* site count, so a
    dataset-wide default would be a poor proxy. If omitted, we fall back to
    len(candidate_sites) -- "however many candidates are being considered" is a
    more contextually grounded stand-in than a global median.

    areas / has_non_diagnostic_condition -- inference-area contract (proposed,
    to confirm with the dashboard/API owner): there are no MeSH ids for a trial
    that hasn't run yet, so the area_* multi-hot columns can't be derived the
    way they are at training time. Instead the caller supplies `areas` directly
    -- a list of the same ~15 area keys in models.mesh_area_map.AREAS, presumably
    offered as a multi-select in the dashboard UI. Missing/empty defaults to no
    area selected (all area_* columns 0, area_other always 0 at inference --
    "other" only means something relative to actual MeSH ids). Similarly,
    has_non_diagnostic_condition can be passed directly as a bool, or derived
    from a `conditions` list of raw free-text strings via the same shared
    matcher silver_to_gold.py uses; defaults to False if neither is given.
    """
    missing_required = [f for f in REQUIRED_TRIAL_FIELDS if trial_params.get(f) is None]
    if missing_required:
        raise ValueError(f"trial_params missing required field(s): {missing_required}")

    defaults = _get_defaults()
    resolved = dict(trial_params)
    for field in ["lead_sponsor_class", "sex"]:
        if resolved.get(field) is None:
            resolved[field] = defaults[field]
    if resolved.get("n_sites") is None:
        resolved["n_sites"] = len(candidate_sites)

    selected_areas = set(resolved.get("areas") or [])
    unknown_areas = selected_areas - set(AREAS)
    if unknown_areas:
        raise ValueError(f"trial_params['areas'] has unknown area(s): {sorted(unknown_areas)}")
    resolved["areas"] = selected_areas

    if resolved.get("has_non_diagnostic_condition") is None:
        resolved["has_non_diagnostic_condition"] = _detect_non_diagnostic(resolved.get("conditions"))

    return resolved


def _build_candidate_row(resolved_trial_params, site):
    """One row = the planned trial's fixed fields + this candidate's site history.

    Single-site feature mapping: training averages avg_site_exp/avg_site_vel over
    all of a trial's sites, but at inference we score one candidate at a time, so
    for this row avg_site_exp = the candidate's own n_trials and avg_site_vel = the
    candidate's own avg_velocity (both from gold.site_history).
    """
    row = {
        "study_type": resolved_trial_params["study_type"],
        "primary_purpose": resolved_trial_params["primary_purpose"],
        "lead_sponsor_class": resolved_trial_params["lead_sponsor_class"],
        "sex": resolved_trial_params["sex"],
        "phase": resolved_trial_params["phase"],
        "enrollment_count": resolved_trial_params["enrollment_count"],
        "n_sites": resolved_trial_params["n_sites"],
        "avg_site_exp": site.get("n_trials"),
        "avg_site_vel": site.get("avg_velocity"),
        "has_non_diagnostic_condition": int(bool(resolved_trial_params["has_non_diagnostic_condition"])),
        "area_other": 0,
    }
    selected_areas = resolved_trial_params["areas"]
    for area in AREAS:
        row[f"area_{area}"] = 1 if area in selected_areas else 0
    return row


def predict_ranking(trial_params: dict, candidate_sites: list):
    """Score each candidate site for the planned trial, return ranked (site, predicted_velocity).

    trial_params required fields: study_type, primary_purpose, phase, enrollment_count.
    Optional: lead_sponsor_class, sex, n_sites, areas, has_non_diagnostic_condition
    (or conditions) -- see _resolve_trial_params for how missing values are filled in.

    candidate_sites: pre-fetched gold.site_history rows (dicts with at least
    n_trials/avg_velocity, plus whatever identifying fields the caller wants
    echoed back, e.g. facility_name/city/country). Pre-fetched rather than
    looked up here by identifier: the caller (API layer) almost always already
    holds these rows from gold.site_history for other purposes (map display,
    the geographic hard-filter), so passing them through avoids a redundant
    DB round-trip and keeps this function pure/easy to test.

    Geography is a hard filter applied by the caller before calling this
    function -- candidate_sites should already be limited to the user's chosen
    regions; this function ranks whatever it's given, it doesn't filter further.

    Cold start: a candidate missing n_trials/avg_velocity (not in gold.site_history,
    or incomplete) is skipped with a printed note rather than imputed -- inventing
    a "neutral" history for an unknown site seems more misleading for ranking
    purposes than just surfacing that it has no track record.
    """
    resolved_trial_params = _resolve_trial_params(trial_params, candidate_sites)

    valid_sites = []
    rows = []
    for site in candidate_sites:
        if site.get("n_trials") is None or site.get("avg_velocity") is None:
            print(f"[INFO]: skipping candidate with no site history: {site}")
            continue
        valid_sites.append(site)
        rows.append(_build_candidate_row(resolved_trial_params, site))

    if not rows:
        return []

    import pandas as pd

    spark = _get_spark()
    model = _get_model()

    candidates_df = spark.createDataFrame(pd.DataFrame(rows))
    predictions = model.transform(candidates_df)

    import pyspark.sql.functions as F

    predictions = predictions.withColumn("predicted_velocity", F.expm1(F.col("prediction")))
    predicted_velocities = [row["predicted_velocity"] for row in predictions.select("predicted_velocity").collect()]

    ranked = list(zip(valid_sites, predicted_velocities))
    ranked.sort(key=lambda pair: pair[1], reverse=True)
    return ranked
