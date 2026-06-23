"""
Shared feature spec for the recruitment-velocity model (Spark MLlib).

This is the single source of truth for which gold.trial_features columns become
model features and how they're encoded. Both train.py and predict.py call
build_feature_stages() so the fitted PipelineModel (which bundles the encoders
together with the regressor) guarantees train/predict can never see different
columns or encodings.
"""
from pyspark.sql import functions as F

from models.mesh_area_map import AREAS, MESH_TO_AREAS

# Low-cardinality categoricals -> StringIndexer + OneHotEncoder.
# study_type was previously dropped (constant -- all INTERVENTIONAL per the
# original EDA), but the silver_to_gold.py INTERVENTIONAL-only filter was since
# removed upstream, so gold.trial_features now includes OBSERVATIONAL trials
# too and study_type carries real signal -- added back as a feature.
CATEGORICAL_COLS = ["primary_purpose", "lead_sponsor_class", "sex", "phase", "study_type"]

# Therapeutic-area multi-hot columns (one binary column per area in
# models/mesh_area_map.py, plus area_other for top-300 MeSH ids that weren't
# assigned to any area). A trial can set more than one (e.g. a breast cancer
# trial sets both oncology and womens_reproductive_health).
AREA_COLS = [f"area_{a}" for a in AREAS] + ["area_other"]

# Numeric features, used as-is (tree models don't need scaling and are robust
# to the outliers flagged in data_exploration/gold_profile.md, e.g. enrollment_count,
# n_sites).
#
# NOTE - target leakage (accepted project simplification, not fixed here):
# avg_site_vel is computed in gold.trial_features from all-time site history,
# which includes each trial's own velocity contribution. This will likely
# dominate feature importance for that reason. A production version would
# compute site stats using only trials completed before each trial's start_date.
NUMERIC_COLS = [
    "enrollment_count", "n_sites", "avg_site_exp", "avg_site_vel",
    "has_non_diagnostic_condition",
] + AREA_COLS

# Deliberately NOT included (the assembled "features" vector only ever draws
# from the one-hot/numeric columns built below, regardless of what else is in
# the source DataFrame):
#   - nct_id: identifier, not a feature
#   - target_velocity: the target, not a feature
#   - duration_months: removed -- target_velocity is literally
#     enrollment_count / duration_months by construction in bronze_to_silver.py,
#     so keeping both enrollment_count and duration_months as predictors lets
#     the model largely just learn to reconstruct the target's own formula
#     rather than genuinely model recruitment dynamics
#   - num_conditions: removed upstream when conditions were re-encoded as
#     MeSH ids (gold.trial_features no longer has this column) -- replaced by
#     the area_* multi-hot columns derived from mesh_conditions_ids below
#   - mesh_conditions_ids: not a model input directly, only the area_*
#     columns derived from it (inference has no MeSH ids -- see predict.py)

FEATURES_COL = "features"


def add_area_multihot_columns(df, mesh_ids_col="mesh_conditions_ids"):
    """Derive the area_* multi-hot columns from a MeSH-id array column (training only).

    Spark-native (array_intersect/array_except), no UDF, so it scales to the
    full gold.trial_features read. At inference there are no MeSH ids for a
    not-yet-run trial -- predict.py builds these same columns directly from
    the caller's selected area(s) instead of calling this function.
    """
    mesh_col = F.coalesce(F.col(mesh_ids_col), F.array().cast("array<string>"))

    for area in AREAS:
        area_ids = [mesh_id for mesh_id, areas in MESH_TO_AREAS.items() if area in areas]
        ids_array = F.array(*[F.lit(i) for i in area_ids])
        df = df.withColumn(
            f"area_{area}",
            F.when(F.size(F.array_intersect(mesh_col, ids_array)) > 0, 1).otherwise(0),
        )

    mapped_ids_array = F.array(*[F.lit(i) for i in MESH_TO_AREAS.keys()])
    df = df.withColumn(
        "area_other",
        F.when(F.size(F.array_except(mesh_col, mapped_ids_array)) > 0, 1).otherwise(0),
    )
    return df


def build_feature_stages():
    """Build the unfitted Spark ML pipeline stages for feature engineering.

    Returns (stages, assembled_feature_col) where `stages` is the ordered list
    of StringIndexer/OneHotEncoder/VectorAssembler transformers to prepend to a
    Pipeline before the regressor. handleInvalid="keep" on both the indexer and
    the encoder means a category never seen during training (e.g. an unseen
    `phase` value at predict time) is routed to an extra "unknown" bucket
    instead of raising -- this is what makes predict.py crash-safe on new data.
    """
    from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler

    indexers = [
        StringIndexer(inputCol=col, outputCol=f"{col}_idx", handleInvalid="keep")
        for col in CATEGORICAL_COLS
    ]
    encoders = [
        OneHotEncoder(inputCol=f"{col}_idx", outputCol=f"{col}_vec", handleInvalid="keep")
        for col in CATEGORICAL_COLS
    ]
    assembler = VectorAssembler(
        inputCols=[f"{col}_vec" for col in CATEGORICAL_COLS] + NUMERIC_COLS,
        outputCol=FEATURES_COL,
    )

    stages = indexers + encoders + [assembler]
    return stages, FEATURES_COL
