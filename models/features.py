"""
Shared feature spec for the recruitment-velocity model (Spark MLlib).

This is the single source of truth for which gold.trial_features columns become
model features and how they're encoded. Both train.py and predict.py call
build_feature_stages() so the fitted PipelineModel (which bundles the encoders
together with the regressor) guarantees train/predict can never see different
columns or encodings.
"""

# Low-cardinality categoricals -> StringIndexer + OneHotEncoder.
# study_type was previously dropped (constant -- all INTERVENTIONAL per the
# original EDA), but the silver_to_gold.py INTERVENTIONAL-only filter was since
# removed upstream, so gold.trial_features now includes OBSERVATIONAL trials
# too and study_type carries real signal -- added back as a feature.
CATEGORICAL_COLS = ["primary_purpose", "lead_sponsor_class", "sex", "phase", "study_type"]

# Numeric features, used as-is (tree models don't need scaling and are robust
# to the outliers flagged in data_exploration/gold_profile.md, e.g. enrollment_count,
# n_sites).
#
# NOTE - target leakage (accepted project simplification, not fixed here):
# avg_site_vel is computed in gold.trial_features from all-time site history,
# which includes each trial's own velocity contribution. This will likely
# dominate feature importance for that reason. A production version would
# compute site stats using only trials completed before each trial's start_date.
NUMERIC_COLS = ["enrollment_count", "n_sites", "num_conditions", "duration_months", "avg_site_exp", "avg_site_vel"]

# Deliberately NOT included (the assembled "features" vector only ever draws
# from the one-hot/numeric columns built below, regardless of what else is in
# the source DataFrame):
#   - nct_id: identifier, not a feature
#   - target_velocity: the target, not a feature

FEATURES_COL = "features"


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
