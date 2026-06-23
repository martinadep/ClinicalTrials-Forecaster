"""
Train the recruitment-velocity Gradient Boosted Trees model (Spark MLlib).

Loads gold.trial_features via JDBC (read-only), builds the feature pipeline from
features.py, trains a GBTRegressor on log1p(target_velocity) to handle its right
skew, evaluates in real units, and saves the fitted PipelineModel to
models/artifacts/velocity_pipeline.

Run directly: python -m models.train
"""
import json
import os
import sys
from datetime import datetime, timezone

import setuptools  # noqa: F401 -- must be imported before pyspark.ml on Python 3.12+,
# which removed the stdlib `distutils` module that pyspark 3.5.x still references;
# importing setuptools first makes its vendored distutils shim available.

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from shared.config import load_dotenv
from shared.db import build_jdbc_url_from_env

load_dotenv()

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import GBTRegressor
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from models.features import add_area_multihot_columns, build_feature_stages, FEATURES_COL

ARTIFACTS_DIR = os.path.join(os.path.dirname(__file__), "artifacts")
ARTIFACT_PATH = os.path.join(ARTIFACTS_DIR, "velocity_pipeline")
CANDIDATE_PATH = os.path.join(ARTIFACTS_DIR, "velocity_pipeline_candidate")
METRICS_PATH = os.path.join(ARTIFACTS_DIR, "metrics.json")
DEFAULTS_PATH = os.path.join(ARTIFACTS_DIR, "defaults.json")

TARGET_COL = "target_velocity"
LOG_TARGET_COL = "log_target"
RANDOM_STATE = 42
MAX_ITER = 100  # number of boosting rounds (GBT's analogue of RF's numTrees)
TOP_N_IMPORTANCES = 15

# Champion/challenger promotion gate: a freshly trained model only replaces the
# currently-served one if its R2 isn't more than this much worse. Guards against
# silently degrading production after a bad upstream data refresh (we've seen
# exactly that happen more than once in this project's pipeline already).
R2_REGRESSION_TOLERANCE = 0.02


def compute_optional_field_defaults(train_df):
    """Mode (categoricals) / median (numerics) for predict.py's optional trial_params.

    Computed from the train split so defaults.json reflects the same data the
    model was actually fit on, and stays current across retrains instead of
    being hardcoded.
    """
    def mode_of(col):
        row = train_df.groupBy(col).count().orderBy(F.desc("count")).first()
        return row[col]

    def median_of(col):
        return train_df.approxQuantile(col, [0.5], 0.0)[0]

    return {
        "lead_sponsor_class": mode_of("lead_sponsor_class"),
        "sex": mode_of("sex"),
    }


def load_previous_metrics():
    if not os.path.exists(METRICS_PATH):
        return None
    with open(METRICS_PATH, encoding="utf-8") as f:
        return json.load(f)


def get_feature_names(df, vector_col):
    """Map VectorAssembler output slot indices back to human-readable names.

    Spark attaches an ML attribute group to the assembled vector column describing
    each slot (binary slots for one-hot dummies, numeric slots for passthrough
    columns). featureImportances is a vector aligned to these same slot indices.
    """
    attrs = df.schema[vector_col].metadata["ml_attr"]["attrs"]
    names_by_idx = {}
    for group in attrs.values():
        for attr in group:
            names_by_idx[attr["idx"]] = attr["name"]
    return [names_by_idx[i] for i in range(len(names_by_idx))]


def main():
    spark = (
        SparkSession.builder.appName("velocity_model_train")
        .master("local[*]")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    jdbc_url, jdbc_props = build_jdbc_url_from_env()
    df = spark.read.jdbc(url=jdbc_url, table="gold.trial_features", properties=jdbc_props)
    loaded_count = df.count()

    df = df.withColumn(
        "has_non_diagnostic_condition",
        F.coalesce(F.col("has_non_diagnostic_condition").cast("int"), F.lit(0)),
    )
    df = add_area_multihot_columns(df)

    df = df.filter(F.col(TARGET_COL).isNotNull())
    dropped_count = loaded_count - df.count()

    df = df.withColumn(LOG_TARGET_COL, F.log1p(F.col(TARGET_COL)))
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=RANDOM_STATE)
    train_count, test_count = train_df.count(), test_df.count()

    feature_stages, features_col = build_feature_stages()
    gbt = GBTRegressor(
        labelCol=LOG_TARGET_COL, featuresCol=features_col, maxIter=MAX_ITER, seed=RANDOM_STATE
    )
    pipeline = Pipeline(stages=feature_stages + [gbt])

    fitted_pipeline = pipeline.fit(train_df)

    predictions = fitted_pipeline.transform(test_df)
    # Invert the log1p transform to report errors in real patients/month units.
    predictions = predictions.withColumn("prediction_real", F.expm1(F.col("prediction")))

    mae = RegressionEvaluator(labelCol=TARGET_COL, predictionCol="prediction_real", metricName="mae").evaluate(predictions)
    rmse = RegressionEvaluator(labelCol=TARGET_COL, predictionCol="prediction_real", metricName="rmse").evaluate(predictions)
    r2 = RegressionEvaluator(labelCol=TARGET_COL, predictionCol="prediction_real", metricName="r2").evaluate(predictions)

    feature_names = get_feature_names(predictions, features_col)
    gbt_model = fitted_pipeline.stages[-1]
    importances = sorted(zip(feature_names, gbt_model.featureImportances.toArray()), key=lambda x: x[1], reverse=True)

    print(f"[INFO]: rows loaded={loaded_count} dropped(null target)={dropped_count} "
          f"train={train_count} test={test_count}")
    print(f"[INFO]: test set -- MAE={mae:.3f} RMSE={rmse:.3f} R2={r2:.4f}")
    print(f"[INFO]: top {TOP_N_IMPORTANCES} feature importances:")
    for name, importance in importances[:TOP_N_IMPORTANCES]:
        print(f"  {name:40s} {importance:.4f}")

    new_metrics = {
        "mae": mae, "rmse": rmse, "r2": r2,
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "train_count": train_count, "test_count": test_count,
    }
    previous_metrics = load_previous_metrics()

    os.makedirs(ARTIFACTS_DIR, exist_ok=True)

    if previous_metrics is None:
        promote, reason = True, "no previous model on record (first training run)"
    elif r2 >= previous_metrics["r2"] - R2_REGRESSION_TOLERANCE:
        promote, reason = True, f"R2 {r2:.4f} vs previous {previous_metrics['r2']:.4f} (within tolerance)"
    else:
        promote, reason = False, f"R2 {r2:.4f} vs previous {previous_metrics['r2']:.4f} (regression > {R2_REGRESSION_TOLERANCE})"

    if promote:
        fitted_pipeline.write().overwrite().save(ARTIFACT_PATH)
        with open(METRICS_PATH, "w", encoding="utf-8") as f:
            json.dump(new_metrics, f, indent=2)
        defaults = compute_optional_field_defaults(train_df)
        with open(DEFAULTS_PATH, "w", encoding="utf-8") as f:
            json.dump(defaults, f, indent=2)
        print(f"[INFO]: PROMOTED new model to {ARTIFACT_PATH} -- {reason}")
    else:
        fitted_pipeline.write().overwrite().save(CANDIDATE_PATH)
        print(f"[WARN]: REJECTED new model, kept serving previous one -- {reason}")
        print(f"[WARN]: challenger saved for inspection at {CANDIDATE_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()
