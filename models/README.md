# models

The recruitment-velocity model: predicts a planned trial's recruitment velocity
(patients enrolled per month) and ranks candidate sites by it. Built with
`pyspark.ml` to stay consistent with the project's Spark architecture.

**Design:** trained at the trial level (one row per trial, target = the trial's
own recruitment velocity), but used at the site level — at inference, the model
runs once per candidate site (same trial fields, that site's history swapped
in), and candidates are ranked by predicted velocity. There's no per-site
recruitment outcome in the source data (ClinicalTrials.gov only reports
trial-level totals), so site quality is captured indirectly via each
candidate's `gold.site_history` stats.

## `features.py`

Shared feature spec — the single source of truth for which `gold.trial_features`
columns become model inputs, imported by both `train.py` and `predict.py` so
they can never see different columns or encodings.

- `CATEGORICAL_COLS`: `primary_purpose`, `lead_sponsor_class`, `sex`, `phase`,
  `study_type` — encoded via `StringIndexer` → `OneHotEncoder`, both with
  `handleInvalid="keep"` so a category never seen during training doesn't crash
  prediction, it just routes to an extra "unknown" bucket.
- `NUMERIC_COLS`: `enrollment_count`, `n_sites`, `num_conditions`,
  `duration_months`, `avg_site_exp`, `avg_site_vel` — used as-is, no scaling
  (tree models don't need it).
- Dropped: `nct_id` (identifier), `target_velocity` (the target).
- **Leakage note (by design, not fixed):** `avg_site_vel` is computed in
  `gold.trial_features` from all-time site history that includes each trial's
  own velocity contribution — mild target leakage, accepted as a project
  simplification. In practice `enrollment_count`/`duration_months` dominate
  feature importance even more directly, since `target_velocity` is literally
  `enrollment_count / duration_months` by construction in `bronze_to_silver.py`.
- `build_feature_stages()` returns the *unfitted* Pipeline stages (no model)
  — `train.py` appends the regressor and fits the whole thing together, which
  is what lets the saved `PipelineModel` bundle the encoders and the model as
  one artifact.

## `train.py`

Trains, evaluates, and conditionally promotes a new model.

1. Reads `gold.trial_features` via JDBC (read-only), drops rows with a null
   target (logs how many).
2. Trains on `log1p(target_velocity)` to handle its right skew (min 0, heavily
   skewed — see `data_exploration/gold_profile.md`); `RandomForestRegressor`,
   200 trees, fixed seed, 80/20 split.
3. Evaluates on the test set in **real units** — predictions are inverted with
   `expm1` before computing MAE/RMSE/R² — and prints feature importances mapped
   back to human-readable names (via the assembled vector's ML attribute
   metadata, since `featureImportances` is just an index-aligned array).
4. **Champion/challenger promotion gate:** compares the new model's R² against
   the currently-served model's recorded R² (`artifacts/metrics.json`). Only
   promotes (overwrites the live model) if it isn't more than
   `R2_REGRESSION_TOLERANCE` (0.02) worse — otherwise the challenger is saved
   separately to `artifacts/velocity_pipeline_candidate` for inspection and the
   live model is left untouched. First-ever run always promotes (nothing to
   compare against yet). This exists because we've seen upstream data/pipeline
   regressions silently degrade results more than once already in this
   project — the gate stops a bad retrain from auto-deploying.
5. On promotion, also writes `artifacts/defaults.json` — the training set's
   mode (categoricals) / median (numerics) for `predict.py`'s optional fields,
   recomputed fresh each promoted retrain so it never goes stale.

**Run it** (inside the `spark` Docker service, same as the ETL jobs):

```bash
docker exec --user root clinical_trial_spark bash -c "cd /app && /opt/spark/bin/spark-submit --master local[*] --packages org.postgresql:postgresql:42.7.3 models/train.py"
```

Look for `[INFO]: rows loaded=... dropped(null target)=... train=... test=...`,
the MAE/RMSE/R² line, the feature importances, and finally either
`PROMOTED new model to ...` or `REJECTED new model, kept serving previous one`.

## `predict.py`

Ranks candidate sites for a planned trial. Importable by an API layer — no
`__main__` block.

- `predict_ranking(trial_params: dict, candidate_sites: list)` → sorted
  `[(site, predicted_velocity), ...]`, descending.
- **Required** `trial_params` fields: `study_type`, `primary_purpose`, `phase`,
  `enrollment_count` — no sensible dataset-wide default exists for what disease
  a trial treats or how big it is, so these raise a clear `ValueError` if
  missing rather than silently feeding `None` into Spark.
- **Optional**: `lead_sponsor_class`, `sex`, `num_conditions`,
  `duration_months` — fall back to `artifacts/defaults.json` (the training
  set's mode/median). `n_sites` is handled separately: it's the user's
  *planned* site count, so it falls back to `len(candidate_sites)` instead of
  a dataset-wide default — "however many candidates are being considered" is a
  more contextually grounded stand-in.
- **`candidate_sites`** are pre-fetched `gold.site_history` rows (dicts with at
  least `n_trials`/`avg_velocity`, plus whatever identifying fields the caller
  wants echoed back). Pre-fetched rather than looked up here by identifier: the
  caller almost certainly already holds these rows for other purposes (map
  display, the geographic hard-filter), so passing them through avoids a
  redundant DB round-trip and keeps this function pure and easy to test.
- **Geography is a hard filter applied by the caller** before calling this
  function — `predict_ranking` ranks whatever candidates it's given, it doesn't
  filter further.
- **Cold start:** a candidate missing `n_trials`/`avg_velocity` (not in
  `gold.site_history`) is skipped with a printed note rather than imputed —
  inventing a "neutral" history for an unknown site seemed more misleading for
  ranking purposes than just surfacing that it has no track record.
- **Single-site feature mapping:** training averages `avg_site_exp`/
  `avg_site_vel` over all of a trial's sites, but inference scores one
  candidate at a time, so for each row `avg_site_exp` = that candidate's own
  `n_trials` and `avg_site_vel` = that candidate's own `avg_velocity`.
- Keeps the `SparkSession` and loaded `PipelineModel` cached at module level
  across calls — starting a new session per prediction would add multi-second
  JVM startup to every request.
- **Serving-latency note:** even with a reused session, each `.transform()`/
  `.collect()` is its own Spark job (~1-2s overhead). Accepted for this demo;
  production serving would export the model (e.g. ONNX) for low-latency
  inference — not implemented here.

No standalone run command — import `predict_ranking` from another script
(e.g. a future `ml_api.py`) or a throwaway test script run the same way as
`train.py` above.

## `artifacts/` (gitignored — regenerated by `train.py`, not committed)

- `velocity_pipeline/` — the live (promoted) `PipelineModel`, Spark's native
  save format (a directory, not a single file).
- `velocity_pipeline_candidate/` — the most recent *rejected* challenger, kept
  for inspection.
- `metrics.json` — the live model's MAE/RMSE/R², training/test row counts, and
  timestamp.
- `defaults.json` — training-set mode/median values `predict.py` uses for
  optional `trial_params` fields.

## Removed

`forecaster.py`, `generate_mock_gold.py`, and the root-level `test_inference.py`
were an earlier, separate Spark ML prototype (mock data + an embedded pipeline)
that predated and is superseded by this `features.py`/`train.py`/`predict.py`
implementation — removed together since `test_inference.py` only existed to
exercise `forecaster.py`.
