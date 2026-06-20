import os
import sys
import pandas as pd

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Integrazione con l'infrastruttura del team
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from shared.config import load_dotenv
from shared.db import build_dsn_from_env

load_dotenv()

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, Imputer
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml import PipelineModel

from pyspark.ml.evaluation import RegressionEvaluator  # <--- NUOVO IMPORT

def train_spark_model():
    # 1. Creiamo una Spark Session locale basilare
    spark = SparkSession.builder \
        .appName("ClinicalTrials-Forecaster-Training") \
        .master("local[*]") \
        .getOrCreate()
    
    # Recuperiamo il DSN di progetto
    dsn = build_dsn_from_env() or os.getenv("DATABASE_URL")
    if not dsn:
        raise ValueError("[ERR]: DSN non trovato. Controlla il file .env")
        
    print("[ML]: Lettura dati tramite Python/Pandas...")
    pdf = pd.read_sql("SELECT * FROM gold.trial_features", dsn)
    
    if pdf.empty:
        print("[ERR]: La tabella gold.trial_features è vuota!")
        spark.stop()
        return

    print("[SPARK ML]: Conversione del dataset in Spark DataFrame distribuito...")
    df = spark.createDataFrame(pdf)

    # -------------------------------------------------------------------------
    # GESTIONE NULLI (TRAINING): Rimozione righe senza Target (Y)
    # -------------------------------------------------------------------------
    df = df.dropna(subset=["target_velocity"])

    # 2. Split in Train e Test distribuito (80% / 20%)
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

    # 3. Definizione del Preprocessing con Spark ML Transformers
    categorical_cols = ["study_type", "primary_purpose", "lead_sponsor_class", "sex", "phase"]
    numeric_cols = ["enrollment_count", "n_sites", "num_conditions", "duration_months"]
    site_historical_cols = ["avg_site_exp", "avg_site_vel"]

    indexers = [StringIndexer(inputCol=col, outputCol=f"{col}_index", handleInvalid="keep") for col in categorical_cols]
    encoders = [OneHotEncoder(inputCol=f"{col}_index", outputCol=f"{col}_vec") for col in categorical_cols]

    imputer_numeric = Imputer(inputCols=numeric_cols, outputCols=[f"{col}_imputed" for col in numeric_cols], strategy="median")
    imputer_site = Imputer(inputCols=site_historical_cols, outputCols=[f"{col}_imputed" for col in site_historical_cols], strategy="mean")

    assembler_inputs = [f"{col}_vec" for col in categorical_cols] + [f"{col}_imputed" for col in numeric_cols] + [f"{col}_imputed" for col in site_historical_cols]
    assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="features")

    # 4. Definizione del Regressore (Random Forest di Spark ML)
    rf = RandomForestRegressor(featuresCol="features", labelCol="target_velocity", numTrees=100, seed=42)

    # 5. Pipeline di Spark ML
    pipeline_stages = indexers + encoders + [imputer_numeric, imputer_site, assembler, rf]
    pipeline = Pipeline(stages=pipeline_stages)

    print("[SPARK ML]: Avvio del training distribuito con gestione dei Null...")
    model_pipeline = pipeline.fit(train_df)
    print("[SPARK ML]: Training completato con successo!")

    # -------------------------------------------------------------------------
    # VALUTAZIONE DEL MODELLO SUL TEST SET (METRICHE DI PERFORMANCE)
    # -------------------------------------------------------------------------
    print("[SPARK ML]: Generazione delle predizioni sul Test Set per la valutazione...")
    predictions = model_pipeline.transform(test_df)

    # Inizializziamo i valutatori per le diverse metriche
    evaluator_r2 = RegressionEvaluator(labelCol="target_velocity", predictionCol="prediction", metricName="r2")
    evaluator_rmse = RegressionEvaluator(labelCol="target_velocity", predictionCol="prediction", metricName="rmse")
    evaluator_mae = RegressionEvaluator(labelCol="target_velocity", predictionCol="prediction", metricName="mae")

    r2 = evaluator_r2.evaluate(predictions)
    rmse = evaluator_rmse.evaluate(predictions)
    mae = evaluator_mae.evaluate(predictions)

    print("\n REPORT PRESTAZIONI MODELLO (TEST SET EVALUATION):")
    print("-" * 55)
    print(f"  -> Coefficiente di Determinazione (R²): {r2:.4f}")
    print(f"  -> Root Mean Squared Error (RMSE):      {rmse:.2f} pazienti/mese")
    print(f"  -> Mean Absolute Error (MAE):           {mae:.2f} pazienti/mese")
    print("-" * 55 + "\n")
    # -------------------------------------------------------------------------

    # 6. SALVATAGGIO DELLA PIPELINE SPARK
    model_path = "models/saved_models/spark_velocity_pipeline"
    model_pipeline.write().overwrite().save(model_path)
    print(f"[SPARK ML]: Pipeline salvata con successo in: {model_path}")
    
    spark.stop()