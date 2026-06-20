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
    # Esclusi: nct_id (ID), state, latitude, longitude (non utili come predittori)
    categorical_cols = ["study_type", "primary_purpose", "lead_sponsor_class", "sex", "phase"]
    numeric_cols = ["enrollment_count", "n_sites", "num_conditions", "duration_months"]
    site_historical_cols = ["avg_site_exp", "avg_site_vel"]

    # Categoriche: handleInvalid="keep" assegna un indice speciale ai valori Null/NaN
    indexers = [StringIndexer(inputCol=col, outputCol=f"{col}_index", handleInvalid="keep") for col in categorical_cols]
    encoders = [OneHotEncoder(inputCol=f"{col}_index", outputCol=f"{col}_vec") for col in categorical_cols]

    # -------------------------------------------------------------------------
    # GESTIONE NULLI (TRAINING): Imputazione colonne numeriche
    # -------------------------------------------------------------------------
    # Feature del Trial: Usiamo la Mediana (resiliente agli outlier)
    imputed_numeric_cols = [f"{col}_imputed" for col in numeric_cols]
    imputer_numeric = Imputer(
        inputCols=numeric_cols, 
        outputCols=imputed_numeric_cols,
        strategy="median"
    )

    # Feature Storiche dei Siti: Usiamo la Media globale
    imputed_site_cols = [f"{col}_imputed" for col in site_historical_cols]
    imputer_site = Imputer(
        inputCols=site_historical_cols, 
        outputCols=imputed_site_cols,
        strategy="mean"
    )

    # Assembler: unisce solo i vettori e le colonne imputate (senza Null)
    assembler_inputs = [f"{col}_vec" for col in categorical_cols] + imputed_numeric_cols + imputed_site_cols
    assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="features")

    # 4. Definizione del Regressore (Random Forest di Spark ML)
    rf = RandomForestRegressor(featuresCol="features", labelCol="target_velocity", numTrees=100, seed=42)

    # 5. Pipeline di Spark ML (Inclusi i due Imputer)
    pipeline_stages = indexers + encoders + [imputer_numeric, imputer_site, assembler, rf]
    pipeline = Pipeline(stages=pipeline_stages)

    print("[SPARK ML]: Avvio del training distribuito con gestione dei Null...")
    model_pipeline = pipeline.fit(train_df)

    print("[SPARK ML]: Training completato con successo!")

    # 6. SALVATAGGIO DELLA PIPELINE SPARK
    model_path = "models/saved_models/spark_velocity_pipeline"
    model_pipeline.write().overwrite().save(model_path)
    print(f"[SPARK ML]: Pipeline salvata con successo in: {model_path}")
    
    spark.stop()


def predict_site_rankings(new_trial_data: dict, candidate_sites: list) -> list:
    """
    Funzione di Inferenza usata dall'API.
    Sfrutta Spark in modalità locale per applicare la pipeline salvata sui candidati.
    """
    model_path = "models/saved_models/spark_velocity_pipeline"
    if not os.path.exists(model_path):
        raise FileNotFoundError("[ERR]: Pipeline di Spark non trovata. Esegui prima il training!")

    spark = SparkSession.builder \
        .appName("Forecaster-Inference-API") \
        .master("local[*]") \
        .getOrCreate()

    model = PipelineModel.load(model_path)

    # Costruiamo le righe di inferenza unendo il nuovo trial con lo storico dei candidati
    inference_rows = []
    for site in candidate_sites:
        # -------------------------------------------------------------------------
        # GESTIONE NULLI (INFERENZA): Fallback sicuri se il DB ha valori vuoti
        # -------------------------------------------------------------------------
        row = {
            "study_type": new_trial_data.get("study_type"),
            "primary_purpose": new_trial_data.get("primary_purpose"),
            "lead_sponsor_class": new_trial_data.get("lead_sponsor_class"),
            "sex": new_trial_data.get("sex"),
            "healthy_volunteers": str(new_trial_data.get("healthy_volunteers", "false")),
            "phase": new_trial_data.get("phase"),
            "enrollment_count": int(new_trial_data["enrollment_count"]) if new_trial_data.get("enrollment_count") is not None else None,
            "n_sites": len(candidate_sites),
            "num_conditions": int(new_trial_data["num_conditions"]) if new_trial_data.get("num_conditions") is not None else None,
            "duration_months": float(new_trial_data["duration_months"]) if new_trial_data.get("duration_months") is not None else None,
            
            # Feature storiche del sito (se il centro è nuovo e non ha storico, assegniamo 0.0)
            "avg_site_exp": float(site["n_trials"]) if site.get("n_trials") is not None else 0.0,
            "avg_site_vel": float(site["avg_velocity"]) if site.get("avg_velocity") is not None else 0.0
        }
        inference_rows.append(row)

    df_inference = spark.createDataFrame(inference_rows)

    # Applichiamo la pipeline. L'imputer interno gestirà gli eventuali None passati sopra.
    df_predictions = model.transform(df_inference)
    predicted_data = df_predictions.select("prediction").collect()

    # Mappiamo le predizioni con l'anagrafica dei siti per la Dashboard
    rankings = []
    for i, site in enumerate(candidate_sites):
        rankings.append({
            "facility_name": site.get("facility_name", "Unknown Facility"),
            "city": site.get("city", "Unknown City"),
            "zip": site.get("zip", "N/A"),
            "country": site.get("country", "Unknown"),
            # Fallback a 0.0 per la mappa se mancano le coordinate nel DB
            "latitude": float(site["latitude"]) if site.get("latitude") is not None else 0.0,
            "longitude": float(site["longitude"]) if site.get("longitude") is not None else 0.0,
            "predicted_velocity": round(float(predicted_data[i]["prediction"]), 2)
        })

    # Ordiniamo la classifica (Velocity maggiore = Posizione più alta)
    rankings.sort(key=lambda x: x["predicted_velocity"], reverse=True)

    return rankings


if __name__ == "__main__":
    train_spark_model()