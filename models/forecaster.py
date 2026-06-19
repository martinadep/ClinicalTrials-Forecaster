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
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.regression import RandomForestRegressor

def train_spark_model():
    # 1. Creiamo una Spark Session locale basilare (senza driver JDBC)
    spark = SparkSession.builder \
        .appName("ClinicalTrials-Forecaster-Training") \
        .master("local[*]") \
        .getOrCreate()
    
    # Recuperiamo il DSN di progetto
    dsn = build_dsn_from_env() or os.getenv("DATABASE_URL")
    if not dsn:
        raise ValueError("[ERR]: DSN non trovato. Controlla il file .env")
        
    print("[ML]: Lettura dati tramite Python/Pandas (senza bisogno di driver JDBC)...")
    # Leggiamo i dati sfruttando la connessione Python nativa, che non soffre dei blocchi di Windows
    pdf = pd.read_sql("SELECT * FROM gold.trial_features", dsn)
    
    if pdf.empty:
        print("[ERR]: La tabella gold.trial_features è vuota!")
        spark.stop()
        return

    print("[SPARK ML]: Conversione del dataset in Spark DataFrame distribuito...")
    # Convertiamo il DataFrame Pandas in un DataFrame Spark. Da questo punto in poi è al 100% Spark ML!
    df = spark.createDataFrame(pdf)

    # 2. Split in Train e Test distribuito (80% / 20%)
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

    # 3. Definizione del Preprocessing con Spark ML Transformers
    categorical_cols = ["study_type", "primary_purpose", "lead_sponsor_class", "sex", "phase"]
    numeric_cols = ["enrollment_count", "n_sites", "num_conditions", "duration_months", "avg_site_exp", "avg_site_vel"]

    indexers = [StringIndexer(inputCol=col, outputCol=f"{col}_index", handleInvalid="keep") for col in categorical_cols]
    encoders = [OneHotEncoder(inputCol=f"{col}_index", outputCol=f"{col}_vec") for col in categorical_cols]

    assembler_inputs = [f"{col}_vec" for col in categorical_cols] + numeric_cols
    assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="features")

    # 4. Definizione del Regressore (Random Forest di Spark ML)
    rf = RandomForestRegressor(featuresCol="features", labelCol="target_velocity", numTrees=100, seed=42)

    # 5. Pipeline di Spark ML
    pipeline_stages = indexers + encoders + [assembler, rf]
    pipeline = Pipeline(stages=pipeline_stages)

    print("[SPARK ML]: Avvio del training distribuito...")
    model_pipeline = pipeline.fit(train_df)

    print("[SPARK ML]: Training completato con successo!")

    # 6. SALVATAGGIO DELLA PIPELINE SPARK
    model_path = "models/saved_models/spark_velocity_pipeline"
    model_pipeline.write().overwrite().save(model_path)
    print(f"[SPARK ML]: Pipeline salvata con successo in: {model_path}")
    
    spark.stop()

from pyspark.ml import PipelineModel

def predict_site_rankings(new_trial_data: dict, candidate_sites: list) -> list:
    """
    Funzione di Inferenza usata dall'API (Step 5 del PDF).
    Sfrutta Spark in modalità locale per applicare la pipeline salvata sui candidati.
    """
    model_path = "models/saved_models/spark_velocity_pipeline"
    if not os.path.exists(model_path):
        raise FileNotFoundError("[ERR]: Pipeline di Spark non trovata. Esegui prima il training!")

    # Inizializza o recupera una sessione Spark locale e leggera per l'API
    spark = SparkSession.builder \
        .appName("Forecaster-Inference-API") \
        .master("local[*]") \
        .getOrCreate()

    # Carica la PipelineModel di Spark salvata (include i trasformatori del testo e i pesi dell'albero)
    model = PipelineModel.load(model_path)

    # Costruiamo le righe di inferenza unendo il nuovo trial con lo storico dei candidati
    inference_rows = []
    for site in candidate_sites:
        row = {
            "study_type": new_trial_data["study_type"],
            "primary_purpose": new_trial_data["primary_purpose"],
            "lead_sponsor_class": new_trial_data["lead_sponsor_class"],
            "sex": new_trial_data["sex"],
            "healthy_volunteers": new_trial_data["healthy_volunteers"],
            "phase": new_trial_data["phase"],
            "enrollment_count": int(new_trial_data["enrollment_count"]),
            "n_sites": len(candidate_sites),
            "num_conditions": int(new_trial_data["num_conditions"]),
            "duration_months": float(new_trial_data["duration_months"]),
            
            # Feature storiche del sito specifico
            "avg_site_exp": float(site["n_trials"]),
            "avg_site_vel": float(site["avg_velocity"])
        }
        inference_rows.append(row)

    # Creiamo un DataFrame Spark a partire dalla lista di dizionari Python
    df_inference = spark.createDataFrame(inference_rows)

    # Applichiamo la pipeline del modello. Spark ML genererà una colonna nativa chiamata "prediction"
    df_predictions = model.transform(df_inference)

    # Riportiamo i risultati in Python (.collect() va benissimo sui pochi siti candidati estratti)
    predicted_data = df_predictions.select("prediction").collect()

    # Mappiamo le predizioni con l'anagrafica dei siti per la Dashboard
    rankings = []
    for i, site in enumerate(candidate_sites):
        rankings.append({
            "facility_name": site["facility_name"],
            "city": site["city"],
            "zip": site["zip"],
            "country": site["country"],
            "latitude": float(site["latitude"]),
            "longitude": float(site["longitude"]),
            "predicted_velocity": round(float(predicted_data[i]["prediction"]), 2)
        })

    # Ordiniamo la classifica (Velocity maggiore = Posizione più alta)
    rankings.sort(key=lambda x: x["predicted_velocity"], reverse=True)

    return rankings


if __name__ == "__main__":
    train_spark_model()