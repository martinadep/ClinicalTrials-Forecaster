import os
import sys
import pandas as pd

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from shared.config import load_dotenv
from shared.db import build_dsn_from_env

load_dotenv()

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, Imputer
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator 

MODEL_PATH = "models/saved_models/spark_velocity_pipeline"

def train_spark_model():
    spark = SparkSession.builder \
        .appName("ClinicalTrials-Forecaster-Training") \
        .master("local[*]") \
        .getOrCreate()
    
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
    
    df = df.dropna(subset=["target_velocity"])
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

    # Nota: Assicurati che combacino con le colonne usate in fase di inferenza
    categorical_cols = ["primary_purpose", "lead_sponsor_class", "sex"]
    numeric_cols = ["enrollment_count", "n_sites", "num_conditions", "duration_months"]
    site_historical_cols = ["avg_site_exp", "avg_site_vel"]

    indexers = [StringIndexer(inputCol=col, outputCol=f"{col}_index", handleInvalid="keep") for col in categorical_cols]
    encoders = [OneHotEncoder(inputCol=f"{col}_index", outputCol=f"{col}_vec") for col in categorical_cols]

    imputer_numeric = Imputer(inputCols=numeric_cols, outputCols=[f"{col}_imputed" for col in numeric_cols], strategy="median")
    imputer_site = Imputer(inputCols=site_historical_cols, outputCols=[f"{col}_imputed" for col in site_historical_cols], strategy="mean")

    assembler_inputs = [f"{col}_vec" for col in categorical_cols] + [f"{col}_imputed" for col in numeric_cols] + [f"{col}_imputed" for col in site_historical_cols]
    assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="features")

    rf = RandomForestRegressor(featuresCol="features", labelCol="target_velocity", numTrees=100, seed=42)

    pipeline_stages = indexers + encoders + [imputer_numeric, imputer_site, assembler, rf]
    pipeline = Pipeline(stages=pipeline_stages)

    print("[SPARK ML]: Avvio del training distribuito...")
    model_pipeline = pipeline.fit(train_df)
    print("[SPARK ML]: Training completato con successo!")

    print("[SPARK ML]: Generazione delle predizioni sul Test Set per la valutazione...")
    predictions = model_pipeline.transform(test_df)

    evaluator_r2 = RegressionEvaluator(labelCol="target_velocity", predictionCol="prediction", metricName="r2")
    evaluator_rmse = RegressionEvaluator(labelCol="target_velocity", predictionCol="prediction", metricName="rmse")
    evaluator_mae = RegressionEvaluator(labelCol="target_velocity", predictionCol="prediction", metricName="mae")

    r2 = evaluator_r2.evaluate(predictions)
    rmse = evaluator_rmse.evaluate(predictions)
    mae = evaluator_mae.evaluate(predictions)

    print("\n TEST SET EVALUATION:")
    print("-" * 55)
    print(f"  -> Determination (R²): {r2:.4f}")
    print(f"  -> Root Mean Squared Error (RMSE):      {rmse:.2f} patients/month")
    print(f"  -> Mean Absolute Error (MAE):           {mae:.2f} patients/month")
    print("-" * 55 + "\n")

    model_pipeline.write().overwrite().save(MODEL_PATH)
    print(f"[SPARK ML]: Pipeline saved in: {MODEL_PATH}")
    
    spark.stop()


# ---------------------------------------------------------------------------
# NUOVA FUNZIONE: predict_site_rankings (Aggiunta per gestire l'inferenza)
# ---------------------------------------------------------------------------
def predict_site_rankings(new_trial_request: dict, candidate_sites: list) -> list:
    """ Riceve le specifiche di un nuovo trial e l'elenco dei siti storici,

    crea il dataset completo per Spark ML, applica la pipeline salvata
    e restituisce i siti ordinati per velocity predetta decrescente.
    """
    spark = SparkSession.builder \
        .appName("ClinicalTrials-Forecaster-Inference") \
        .master("local[*]") \
        .getOrCreate()
    
    # 1. Controlliamo se esiste il modello salvato
    if not os.path.exists(MODEL_PATH):
        spark.stop()
        raise FileNotFoundError(f"[ERR]: Modello non trovato in {MODEL_PATH}. Esegui prima il training.")
        
    model = PipelineModel.load(MODEL_PATH)
    
    # 2. Costruiamo la lista di dizionari unendo le specifiche fisse del trial a ogni sito candididato
    # Nota bene: Calcoliamo dinamicamente n_sites basandoci sulla lunghezza della lista candidati
    n_sites_total = len(candidate_sites)
    
    inference_data = []
    for site in candidate_sites:
        row = {
            # Dati identificativi del sito
            "facility_name": site.get("facility_name"),
            "city": site.get("city"),
            "zip": site.get("zip"),
            "country": site.get("country"),
            "latitude": site.get("latitude"),
            "longitude": site.get("longitude"),
            
            # Caratteristiche fisse del nuovo Trial richieste dal modello
            "primary_purpose": new_trial_request.get("primary_purpose"),
            "lead_sponsor_class": new_trial_request.get("lead_sponsor_class"),
            "sex": new_trial_request.get("sex"),
            "enrollment_count": new_trial_request.get("enrollment_count"),
            "num_conditions": new_trial_request.get("num_conditions"),
            "duration_months": new_trial_request.get("duration_months"),
            "n_sites": n_sites_total,
            
            # Caratteristiche storiche del sito specifico (possono essere anche None, l'Imputer gestirà il fallback)
            "avg_site_exp": site.get("n_trials"),  # Mappatura corretta tra db (n_trials) e modello (avg_site_exp)
            "avg_site_vel": site.get("avg_velocity")
        }
        inference_data.append(row)
        
    # 3. Trasformiamo in DataFrame Spark
    pdf_inference = pd.DataFrame(inference_data)
    df_spark = spark.createDataFrame(pdf_inference)
    
    # 4. Applichiamo la pipeline ML caricata per ottenere la colonna 'prediction'
    predictions_df = model.transform(df_spark)
    
    # Rinominiamo la colonna e selezioniamo i dati necessari da riconvertire in Python
    output_df = predictions_df.withColumnRenamed("prediction", "predicted_velocity") \
                              .select("facility_name", "city", "zip", "country", "latitude", "longitude", "predicted_velocity")
                              
    # 5. Convertiamo indietro in lista di dizionari ed ordiniamo per velocity decrescente
    results = [row.asDict() for row in output_df.collect()]
    results = sorted(results, key=lambda x: x["predicted_velocity"], reverse=True)
    
    spark.stop()
    return results


if __name__ == "__main__":
    train_spark_model()