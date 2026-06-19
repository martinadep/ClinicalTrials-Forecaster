import os
import sys
import psycopg2
import psycopg2.extras

# 1. Carica le configurazioni del team
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from shared.config import load_dotenv
from shared.db import build_dsn_from_env
from models.forecaster import predict_site_rankings

load_dotenv()

def run_inference_test():
    # Connessione a Postgres usando i metodi in shared
    dsn = build_dsn_from_env() or os.getenv("DATABASE_URL")
    if not dsn:
        print("[ERR]: Connessione al database non configurata nel file .env")
        return

    conn = psycopg2.connect(dsn)
    # Usiamo RealDictCursor così i dati estratti da Postgres diventano dizionari Python pesanti
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    try:
        # 2. SIMULA L'ESTRAZIONE GEOGRAFICA (Compito di Person M / API) [cite: 54, 56, 115]
        # Chiediamo i siti candidati per una specifica area (es. Italy) [cite: 43, 56, 68]
        print("[TEST]: Estrazione dei siti candidati in Italia da gold.site_history...")
        cursor.execute("""
            SELECT facility_name, city, zip, country, latitude, longitude, n_trials, avg_velocity 
            FROM gold.site_history 
            WHERE country = 'Italy';
        """)
        candidate_sites = cursor.fetchall()

        if not candidate_sites:
            print("[WARN]: Nessun sito trovato in Italia. Hai inserito i mock data?")
            return

        # 3. SIMULA L'INPUT DI UN NUOVO TRIAL DALLA DASHBOARD (Compito di Person C / UI) [cite: 5, 43, 119]
        # Un utente inserisce i parametri di un nuovo studio che vuole pianificare [cite: 5, 43, 67]
        new_trial_request = {
            "study_type": "INTERVENTIONAL",
            "primary_purpose": "TREATMENT",
            "lead_sponsor_class": "INDUSTRY",
            "sex": "ALL",
            "healthy_volunteers": True,
            "phase": "PHASE3",
            "enrollment_count": 400,
            "num_conditions": 1,
            "duration_months": 12.0
        }

        print(f"[TEST]: Trovati {len(candidate_sites)} siti candidati. Avvio inferenza Spark ML...")

        # 4. INVOCAZIONE DELLA TUA FUNZIONE DI PREDIZIONE [cite: 72]
        # Calcoliamo la velocity predetta per ciascun candidato accoppiato al trial [cite: 44, 72, 79]
        ranked_results = predict_site_rankings(new_trial_request, candidate_sites)

        # 5. STAMPA DEI RISULTATI ORDINATI (Quello che vedrà la Dashboard) [cite: 46, 73, 99]
        print("\n🏆 CLASSIFICA RECOMMENDATION (ORDINATA PER VELOCITY STIMATA):")
        print("-" * 75)
        for rank, site in enumerate(ranked_results, 1):
            print(f"{rank}. {site['facility_name']} ({site['city']}) "
                  f"-> Velocity Predetta: {site['predicted_velocity']} paz/mese")
        print("-" * 75)

    except Exception as e:
        print(f"[ERR]: Errore durante il test di inferenza: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    run_inference_test()