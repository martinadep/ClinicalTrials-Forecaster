import os
import sys
import psycopg2
import psycopg2.extras

# 1. Carica le configurazioni del team
# Saliamo di un livello per importare correttamente 'shared' e 'models' se lo script è in un sotto-folder
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
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
    # Usiamo RealDictCursor così i dati estratti da Postgres diventano dizionari Python
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    try:
        # 2. SIMULA L'ESTRAZIONE GEOGRAFICA
        # Estraiamo anche i campi potenzialmente Nulli (n_trials, avg_velocity, latitude, longitude)
        # per verificare la resilienza della nuova funzione
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

        # 3. SIMULA L'INPUT DI UN NUOVO TRIAL DALLA DASHBOARD
        # MODIFICATO: Rimosso 'healthy_volunteers' coerentemente con la pipeline Spark ML
        new_trial_request = {
            "primary_purpose": "TREATMENT",
            "lead_sponsor_class": "INDUSTRY",
            "sex": "ALL",
            "enrollment_count": 400,
            "num_conditions": 1,
            "duration_months": 12.0
        }

        print(f"[TEST]: Trovati {len(candidate_sites)} siti candidati. Avvio inferenza Spark ML...")

        # 4. INVOCAZIONE DELLA FUNZIONE DI PREDIZIONE AGGIORNATA
        # Il modello adesso gestisce internamente i None inserendo la mediana/media 
        # o assegnando 0.0 di default per i siti privi di storico o coordinate.
        ranked_results = predict_site_rankings(new_trial_request, candidate_sites)

        # 5. STAMPA DEI PRIMI 20 RISULTATI ORDINATI (Quello che vedrà la Dashboard)
        print(f"\n🏆 TOP 20 SITI SU {len(ranked_results)} DISPONIBILI (ORDINATI PER VELOCITY STIMATA):")
        print("-" * 75)
        
        # MODIFICATO: aggiunto lo slicing [:20] per limitare la stampa ai primi 20 siti
        for rank, site in enumerate(ranked_results[:20], 1):
            # Gestione formattazione stringa sicura in caso di fallback su Unknown
            facility = site.get('facility_name', 'Struttura Sconosciuta')
            city = site.get('city', 'Città Sconosciuta')
            velocity = site.get('predicted_velocity', 0.0)
            
            # Formattato l'output numerico a 2 decimali (.2f) per pulizia visiva
            print(f"{rank:02d}. {facility} ({city}) "
                  f"-> Velocity Predetta: {velocity:.2f} paz/mese")
        print("-" * 75)

    except Exception as e:
        print(f"[ERR]: Errore durante il test di inferenza: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    run_inference_test()