import os
import sys
import random

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

try:
    import psycopg2
    from shared.config import load_dotenv
    from shared.db import build_dsn_from_env
except ImportError as e:
    raise RuntimeError(f"Error importing modules: {e}")

load_dotenv()

CONDITIONS = ["diabetes", "oncology", "cardiology", "neurology", "dermatology"]
STUDY_TYPES = ["INTERVENTIONAL", "OBSERVATIONAL"]
PURPOSES = ["TREATMENT", "PREVENTION", "DIAGNOSTIC", "BASIC_SCIENCE"]
SPONSOR_CLASSES = ["INDUSTRY", "NIH", "OTHER", "FED"]
SEXES = ["ALL", "FEMALE", "MALE"]
PHASES = ["PHASE1", "PHASE2", "PHASE3", "PHASE4", "NA"]

MOCK_SITES = [
    {"facility_name": "Ospedale Maggiore", "city": "Milan", "state": "Lombardy", "zip": "20122", "country": "Italy", "lat": 45.4642, "lon": 9.1900},
    {"facility_name": "Policlinico Gemelli", "city": "Rome", "state": "Lazio", "zip": "00168", "country": "Italy", "lat": 41.9304, "lon": 12.4284},
    {"facility_name": "Charité", "city": "Berlin", "state": "Berlin", "zip": "10117", "country": "Germany", "lat": 52.5250, "lon": 13.3777},
    {"facility_name": "Hôpital Pitié-Salpêtrière", "city": "Paris", "state": "Île-de-France", "zip": "75013", "country": "France", "lat": 48.8394, "lon": 2.3653},
    {"facility_name": "UCHealth Anschutz", "city": "Aurora", "state": "Colorado", "zip": "80045", "country": "United States", "lat": 39.7294, "lon": -104.8319},
    {"facility_name": "Mayo Clinic", "city": "Rochester", "state": "Minnesota", "zip": "55905", "country": "United States", "lat": 44.0224, "lon": -92.4668},
]

def generate_and_insert_mock_data():
    dsn = build_dsn_from_env()
    if not dsn:
        dsn = os.getenv("DATABASE_URL")
        
    if not dsn:
        raise ValueError("[ERR]: Cannot compose DSN. Check the .env file")
        
    print(f"[INFO]: Connessione a Postgres in corso...")
    conn = psycopg2.connect(dsn)
    cursor = conn.cursor()
    
    try:
        print("Cleaning old mock data...")
        cursor.execute("TRUNCATE TABLE gold.trial_features CASCADE;")
        cursor.execute("TRUNCATE TABLE gold.site_history CASCADE;")
        conn.commit()

        print("Generating gold.site_history and gold.site_conditions_history...")
        site_records = []
        
        for site in MOCK_SITES:
            n_trials = random.randint(5, 50)
            avg_velocity = round(random.uniform(2.0, 35.0), 2)
            last_year = random.randint(2022, 2026)
            
            cursor.execute("""
                INSERT INTO gold.site_history 
                (country, city, state, zip, facility_name, latitude, longitude, n_trials, avg_velocity, last_year)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, (site["country"], site["city"], site["state"], site["zip"], site["facility_name"], 
                  site["lat"], site["lon"], n_trials, avg_velocity, last_year))
            
            site_records.append({**site, "n_trials": n_trials, "avg_velocity": avg_velocity})
            
            chosen_conditions = random.sample(CONDITIONS, random.randint(2, 4))
            remaining_trials = n_trials
            
            for i, cond in enumerate(chosen_conditions):
                if i == len(chosen_conditions) - 1:
                    cond_trials = remaining_trials
                else:
                    cond_trials = random.randint(1, max(1, remaining_trials - (len(chosen_conditions) - i)))
                    remaining_trials -= cond_trials
                
                if cond_trials > 0:
                    cursor.execute("""
                        INSERT INTO gold.site_conditions_history 
                        (country, city, zip, condition_name, n_trials_for_condition)
                        VALUES (%s, %s, %s, %s, %s);
                    """, (site["country"], site["city"], site["zip"], cond, cond_trials))

        print("Generating gold.trial_features...")
        for i in range(1, 1001):
            nct_id = f"NCT{str(i).zfill(8)}"
            study_type = random.choice(STUDY_TYPES)
            primary_purpose = random.choice(PURPOSES)
            lead_sponsor_class = random.choice(SPONSOR_CLASSES)
            sex = random.choice(SEXES)
            healthy_volunteers = random.choice([True, False])
            phase = "NA" if study_type == "OBSERVATIONAL" else random.choice(PHASES[:-1])
            
            enrollment_count = random.randint(20, 500)
            duration_months = round(random.uniform(6.0, 36.0), 1)
            num_conditions = random.randint(1, 3)
            
            k_sites = random.randint(1, 4)
            participating_sites = random.sample(site_records, k_sites)
            
            avg_site_exp = round(sum(s["n_trials"] for s in participating_sites) / k_sites, 2)
            avg_site_vel = round(sum(s["avg_velocity"] for s in participating_sites) / k_sites, 2)
            
            base_velocity = (enrollment_count / duration_months)
            target_velocity = round(base_velocity * 0.7 + avg_site_vel * 0.3 + random.uniform(-2.0, 2.0), 2)
            target_velocity = max(0.1, target_velocity)
            
            cursor.execute("""
                INSERT INTO gold.trial_features (
                    nct_id, study_type, primary_purpose, lead_sponsor_class, sex, 
                    healthy_volunteers, phase, enrollment_count, n_sites, 
                    num_conditions, duration_months, avg_site_exp, avg_site_vel, TARGET_velocity
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, (nct_id, study_type, primary_purpose, lead_sponsor_class, sex,
                  healthy_volunteers, phase, enrollment_count, k_sites,
                  num_conditions, duration_months, avg_site_exp, avg_site_vel, target_velocity))

        conn.commit()
        print("Mock data inserted successfully!")
    except Exception as e:
        conn.rollback()
        print(f"[ERR]: Error occurred while writing data: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    generate_and_insert_mock_data()