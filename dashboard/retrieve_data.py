import os
import psycopg2
import pandas as pd
from shared.config import load_dotenv
from shared.db import build_dsn_from_env

def main():
    load_dotenv()
    dsn = os.getenv("DATABASE_URL") or build_dsn_from_env()
    conn = psycopg2.connect(dsn)
    
    try:
        print("Data Extraction...")
        os.makedirs("dashboard/data", exist_ok=True)


        query_mesh_counts = """
            SELECT 
                dm.mesh_condition_name AS condition,
                SUM(sch.n_trials_for_condition)::INTEGER AS count
            FROM gold.site_conditions_history sch
            INNER JOIN gold.dim_mesh_conditions dm 
               ON sch.mesh_condition_id = dm.mesh_condition_id
            GROUP BY dm.mesh_condition_name
            ORDER BY count DESC;
        """
        conditions_count = pd.read_sql_query(query_mesh_counts, conn)
        conditions_count = conditions_count[conditions_count['count'] >= 3]
        conditions = conditions_count['condition'].unique()
        pd.DataFrame({'condition': sorted(conditions)}).to_csv('dashboard/data/conditions.csv', index=False)
        conditions_count.to_csv('dashboard/data/cond_count.csv', index=False)
        

        query_params_gold = """
            SELECT DISTINCT study_type, primary_purpose, lead_sponsor_class, sex, phase
            FROM gold.trial_features
        """
        df_params_gold = pd.read_sql_query(query_params_gold, conn)
        study_types = df_params_gold['study_type'].unique()
        purposes = df_params_gold['primary_purpose'].unique()
        sponsors = df_params_gold['lead_sponsor_class'].unique()
        sexes = df_params_gold['sex'].unique()
        phases = df_params_gold['phase'].unique()        
        pd.DataFrame({'study_type': sorted(study_types)}).to_csv('dashboard/data/study_types.csv', index=False)
        pd.DataFrame({'primary_purpose': sorted(purposes)}).to_csv('dashboard/data/purposes.csv', index=False)
        pd.DataFrame({'lead_sponsor_class': sorted(sponsors)}).to_csv('dashboard/data/sponsors.csv', index=False)
        pd.DataFrame({'sex': sorted(sexes)}).to_csv('dashboard/data/sexes.csv', index=False)
        pd.DataFrame({'phase': sorted(phases)}).to_csv('dashboard/data/phases.csv', index=False)
          

        query_geo = """
                SELECT DISTINCT country, city 
                FROM gold.site_history
                WHERE country IS NOT NULL AND city IS NOT NULL;
            """
        df_geo = pd.read_sql_query(query_geo, conn)
        countries = df_geo['country'].dropna().unique()
        cities = df_geo['city'].dropna().unique()
        pd.DataFrame({'country': sorted(countries)}).to_csv('dashboard/data/countries.csv', index=False)
        pd.DataFrame({'city': sorted(cities)}).to_csv('dashboard/data/cities.csv', index=False)
        

        query_sites = "SELECT country, city, state, zip, facility_name AS site, n_trials, avg_velocity, latitude AS lat, longitude AS lon FROM gold.site_history;"
        df_sites = pd.read_sql_query(query_sites, conn)
        df_sites.to_csv('dashboard/data/site_history.csv', index=False)
        
                                    
        print("Data Extraction Completed Successfully!")

    except Exception as e:
        print(f"Error during extraction: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()