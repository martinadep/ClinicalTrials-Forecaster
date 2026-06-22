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

        query_silver = """
            SELECT nct_id, array_to_json(conditions) as conditions 
            FROM silver.trial_sites;
        """

        query_gold = """
            SELECT DISTINCT study_type, primary_purpose, lead_sponsor_class, sex, phase
            FROM gold.trial_features
        """
        df_gold = pd.read_sql_query(query_gold, conn)
        study_types = df_gold['study_type'].unique()
        purposes = df_gold['primary_purpose'].unique()
        sponsors = df_gold['lead_sponsor_class'].unique()
        sexes = df_gold['sex'].unique()
        phases = df_gold['phase'].unique()

        df_silver = pd.read_sql_query(query_silver, conn)
        
        df_silver['conditions'] = df_silver['conditions'].apply(lambda x: x if isinstance(x, list) else [])

        exploded_conditions = df_silver['conditions'].explode()
        conditions_count = exploded_conditions.value_counts().reset_index()
        conditions_count.columns = ['condition', 'count']
        conditions_count = conditions_count[conditions_count['count'] >= 40]
        conditions = conditions_count['condition'].unique()  
        

        os.makedirs("dashboard/data", exist_ok=True) 
        
        pd.DataFrame({'condition': sorted(conditions)}).to_csv('dashboard/data/conditions.csv', index=False)
        conditions_count.to_csv('dashboard/data/cond_count.csv', index=False)
        pd.DataFrame({'study_type': sorted(study_types)}).to_csv('dashboard/data/study_types.csv', index=False)
        pd.DataFrame({'primary_purpose': sorted(purposes)}).to_csv('dashboard/data/purposes.csv', index=False)
        pd.DataFrame({'lead_sponsor_class': sorted(sponsors)}).to_csv('dashboard/data/sponsors.csv', index=False)
        pd.DataFrame({'sex': sorted(sexes)}).to_csv('dashboard/data/sexes.csv', index=False)
        pd.DataFrame({'phase': sorted(phases)}).to_csv('dashboard/data/phases.csv', index=False)

        print("Data Extraction Completed Successfully!")

    except Exception as e:
        print(f"Error during extraction: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()