import os, sys
import streamlit as st
import pandas as pd
import requests

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

ML_API_URL = os.environ.get("ML_API_URL", "http://localhost:8000")

st.set_page_config(layout="wide")

st.title("Clinical Trial Site Recommender")

st.write(
    "Describe a new clinical trial you are planning, and this dashboard returns a "
    "ranked list of sites or regions **predicted by our Spark ML Model** to recruit it quickly, measured "
    "as **recruitment velocity** (patients enrolled per month)."
)

DATA_DIR = "dashboard/data"

@st.cache_data
def load_metadata():
    conditions_df = pd.read_csv(os.path.join(DATA_DIR, "conditions.csv"))
    
    study_types = pd.read_csv(os.path.join(DATA_DIR, "study_types.csv"))["study_type"].tolist()
    phases = pd.read_csv(os.path.join(DATA_DIR, "phases.csv"))["phase"].tolist()
    sexes = pd.read_csv(os.path.join(DATA_DIR, "sexes.csv"))["sex"].tolist()
    countries = pd.read_csv(os.path.join(DATA_DIR, "countries.csv"))["country"].tolist()
    cities = pd.read_csv(os.path.join(DATA_DIR, "cities.csv"))["city"].tolist()

    site_history = pd.read_csv(os.path.join(DATA_DIR, "site_history.csv"))

    return conditions_df, study_types, phases, sexes, countries, cities, site_history

try:
    CONDITIONS_DF, STUDY_TYPES, PHASES, SEXES, ALL_COUNTRIES, ALL_CITIES, SITE_HISTORY_DF = load_metadata()
except Exception as e:
    st.error(f"Error while loading metadata from {DATA_DIR}. Check if they are present.")
    st.stop()


st.sidebar.header("Trial details")

condition_names = CONDITIONS_DF["condition_name"].tolist()
condition_map = dict(zip(CONDITIONS_DF["condition_name"], CONDITIONS_DF["condition_id"]))

chosen_condition_name = st.sidebar.selectbox(
    "Medical condition", options=condition_names,
    index=None, placeholder="Type to search a condition...",
)

condition_id = condition_map.get(chosen_condition_name) if chosen_condition_name else None

study_type = st.sidebar.selectbox("Study type", STUDY_TYPES)
phase = st.sidebar.selectbox("Phase", PHASES)
sex = st.sidebar.selectbox("Sex (Eligibility)", SEXES)

enrollment = st.sidebar.number_input(
    "Target enrollment (number of patients)", min_value=5, value=100,
)

st.sidebar.divider()

selection_mode = st.sidebar.radio(
    "Select candidates by",
    ["Country", "City"],
)

chosen = []
if selection_mode == "City":
    chosen = st.sidebar.multiselect("Candidate cities", ALL_CITIES)
else:
    chosen = st.sidebar.multiselect("Candidate countries", ALL_COUNTRIES)

run = st.sidebar.button("Recommend sites")


if run:
    if condition_id is None or len(chosen) == 0:
        st.warning("Please select a condition and at least one candidate geographical filter in the sidebar.")
    else:
        
        if selection_mode == "City":
            filtered_candidates = SITE_HISTORY_DF[SITE_HISTORY_DF["city"].isin(chosen)]
        else:
            filtered_candidates = SITE_HISTORY_DF[SITE_HISTORY_DF["country"].isin(chosen)]
            
        if filtered_candidates.empty:
            st.warning("No historical sites found matching your geographical filter.")
        else:
            with st.spinner("Spark Engine is generating ML predictions..."):
                trial_params = {
                    "study_type": study_type,
                    "primary_purpose": "TREATMENT", 
                    "phase": phase,
                    "enrollment_count": int(enrollment),
                    "sex": sex,
                    "conditions": [condition_id],
                    "areas": []  
                }
                
                candidates_df = filtered_candidates.rename(columns={
                    "site": "facility_name",
                    "city": "city",
                    "country": "country"
                })
                # NaN (e.g. missing state/zip) isn't JSON-compliant, and pandas'
                # newer string dtype doesn't reliably clear it via .where(), so
                # sanitize per-value after to_dict() instead.
                candidates_list = [
                    {k: (None if pd.isna(v) else v) for k, v in row.items()}
                    for row in candidates_df.to_dict(orient="records")
                ]
                
                try:

                    resp = requests.post(
                        f"{ML_API_URL}/predict",
                        json={"trial_params": trial_params, "candidate_sites": candidates_list},
                        # ml-api's first request after startup pays Spark JVM
                        # boot + Maven jar resolution (~30-40s observed);
                        # subsequent requests reuse the cached session and are fast.
                        timeout=90,
                    )
                    resp.raise_for_status()
                    ranked_results = resp.json()["ranked_results"]

                    if not ranked_results:
                        st.error("No valid candidate sites with a historical background available to rank.")
                    else:
                        output_rows = []
                        for item in ranked_results:
                            site_dict = item["site"]
                            pred_vel = item["predicted_velocity"]
                            output_rows.append({
                                "Site": site_dict.get("facility_name"),
                                "City": site_dict.get("city"),
                                "Country": site_dict.get("country"),
                                "Velocity": round(pred_vel, 4),
                                "lat": site_dict.get("lat"),
                                "lon": site_dict.get("lon")
                            })
                        
                        ranking_df = pd.DataFrame(output_rows)
                        
                        st.header("Recommended sites")
                        best = ranking_df.iloc[0]
                        
                        st.success(
                            f"**Top recommendation: {best['Site']}** ({best['City']}, {best['Country']}) — "
                            f"{best['Velocity']} predicted patients/month"
                        )

                        st.dataframe(
                            ranking_df[["Site", "City", "Country", "Velocity"]],
                            use_container_width=True,
                        )

                        st.subheader("Site locations")
                        st.map(ranking_df[["lat", "lon"]])
                        
                except Exception as ex:
                    st.error(f"Prediction Engine Failure: {ex}")

st.info("← Fill in the trial details in the sidebar and click **Recommend sites**.")