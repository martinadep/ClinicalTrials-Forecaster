import os
import streamlit as st
import pandas as pd

st.set_page_config(layout="wide")

st.title("Clinical Trial Site Recommender")

st.write(
    "Describe a new clinical trial you are planning, and this dashboard returns a "
    "ranked list of sites or regions most likely to recruit it quickly, measured "
    "as **recruitment velocity** (patients enrolled per month)."
)

# ---------- CARICAMENTO DATI REALI ----------
DATA_DIR = "dashboard/data"

@st.cache_data
def load_metadata():
    """Carica i metadati estratti per popolare i selettori della sidebar."""
    conditions = pd.read_csv(os.path.join(DATA_DIR, "conditions.csv"))["condition"].tolist()
    study_types = pd.read_csv(os.path.join(DATA_DIR, "study_types.csv"))["study_type"].tolist()
    phases = pd.read_csv(os.path.join(DATA_DIR, "phases.csv"))["phase"].tolist()
    
    # Nuovi file reali integrati
    # purposes = pd.read_csv(os.path.join(DATA_DIR, "purposes.csv"))["primary_purpose"].tolist()
    sexes = pd.read_csv(os.path.join(DATA_DIR, "sexes.csv"))["sex"].tolist()
    # sponsors = pd.read_csv(os.path.join(DATA_DIR, "sponsors.csv"))["lead_sponsor_class"].tolist()
    
    return conditions, study_types, phases, sexes

try:
    CONDITIONS, STUDY_TYPES, PHASES, SEXES = load_metadata()
except Exception as e:
    st.error(f"Errore nel caricamento dei metadati da {DATA_DIR}. Assicurati di aver eseguito retrieve_data.py.")
    st.stop()


# ---------- FUNZIONE DI PREDIZIONE / FILTRO REALE ----------
def predict_sites(selected_condition):
    """
    Legge il file reale cond_count.csv (o la tabella dei siti se hai le geolocalizzazioni associate)
    Mockiamo la velocity usando il count reale normalizzato o un calcolo sui dati estratti.
    """
    cond_count_path = os.path.join(DATA_DIR, "cond_count.csv")
    if not os.path.exists(cond_count_path):
        return []
    
    df_counts = pd.read_csv(cond_count_path)
    
    # Recuperiamo il valore reale di count per la condizione selezionata
    condition_data = df_counts[df_counts['condition'] == selected_condition]
    
    if condition_data.empty:
        return []
    
    real_count = int(condition_data.iloc[0]['count'])
    
    # Simulazione di siti associati (In produzione qui leggerai la tabella gold/silver dei siti geolocalizzati)
    results = [
        {"Site": "Ospedale San Raffaele", "City": "Milan", "Country": "Italy", "Velocity": round(real_count * 0.05, 1), "lat": 45.5057, "lon": 9.2647},
        {"Site": "Policlinico Gemelli", "City": "Rome", "Country": "Italy", "Velocity": round(real_count * 0.04, 1), "lat": 41.9311, "lon": 12.4255},
        {"Site": "Charité", "City": "Berlin", "Country": "Germany", "Velocity": round(real_count * 0.06, 1), "lat": 52.5263, "lon": 13.3766},
        {"Site": "Massachusetts General", "City": "Boston", "Country": "United States", "Velocity": round(real_count * 0.08, 1), "lat": 42.3626, "lon": -71.0688},
    ]
    return results


# ---------- SIDEBAR: input dell'utente ----------
st.sidebar.header("Trial details")

condition = st.sidebar.selectbox(
    "Medical condition", options=CONDITIONS,
    index=None, placeholder="Type to search a condition...",
)

study_type = st.sidebar.selectbox("Study type", STUDY_TYPES)

phase = st.sidebar.selectbox("Phase", PHASES)

sex = st.sidebar.selectbox("Sex (Eligibility)", SEXES)

# sponsor_class = st.sidebar.selectbox("Lead sponsor class", SPONSORS)

enrollment = st.sidebar.number_input(
    "Target enrollment (number of patients)", min_value=1, value=100,
)

st.sidebar.divider()

# Liste di città e nazioni geografiche
ALL_CITIES = ["Berlin", "Boston", "Milan", "Rome"]
ALL_COUNTRIES = ["Germany", "Italy", "United States"]

selection_mode = st.sidebar.radio(
    "Select candidates by",
    ["City", "Country"],
)

chosen = []
if selection_mode == "City":
    chosen = st.sidebar.multiselect("Candidate cities", ALL_CITIES)
else:
    chosen = st.sidebar.multiselect("Candidate countries", ALL_COUNTRIES)

run = st.sidebar.button("Recommend sites")


# ---------- MAIN AREA: risultati ----------
if run:
    if condition is None or len(chosen) == 0:
        st.warning("Please select a condition and at least one candidate geographical filter in the sidebar.")
    else:
        predictions = predict_sites(condition)
        
        if not predictions:
            st.error("No data available for the selected condition.")
        else:
            ranking = pd.DataFrame(predictions)
            
            # Filtra in base alla selezione geografica dell'utente
            if selection_mode == "City":
                ranking = ranking[ranking["City"].isin(chosen)]
            else:
                ranking = ranking[ranking["Country"].isin(chosen)]
                
            if ranking.empty:
                st.warning("No sites found matching your combined filters.")
            else:
                ranking = ranking.sort_values("Velocity", ascending=False).reset_index(drop=True)

                st.header("Recommended sites")
                st.write(f"Ranked by recruitment velocity metrics for **{condition}**:")

                # Highlight del top recommendation
                best = ranking.iloc[0]
                st.success(
                    f"**Top recommendation: {best['Site']}** "
                    f"({best['City']}, {best['Country']}) — "
                    f"{best['Velocity']} estimated patients/month"
                )

                st.dataframe(
                    ranking[["Site", "City", "Country", "Velocity"]],
                    use_container_width=True,
                )

                st.subheader("Site locations")
                st.map(ranking[["lat", "lon"]])
else:
    st.info("← Fill in the trial details in the sidebar and click **Recommend sites**.")