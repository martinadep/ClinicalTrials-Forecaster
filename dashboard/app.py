import streamlit as st
import pandas as pd
import random                           #tb removed

st.title("Clinical Trial Site Recommender")

st.write(
    "Describe a new clinical trial you are planning, and this dashboard returns a "
    "ranked list of sites or regions most likely to recruit it quickly, measured "
    "as **recruitment velocity** (patients enrolled per month)."
)

# Provisional list of conditions (placeholder).
# Will later be replaced by the real MeSH terms extracted from the dataset.
CONDITIONS = [
    "Diabetes", "Hypertension", "Breast cancer", "Lung cancer", "Asthma",
    "Depression", "Alzheimer disease", "Obesity", "Stroke",
    "Rheumatoid arthritis", "Parkinson disease", "Heart failure",
    "Chronic kidney disease", "COVID-19", "Multiple sclerosis",
]

# Provisional sites with real coordinates (placeholder).
# Each site has: facility name, city, country, lat, lon.
# Will later be replaced by real sites + geoPoints from the gold data.
FAKE_SITES = [
    {"facility": "Ospedale San Raffaele", "city": "Milan", "country": "Italy", "lat": 45.5057, "lon": 9.2647},
    {"facility": "Policlinico Gemelli", "city": "Rome", "country": "Italy", "lat": 41.9311, "lon": 12.4255},
    {"facility": "Charité", "city": "Berlin", "country": "Germany", "lat": 52.5263, "lon": 13.3766},
    {"facility": "Klinikum", "city": "Munich", "country": "Germany", "lat": 48.1100, "lon": 11.4700},
    {"facility": "Hôpital Pitié-Salpêtrière", "city": "Paris", "country": "France", "lat": 48.8389, "lon": 2.3640},
    {"facility": "Hospital Clínic", "city": "Barcelona", "country": "Spain", "lat": 41.3890, "lon": 2.1500},
    {"facility": "Massachusetts General", "city": "Boston", "country": "United States", "lat": 42.3626, "lon": -71.0688},
]

# Helper lists for the selection menus, built from the data above.
ALL_CITIES = sorted({s["city"] for s in FAKE_SITES})
ALL_COUNTRIES = sorted({s["country"] for s in FAKE_SITES})


# Placeholder prediction function.
# Will later be replaced by a real call to the trained model / inference API.
def predict_sites(condition, study_type, phase, enrollment, selected_sites):
    results = []
    for site in selected_sites:
        results.append({
            "Site": site["facility"],
            "City": site["city"],
            "Country": site["country"],
            "Predicted velocity (patients/month)": round(random.uniform(5, 30), 1),
            "lat": site["lat"],
            "lon": site["lon"],
        })
    return results


# ---------- SIDEBAR: user inputs ----------
st.sidebar.header("Trial details")

condition = st.sidebar.selectbox(
    "Medical condition", options=CONDITIONS,
    index=None, placeholder="Type to search a condition...",
)

study_type = st.sidebar.selectbox("Study type", ["Interventional", "Observational"])

phase = st.sidebar.selectbox(
    "Phase", ["Phase 1", "Phase 2", "Phase 3", "Phase 4", "Not applicable"],
)

enrollment = st.sidebar.number_input(
    "Target enrollment (number of patients)", min_value=1, value=100,
)

st.sidebar.divider()

# Choose how to select candidates: by city (primary) or by country (extra). 
selection_mode = st.sidebar.radio(
    "Select candidates by",
    ["City", "Country"],
)

if selection_mode == "City":
    chosen = st.sidebar.multiselect("Candidate cities", ALL_CITIES)
    selected_sites = [s for s in FAKE_SITES if s["city"] in chosen]
else:
    chosen = st.sidebar.multiselect("Candidate countries", ALL_COUNTRIES)
    selected_sites = [s for s in FAKE_SITES if s["country"] in chosen]

run = st.sidebar.button("Recommend sites")

# ---------- MAIN AREA: results ----------
if run:
    if condition is None or len(selected_sites) == 0:
        st.warning("Please select a condition and at least one candidate in the sidebar.")
    else:
        predictions = predict_sites(condition, study_type, phase, enrollment, selected_sites)
        ranking = pd.DataFrame(predictions)
        ranking = ranking.sort_values(
            "Predicted velocity (patients/month)", ascending=False
        ).reset_index(drop=True)

        st.header("Recommended sites")
        st.write(f"Ranked by predicted recruitment velocity for **{condition}**:")

        # Highlight the top recommendation (row 0, the best one)
        best = ranking.iloc[0]
        st.success(
            f"**Top recommendation: {best['Site']}** "
            f"({best['City']}, {best['Country']}) — "
            f"{best['Predicted velocity (patients/month)']} patients/month"
        )

        st.dataframe(
            ranking[["Site", "City", "Country", "Predicted velocity (patients/month)"]],
            use_container_width=True,
        )

        st.subheader("Site locations")
        st.map(ranking[["lat", "lon"]])
else:
    st.info("← Fill in the trial details in the sidebar and click **Recommend sites**.")
