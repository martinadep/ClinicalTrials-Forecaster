import streamlit as st
import pandas as pd
import random

st.title("Clinical Trial Site Recommender")
st.write("Fill in your trial information in the sidebar to discover quicker participant sourcing locations.")

# Provisional list of conditions (placeholder).
# Will later be replaced by the real MeSH terms extracted from the dataset.
CONDITIONS = [
    "Diabetes", "Hypertension", "Breast cancer", "Lung cancer", "Asthma",
    "Depression", "Alzheimer disease", "Obesity", "Stroke",
    "Rheumatoid arthritis", "Parkinson disease", "Heart failure",
    "Chronic kidney disease", "COVID-19", "Multiple sclerosis",
]

# Provisional sites with real coordinates (placeholder).
# Will later be replaced by real sites + geoPoints from the gold data.
FAKE_SITES = {
    "Italy": [
        {"site": "Ospedale San Raffaele (Milan)", "lat": 45.5057, "lon": 9.2647},
        {"site": "Policlinico Gemelli (Rome)", "lat": 41.9311, "lon": 12.4255},
    ],
    "Germany": [
        {"site": "Charité (Berlin)", "lat": 52.5263, "lon": 13.3766},
        {"site": "Klinikum (Munich)", "lat": 48.1100, "lon": 11.4700},
    ],
    "France": [
        {"site": "Hôpital Pitié-Salpêtrière (Paris)", "lat": 48.8389, "lon": 2.3640},
    ],
    "Spain": [
        {"site": "Hospital Clínic (Barcelona)", "lat": 41.3890, "lon": 2.1500},
    ],
    "United States": [
        {"site": "Massachusetts General (Boston)", "lat": 42.3626, "lon": -71.0688},
    ],
}


# Placeholder prediction function.
# Will later be replaced by a real call to the trained model / inference API.
def predict_sites(condition, study_type, phase, enrollment, regions):
    results = []
    for region in regions:
        for site in FAKE_SITES.get(region, []):
            results.append({
                "Site": site["site"],
                "Region": region,
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

regions = st.sidebar.multiselect(
    "Candidate regions / countries",
    list(FAKE_SITES.keys()),
)

run = st.sidebar.button("Recommend sites")

# ---------- MAIN AREA: results ----------
if run:
    if condition is None or len(regions) == 0:
        st.warning("Please select a condition and at least one region in the sidebar.")
    else:
        predictions = predict_sites(condition, study_type, phase, enrollment, regions)
        ranking = pd.DataFrame(predictions)
        ranking = ranking.sort_values(
            "Predicted velocity (patients/month)", ascending=False
        ).reset_index(drop=True)

        st.header("Recommended sites")
        st.write(f"Ranked by predicted recruitment velocity for **{condition}**:")
        st.dataframe(
            ranking[["Site", "Region", "Predicted velocity (patients/month)"]],
            use_container_width=True,
        )

        st.subheader("Site locations")
        st.map(ranking[["lat", "lon"]])
else:
    st.info("👈 Fill in the trial details in the sidebar and click **Recommend sites**.")
