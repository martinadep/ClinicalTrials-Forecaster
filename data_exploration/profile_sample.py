"""
=====================================================================
 profile_sample.py
 Script di profilazione del campione di ClinicalTrials.gov API v2
 Progetto: Clinical Trial Site Selection & Recruitment Forecasting
=====================================================================

COSA FA QUESTO SCRIPT
---------------------
1. Scarica un campione di trial dall'API v2 di ClinicalTrials.gov
   (default: 5000 trial, configurabile).
2. Per ciascun trial, estrae i campi descritti nel Data Dictionary.
3. Calcola statistiche di:
     - % di valori mancanti (missing rate)
     - distribuzione dei valori delle variabili categoriche (enum)
     - presenza/assenza di liste annidate (locations, conditions, ecc.)
     - coverage dei termini MeSH
     - distribuzione di outlier per enrollmentInfo.count
4. Produce due file di output:
     - sample_profile_report.txt   (report leggibile da incollare nel Word)
     - sample_profile_data.csv     (dati grezzi per analisi successive)

COME ESEGUIRLO
--------------
1. Apri questo file in Visual Studio Code.
2. Premi F5 (oppure clicca sulla freccia "Play" in alto a destra).
3. Aspetta circa 15-30 minuti (scarica i dati dall'API).
4. Apri il file sample_profile_report.txt per vedere i risultati.

REQUISITI
---------
- Python 3.9 o superiore
- Pacchetto 'requests' (lo script ti dice come installarlo se manca)
"""

# =====================================================================
# IMPORT - Librerie standard, nessuna installazione speciale serve
# tranne 'requests' (vedi sotto)
# =====================================================================

import sys
import time
import json
import csv
import os
from datetime import datetime
from collections import Counter, defaultdict

# requests è l'unica libreria esterna che ci serve
try:
    import requests
except ImportError:
    print("\n[ERRORE] Manca la libreria 'requests'.")
    print("Per installarla, apri il terminale (in VS Code: menu Terminal -> New Terminal)")
    print("e digita questo comando, poi premi Invio:\n")
    print("    pip install requests\n")
    print("Dopo l'installazione, esegui di nuovo lo script.")
    sys.exit(1)


# =====================================================================
# CONFIGURAZIONE - puoi modificare questi valori se vuoi
# =====================================================================

SAMPLE_SIZE = 5000           # numero di trial da scaricare
PAGE_SIZE = 1000             # 1000 è il massimo consentito dall'API
SLEEP_BETWEEN_REQUESTS = 0.2 # secondi di pausa tra chiamate (5 req/s)
MAX_RETRIES = 5              # tentativi su errore di rete
OUTPUT_DIR = "./data_exploration"             # cartella in cui salvare i file output

API_BASE_URL = "https://clinicaltrials.gov/api/v2/studies"

# Campi che vogliamo estrarre dall'API (riduce il payload del ~40%)
# I nomi seguono la sintassi dell'API v2 (CamelCase, non json-path).
FIELDS_TO_FETCH = [
    "NCTId",
    "BriefTitle",
    "OfficialTitle",
    "OverallStatus",
    "StatusVerifiedDate",
    "WhyStopped",
    "StartDate", "StartDateType",
    "PrimaryCompletionDate", "PrimaryCompletionDateType",
    "CompletionDate", "CompletionDateType",
    "StudyFirstPostDate",
    "LastUpdatePostDate",
    "LeadSponsorName", "LeadSponsorClass",
    "CollaboratorName", "CollaboratorClass",
    "Condition", "Keyword",
    "ConditionMeshTerm",
    "StudyType", "Phase",
    "DesignAllocation", "DesignInterventionModel", "DesignMasking",
    "EnrollmentCount", "EnrollmentType",
    "EligibilityCriteria", "MinimumAge", "MaximumAge",
    "Sex", "HealthyVolunteers", "StdAge",
    "InterventionType", "InterventionName",
    "OversightHasDMC", "IsFDARegulatedDrug", "IsFDARegulatedDevice",
    "LocationFacility", "LocationCity", "LocationState",
    "LocationCountry", "LocationStatus",
    "LocationGeoPoint",
]


# =====================================================================
# FUNZIONE 1 - Scarica un batch di trial dall'API
# =====================================================================

def fetch_page(page_token=None):
    """
    Scarica una pagina di trial dall'API.
    Restituisce: (lista di trial, prossimo page_token oppure None)
    """
    params = {
        "pageSize": PAGE_SIZE,
        "format": "json",
        "fields": "|".join(FIELDS_TO_FETCH),
    }
    if page_token:
        params["pageToken"] = page_token

    # Retry con backoff esponenziale in caso di errore di rete o 429
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(API_BASE_URL, params=params, timeout=60)
            if response.status_code == 200:
                data = response.json()
                return data.get("studies", []), data.get("nextPageToken")
            elif response.status_code == 429:
                # Rate limit, aspettiamo di più
                wait = 2 ** attempt
                print(f"  [WARN] Rate limited (429). Attendo {wait}s...")
                time.sleep(wait)
            else:
                print(f"  [WARN] HTTP {response.status_code}. Retry...")
                time.sleep(2 ** attempt)
        except requests.exceptions.RequestException as e:
            print(f"  [WARN] Errore di rete: {e}. Retry tra {2**attempt}s...")
            time.sleep(2 ** attempt)

    print("  [ERROR] Impossibile completare la richiesta dopo i retry.")
    return [], None


# =====================================================================
# FUNZIONE 2 - Estrae un singolo campo da un trial gestendo i moduli annidati
# =====================================================================

def safe_get(study, *path, default=None):
    """
    Naviga in modo sicuro il JSON annidato.
    Esempio: safe_get(study, 'protocolSection', 'statusModule', 'overallStatus')
    """
    current = study
    for key in path:
        if not isinstance(current, dict):
            return default
        current = current.get(key)
        if current is None:
            return default
    return current


# =====================================================================
# FUNZIONE 3 - Estrae tutte le variabili rilevanti da un trial
# =====================================================================

def extract_features(study):
    """
    Da un trial JSON estrae un dict piatto con le variabili che vogliamo profilare.
    Le chiavi del dict corrispondono ai nomi nel Data Dictionary.
    """
    protocol = safe_get(study, "protocolSection", default={}) or {}
    derived = safe_get(study, "derivedSection", default={}) or {}

    ident = protocol.get("identificationModule", {}) or {}
    status = protocol.get("statusModule", {}) or {}
    sponsor = protocol.get("sponsorCollaboratorsModule", {}) or {}
    conds = protocol.get("conditionsModule", {}) or {}
    design = protocol.get("designModule", {}) or {}
    elig = protocol.get("eligibilityModule", {}) or {}
    arms = protocol.get("armsInterventionsModule", {}) or {}
    oversight = protocol.get("oversightModule", {}) or {}
    locs = protocol.get("contactsLocationsModule", {}) or {}

    cond_mesh = safe_get(derived, "conditionBrowseModule", "meshes", default=[]) or []

    # Lista delle locations (può essere vuota)
    locations_list = locs.get("locations", []) or []

    # Analizziamo lo status sito-level: per ciascun sito vediamo se c'è 'status'
    sites_with_status = sum(1 for l in locations_list if l.get("status"))
    sites_with_geopoint = sum(
        1 for l in locations_list
        if l.get("geoPoint") and l["geoPoint"].get("lat") is not None
    )

    # Collaboratori
    collaborators = sponsor.get("collaborators", []) or []

    # Interventi
    interventions = arms.get("interventions", []) or []

    return {
        # Identification
        "nctId": ident.get("nctId"),
        "briefTitle": ident.get("briefTitle"),
        "officialTitle": ident.get("officialTitle"),

        # Status
        "overallStatus": status.get("overallStatus"),
        "statusVerifiedDate": status.get("statusVerifiedDate"),
        "whyStopped": status.get("whyStopped"),
        "startDate": safe_get(status, "startDateStruct", "date"),
        "startDateType": safe_get(status, "startDateStruct", "type"),
        "primaryCompletionDate": safe_get(status, "primaryCompletionDateStruct", "date"),
        "primaryCompletionDateType": safe_get(status, "primaryCompletionDateStruct", "type"),
        "completionDate": safe_get(status, "completionDateStruct", "date"),
        "completionDateType": safe_get(status, "completionDateStruct", "type"),
        "studyFirstPostDate": safe_get(status, "studyFirstPostDateStruct", "date"),
        "lastUpdatePostDate": safe_get(status, "lastUpdatePostDateStruct", "date"),

        # Sponsor / Collaborators
        "leadSponsorName": safe_get(sponsor, "leadSponsor", "name"),
        "leadSponsorClass": safe_get(sponsor, "leadSponsor", "class"),
        "n_collaborators": len(collaborators),

        # Conditions
        "n_conditions": len(conds.get("conditions", []) or []),
        "n_keywords": len(conds.get("keywords", []) or []),
        "n_mesh_terms": len(cond_mesh),
        "has_mesh": len(cond_mesh) > 0,

        # Design
        "studyType": design.get("studyType"),
        "phases": design.get("phases", []),
        "allocation": safe_get(design, "designInfo", "allocation"),
        "interventionModel": safe_get(design, "designInfo", "interventionModel"),
        "masking": safe_get(design, "designInfo", "maskingInfo", "masking"),
        "enrollmentCount": safe_get(design, "enrollmentInfo", "count"),
        "enrollmentType": safe_get(design, "enrollmentInfo", "type"),

        # Eligibility
        "eligibilityCriteria_len": len(elig.get("eligibilityCriteria", "") or ""),
        "minimumAge": elig.get("minimumAge"),
        "maximumAge": elig.get("maximumAge"),
        "sex": elig.get("sex"),
        "healthyVolunteers": elig.get("healthyVolunteers"),
        "stdAges": elig.get("stdAges", []),

        # Interventions
        "n_interventions": len(interventions),
        "intervention_types": [i.get("type") for i in interventions if i.get("type")],

        # Oversight
        "oversightHasDmc": oversight.get("oversightHasDmc"),
        "isFdaRegulatedDrug": oversight.get("isFdaRegulatedDrug"),
        "isFdaRegulatedDevice": oversight.get("isFdaRegulatedDevice"),

        # Locations
        "n_sites": len(locations_list),
        "n_countries": len(set(
            l.get("country") for l in locations_list if l.get("country")
        )),
        "sites_with_status": sites_with_status,
        "sites_with_geopoint": sites_with_geopoint,
    }


# =====================================================================
# FUNZIONE 4 - Analizza il campione e produce le statistiche
# =====================================================================

def analyze_sample(records):
    """
    Calcola le statistiche di missing/coverage/distribuzione sul campione.
    Restituisce un dict di metriche pronte per il report.
    """
    n = len(records)
    if n == 0:
        return {}

    def pct(num, tot=n):
        return f"{(num / tot * 100):.1f}%" if tot > 0 else "n/a"

    # Missing rate per campo
    missing = {}
    fields_to_check = [
        "overallStatus", "statusVerifiedDate", "whyStopped",
        "startDate", "primaryCompletionDate", "completionDate",
        "leadSponsorName", "leadSponsorClass",
        "studyType", "allocation", "interventionModel", "masking",
        "enrollmentCount", "enrollmentType",
        "eligibilityCriteria_len", "minimumAge", "maximumAge",
        "sex", "healthyVolunteers",
        "oversightHasDmc", "isFdaRegulatedDrug", "isFdaRegulatedDevice",
    ]
    for f in fields_to_check:
        miss = sum(1 for r in records if r.get(f) is None or r.get(f) == "")
        missing[f] = (miss, pct(miss))

    # Distribuzione overallStatus
    status_dist = Counter(r.get("overallStatus") for r in records)

    # Distribuzione ACTUAL vs ESTIMATED
    start_type = Counter(r.get("startDateType") for r in records)
    enroll_type = Counter(r.get("enrollmentType") for r in records)

    # Distribuzione leadSponsorClass
    sponsor_class = Counter(r.get("leadSponsorClass") for r in records)

    # Distribuzione studyType
    study_type = Counter(r.get("studyType") for r in records)

    # Distribuzione sex
    sex_dist = Counter(r.get("sex") for r in records)

    # Coverage MeSH
    mesh_present = sum(1 for r in records if r.get("has_mesh"))

    # Coverage geopoint a livello sito
    total_sites = sum(r.get("n_sites", 0) for r in records)
    total_sites_with_status = sum(r.get("sites_with_status", 0) for r in records)
    total_sites_with_geo = sum(r.get("sites_with_geopoint", 0) for r in records)

    # Outlier enrollment
    enrolls = [r.get("enrollmentCount") for r in records if isinstance(r.get("enrollmentCount"), int)]
    enrolls.sort()
    if enrolls:
        p50 = enrolls[len(enrolls)//2]
        p90 = enrolls[int(len(enrolls)*0.90)]
        p99 = enrolls[int(len(enrolls)*0.99)]
        p_max = enrolls[-1]
        big_outliers = sum(1 for e in enrolls if e > 10000)
    else:
        p50 = p90 = p99 = p_max = big_outliers = 0

    # Distribuzione n_sites
    sites_per_trial = [r.get("n_sites", 0) for r in records]
    avg_sites = sum(sites_per_trial) / len(sites_per_trial) if sites_per_trial else 0

    # whyStopped solo per trial TERMINATED/WITHDRAWN/SUSPENDED
    stopped_statuses = {"TERMINATED", "WITHDRAWN", "SUSPENDED"}
    stopped_trials = [r for r in records if r.get("overallStatus") in stopped_statuses]
    stopped_with_reason = sum(1 for r in stopped_trials if r.get("whyStopped"))

    return {
        "n_records": n,
        "missing": missing,
        "status_dist": status_dist,
        "start_type_dist": start_type,
        "enroll_type_dist": enroll_type,
        "sponsor_class_dist": sponsor_class,
        "study_type_dist": study_type,
        "sex_dist": sex_dist,
        "mesh_coverage_pct": pct(mesh_present),
        "total_sites": total_sites,
        "site_status_coverage_pct": pct(total_sites_with_status, total_sites),
        "site_geo_coverage_pct": pct(total_sites_with_geo, total_sites),
        "enrollment_p50": p50,
        "enrollment_p90": p90,
        "enrollment_p99": p99,
        "enrollment_max": p_max,
        "enrollment_big_outliers": big_outliers,
        "enrollment_outliers_pct": pct(big_outliers, len(enrolls)),
        "avg_sites_per_trial": f"{avg_sites:.1f}",
        "stopped_trials": len(stopped_trials),
        "stopped_with_reason": stopped_with_reason,
        "stopped_with_reason_pct": pct(stopped_with_reason, len(stopped_trials)) if stopped_trials else "n/a",
    }


# =====================================================================
# FUNZIONE 5 - Scrive il report leggibile
# =====================================================================

def write_report(stats, filepath):
    """Scrive un report di testo leggibile, pronto da incollare nel documento."""
    lines = []
    lines.append("=" * 70)
    lines.append(" PROFILAZIONE EMPIRICA DEL CAMPIONE — ClinicalTrials.gov API v2")
    lines.append(f" Generato: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append(f" Dimensione campione: {stats['n_records']} trial")
    lines.append("=" * 70)
    lines.append("")

    lines.append("SEZIONE 8.4 — Tabella di profilazione (valori empirici)")
    lines.append("-" * 70)
    lines.append("")
    lines.append("Variabile                          | % missing | Note")
    lines.append("-" * 70)
    lines.append(f"enrollmentInfo.count               | {stats['missing']['enrollmentCount'][1]:9} | {stats['enrollment_outliers_pct']} sopra 10.000 pazienti (outlier)")
    lines.append(f"locations[].geoPoint (sito-level)  | {(100 - float(stats['site_geo_coverage_pct'].rstrip('%'))):.1f}%     | Coverage {stats['site_geo_coverage_pct']}")
    lines.append(f"conditionBrowseModule.meshes[]     | {(100 - float(stats['mesh_coverage_pct'].rstrip('%'))):.1f}%     | Coverage {stats['mesh_coverage_pct']}")
    lines.append(f"overallStatus = UNKNOWN            | n/a       | {stats['status_dist'].get('UNKNOWN', 0)} record ({stats['status_dist'].get('UNKNOWN', 0)*100/stats['n_records']:.1f}%)")
    lines.append(f"startDateStruct.type = ACTUAL      | n/a       | {stats['start_type_dist'].get('ACTUAL', 0)*100/stats['n_records']:.1f}% del totale")
    lines.append(f"enrollmentInfo.type = ACTUAL       | n/a       | {stats['enroll_type_dist'].get('ACTUAL', 0)*100/stats['n_records']:.1f}% del totale")
    lines.append(f"locations[].status presente        | n/a       | Coverage {stats['site_status_coverage_pct']}")
    lines.append(f"whyStopped (per TERMINATED)        | n/a       | Coverage {stats['stopped_with_reason_pct']}")
    lines.append("")

    lines.append("=" * 70)
    lines.append("DISTRIBUZIONE overallStatus")
    lines.append("-" * 70)
    for status, count in stats['status_dist'].most_common():
        lines.append(f"  {str(status):30} {count:6} ({count*100/stats['n_records']:.1f}%)")
    lines.append("")

    lines.append("DISTRIBUZIONE leadSponsor.class")
    lines.append("-" * 70)
    for cls, count in stats['sponsor_class_dist'].most_common():
        lines.append(f"  {str(cls):30} {count:6} ({count*100/stats['n_records']:.1f}%)")
    lines.append("")

    lines.append("DISTRIBUZIONE studyType")
    lines.append("-" * 70)
    for st, count in stats['study_type_dist'].most_common():
        lines.append(f"  {str(st):30} {count:6} ({count*100/stats['n_records']:.1f}%)")
    lines.append("")

    lines.append("DISTRIBUZIONE sex")
    lines.append("-" * 70)
    for s, count in stats['sex_dist'].most_common():
        lines.append(f"  {str(s):30} {count:6} ({count*100/stats['n_records']:.1f}%)")
    lines.append("")

    lines.append("DISTRIBUZIONE startDate.type")
    lines.append("-" * 70)
    for t, count in stats['start_type_dist'].most_common():
        lines.append(f"  {str(t):30} {count:6} ({count*100/stats['n_records']:.1f}%)")
    lines.append("")

    lines.append("DISTRIBUZIONE enrollmentInfo.type")
    lines.append("-" * 70)
    for t, count in stats['enroll_type_dist'].most_common():
        lines.append(f"  {str(t):30} {count:6} ({count*100/stats['n_records']:.1f}%)")
    lines.append("")

    lines.append("=" * 70)
    lines.append("STATISTICHE enrollmentInfo.count")
    lines.append("-" * 70)
    lines.append(f"  Mediana (p50):                   {stats['enrollment_p50']}")
    lines.append(f"  p90:                             {stats['enrollment_p90']}")
    lines.append(f"  p99:                             {stats['enrollment_p99']}")
    lines.append(f"  Max:                             {stats['enrollment_max']}")
    lines.append(f"  Trial con > 10.000 pazienti:     {stats['enrollment_big_outliers']} ({stats['enrollment_outliers_pct']})")
    lines.append("")

    lines.append("STATISTICHE locations[]")
    lines.append("-" * 70)
    lines.append(f"  Siti totali nel campione:        {stats['total_sites']}")
    lines.append(f"  Media siti per trial:            {stats['avg_sites_per_trial']}")
    lines.append(f"  Coverage locations[].status:     {stats['site_status_coverage_pct']}")
    lines.append(f"  Coverage locations[].geoPoint:   {stats['site_geo_coverage_pct']}")
    lines.append("")

    lines.append("=" * 70)
    lines.append("MISSING RATE PER VARIABILE (dettaglio completo)")
    lines.append("-" * 70)
    for field, (count, pct_str) in stats['missing'].items():
        lines.append(f"  {field:35} {pct_str:8} ({count} mancanti su {stats['n_records']})")
    lines.append("")

    lines.append("=" * 70)
    lines.append("ISTRUZIONI PER L'USO DI QUESTI DATI")
    lines.append("=" * 70)
    lines.append("")
    lines.append("1. Sezione 8.4 del Data Dictionary:")
    lines.append("   sostituisci '(da misurare)' con i valori della tabella in cima.")
    lines.append("")
    lines.append("2. Colonna 'Data Quality Alert' nella tabella della sezione 6:")
    lines.append("   confronta i tuoi numeri reali con le stime nel documento e")
    lines.append("   aggiorna quelle che divergono significativamente.")
    lines.append("")
    lines.append("3. Aggiorna la sezione 5 (Note metodologiche): la frase sulla")
    lines.append("   'verifica empirica' può ora citare la data del campione.")
    lines.append("")
    lines.append("4. Il file sample_profile_data.csv contiene i dati grezzi se vuoi")
    lines.append("   fare analisi aggiuntive in Excel o pandas.")
    lines.append("")

    with open(filepath, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


# =====================================================================
# FUNZIONE 6 - Salva i dati grezzi in CSV per analisi successive
# =====================================================================

def write_csv(records, filepath):
    """Salva tutti i record estratti in un file CSV."""
    if not records:
        return
    # Tutti i campi possibili
    fieldnames = list(records[0].keys())
    # Sostituiamo le liste con la loro lunghezza o stringa joined
    with open(filepath, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in records:
            row = {}
            for k, v in r.items():
                if isinstance(v, list):
                    row[k] = "|".join(str(x) for x in v) if v else ""
                else:
                    row[k] = v
            writer.writerow(row)


# =====================================================================
# MAIN - punto di ingresso dello script
# =====================================================================

def main():
    print("=" * 70)
    print(" Profilazione del campione — ClinicalTrials.gov API v2")
    print("=" * 70)
    print(f"\nObiettivo: scaricare {SAMPLE_SIZE} trial e analizzarli.")
    print(f"Stima tempo: 10-30 minuti (dipende dalla connessione).\n")

    records = []
    page_token = None
    pages_fetched = 0

    start_time = time.time()

    while len(records) < SAMPLE_SIZE:
        pages_fetched += 1
        print(f"Pagina {pages_fetched}: scarico fino a {PAGE_SIZE} trial "
              f"(totale finora: {len(records)})...")

        studies, page_token = fetch_page(page_token)
        if not studies:
            print("  [INFO] Nessun trial restituito, interrompo.")
            break

        for s in studies:
            try:
                rec = extract_features(s)
                records.append(rec)
                if len(records) >= SAMPLE_SIZE:
                    break
            except Exception as e:
                print(f"  [WARN] Errore nel parsing di un trial: {e}")

        if not page_token:
            print("  [INFO] Non ci sono altre pagine, interrompo.")
            break

        time.sleep(SLEEP_BETWEEN_REQUESTS)

    elapsed = time.time() - start_time
    print(f"\nScaricati {len(records)} trial in {elapsed:.1f} secondi.")
    print("Analizzo il campione...\n")

    # Analisi
    stats = analyze_sample(records)

    # Output
    report_path = os.path.join(OUTPUT_DIR, "sample_profile_report.txt")
    csv_path = os.path.join(OUTPUT_DIR, "sample_profile_data.csv")

    write_report(stats, report_path)
    write_csv(records, csv_path)

    print("=" * 70)
    print(" COMPLETATO")
    print("=" * 70)
    print(f"\nFile generati nella cartella corrente:")
    print(f"  - {report_path}")
    print(f"  - {csv_path}")
    print(f"\nApri sample_profile_report.txt per vedere i risultati.")
    print(f"I dati grezzi sono in sample_profile_data.csv (utile per Excel).\n")


if __name__ == "__main__":
    main()
