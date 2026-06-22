"""
=====================================================================
 profile_sample.py
ClinicalTrials.gov API v2 Sample Profiling Script
Project: Clinical Site Selection and Recruitment Forecasting
=====================================================================
"""

"""
WHAT THIS SCRIPT DOES
---------------------
1. Get a sample of trials from the ClinicalTrials.gov v2 API
   (default: 5,000 trials, configurable).
2. For each trial, extracts the fields described in the Data Dictionary.
3. Calculates statistics for:
     - % of missing values (missing rate)
     - distribution of values for categorical variables (enums)
     - presence/absence of nested lists (locations, conditions, etc.)
     - coverage of MeSH terms
     - distribution of outliers for `enrollmentInfo.count`
4. Generates two output files:
     - sample_profile_report.txt  
     - sample_profile_data.csv   
"""


import sys
import time
import json
import csv
import os
from datetime import datetime
from collections import Counter, defaultdict

try:
    import requests
except ImportError:
    print("\n[ERR] “The ‘requests’ library is missing.”)
    sys.exit(1)


# =====================================================================
# SETUP 
# =====================================================================

SAMPLE_SIZE = 5000           
PAGE_SIZE = 1000            
SLEEP_BETWEEN_REQUESTS = 0.2 
MAX_RETRIES = 5              
OUTPUT_DIR = "./data_exploration"           # folder to save the output to 
API_BASE_URL = "https://clinicaltrials.gov/api/v2/studies"

# Fields we want to extract from the API
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
# FUNCTION 1 - Get a batch of trials from the API
# =====================================================================

def fetch_page(page_token=None):
    """
    Get a trials page from the API.
    Returns: (trial list, next page_token, or None)
    """
    params = {
        "pageSize": PAGE_SIZE,
        "format": "json",
        "fields": "|".join(FIELDS_TO_FETCH),
    }
    if page_token:
        params["pageToken"] = page_token

    # Retry with exponential backoff on network error or 429
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(API_BASE_URL, params=params, timeout=60)
            if response.status_code == 200:
                data = response.json()
                return data.get("studies", []), data.get("nextPageToken")
            elif response.status_code == 429
                wait = 2 ** attempt
                print(f"  [WARN] Rate limited (429). Attendo {wait}s...")
                time.sleep(wait)
            else:
                print(f"  [WARN] HTTP {response.status_code}. Retry...")
                time.sleep(2 ** attempt)
        except requests.exceptions.RequestException as e:
            print(f"  [WARN] Errore di rete: {e}. Retry tra {2**attempt}s...")
            time.sleep(2 ** attempt)

    print("  [ERR] Unable to complete request after retries.")
    return [], None


# =====================================================================
# FUNCTION 2 - Extracts a single field from a trial by handling nested forms
# =====================================================================

def safe_get(study, *path, default=None):
    """
    Safely navigate nested JSON.
    Example: safe_get(study, 'protocolSection', 'statusModule', 'overallStatus')
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
# FUNCTION 3 - Extracts all relevant variables from a trial
# =====================================================================

def extract_features(study):
    """
    From a JSON trial it extracts a flat dict with the variables we want to profile.
    The keys of the dict correspond to the names in the Data Dictionary.
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

    locations_list = locs.get("locations", []) or []

    # Let's analyze the site-level status: for each site we see if there is 'status'
    sites_with_status = sum(1 for l in locations_list if l.get("status"))
    sites_with_geopoint = sum(
        1 for l in locations_list
        if l.get("geoPoint") and l["geoPoint"].get("lat") is not None
    )

    collaborators = sponsor.get("collaborators", []) or []

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
# FUNCTION 4 - Analyzes the sample and produces statistics
# =====================================================================

def analyze_sample(records):
    """
    Calculate missing/coverage/distribution statistics on the sample.
    Returns a dict of metrics ready for the report.
    """
    n = len(records)
    if n == 0:
        return {}

    def pct(num, tot=n):
        return f"{(num / tot * 100):.1f}%" if tot > 0 else "n/a"

    # Missing rate per field
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

    # Distribution overallStatus
    status_dist = Counter(r.get("overallStatus") for r in records)

    # Distribution ACTUAL vs ESTIMATED
    start_type = Counter(r.get("startDateType") for r in records)
    enroll_type = Counter(r.get("enrollmentType") for r in records)

    # Distribution leadSponsorClass
    sponsor_class = Counter(r.get("leadSponsorClass") for r in records)

    # Distribution studyType
    study_type = Counter(r.get("studyType") for r in records)

    # Distribution sex
    sex_dist = Counter(r.get("sex") for r in records)

    # Coverage MeSH
    mesh_present = sum(1 for r in records if r.get("has_mesh"))

    # Site-level geopoint coverage
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

    # Distribution n_sites
    sites_per_trial = [r.get("n_sites", 0) for r in records]
    avg_sites = sum(sites_per_trial) / len(sites_per_trial) if sites_per_trial else 0

    # whyStopped only for TERMINATED/WITHDRAWN/SUSPENDED trials
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
# FUNCTION 5 - Writes the report
# =====================================================================

def write_report(stats, filepath):
    lines = []
    lines.append("=" * 70)
    lines.append(" EMPIRICAL SAMPLE PROFILING — ClinicalTrials.gov API v2")
    lines.append(f" Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append(f" Sample size: {stats['n_records']} trial")
    lines.append("=" * 70)
    lines.append("")

    lines.append("SECTION 8.4 — Profiling table (empirical values)")
    lines.append("-" * 70)
    lines.append("")
    lines.append("Variable                            | % missing | Note")
    lines.append("-" * 70)
    lines.append(f"enrollmentInfo.count               | {stats['missing']['enrollmentCount'][1]:9} | {stats['enrollment_outliers_pct']} on 10,000 patients (outlier)")
    lines.append(f"locations[].geoPoint (site-level)  | {(100 - float(stats['site_geo_coverage_pct'].rstrip('%'))):.1f}%     | Coverage {stats['site_geo_coverage_pct']}")
    lines.append(f"conditionBrowseModule.meshes[]     | {(100 - float(stats['mesh_coverage_pct'].rstrip('%'))):.1f}%     | Coverage {stats['mesh_coverage_pct']}")
    lines.append(f"overallStatus = UNKNOWN            | n/a       | {stats['status_dist'].get('UNKNOWN', 0)} record ({stats['status_dist'].get('UNKNOWN', 0)*100/stats['n_records']:.1f}%)")
    lines.append(f"startDateStruct.type = ACTUAL      | n/a       | {stats['start_type_dist'].get('ACTUAL', 0)*100/stats['n_records']:.1f}% del total")
    lines.append(f"enrollmentInfo.type = ACTUAL       | n/a       | {stats['enroll_type_dist'].get('ACTUAL', 0)*100/stats['n_records']:.1f}% del total")
    lines.append(f"locations[].status present         | n/a       | Coverage {stats['site_status_coverage_pct']}")
    lines.append(f"whyStopped (per TERMINATED)        | n/a       | Coverage {stats['stopped_with_reason_pct']}")
    lines.append("")

    lines.append("=" * 70)
    lines.append("DISTRIBUTION overallStatus")
    lines.append("-" * 70)
    for status, count in stats['status_dist'].most_common():
        lines.append(f"  {str(status):30} {count:6} ({count*100/stats['n_records']:.1f}%)")
    lines.append("")

    lines.append("DISTRIBUTION leadSponsor.class")
    lines.append("-" * 70)
    for cls, count in stats['sponsor_class_dist'].most_common():
        lines.append(f"  {str(cls):30} {count:6} ({count*100/stats['n_records']:.1f}%)")
    lines.append("")

    lines.append("DISTRIBUTION studyType")
    lines.append("-" * 70)
    for st, count in stats['study_type_dist'].most_common():
        lines.append(f"  {str(st):30} {count:6} ({count*100/stats['n_records']:.1f}%)")
    lines.append("")

    lines.append("DISTRIBUTION sex")
    lines.append("-" * 70)
    for s, count in stats['sex_dist'].most_common():
        lines.append(f"  {str(s):30} {count:6} ({count*100/stats['n_records']:.1f}%)")
    lines.append("")

    lines.append("DISTRIBUTION startDate.type")
    lines.append("-" * 70)
    for t, count in stats['start_type_dist'].most_common():
        lines.append(f"  {str(t):30} {count:6} ({count*100/stats['n_records']:.1f}%)")
    lines.append("")

    lines.append("DISTRIBUTION enrollmentInfo.type")
    lines.append("-" * 70)
    for t, count in stats['enroll_type_dist'].most_common():
        lines.append(f"  {str(t):30} {count:6} ({count*100/stats['n_records']:.1f}%)")
    lines.append("")

    lines.append("=" * 70)
    lines.append("STATS enrollmentInfo.count")
    lines.append("-" * 70)
    lines.append(f"  Median (p50):                    {stats['enrollment_p50']}")
    lines.append(f"  p90:                             {stats['enrollment_p90']}")
    lines.append(f"  p99:                             {stats['enrollment_p99']}")
    lines.append(f"  Max:                             {stats['enrollment_max']}")
    lines.append(f"  Trial with > 10.000 patients:    {stats['enrollment_big_outliers']} ({stats['enrollment_outliers_pct']})")
    lines.append("")

    lines.append("STATS locations[]")
    lines.append("-" * 70)
    lines.append(f"  Total sites in the sample:       {stats['total_sites']}")
    lines.append(f"  Average sites for trials:        {stats['avg_sites_per_trial']}")
    lines.append(f"  Coverage locations[].status:     {stats['site_status_coverage_pct']}")
    lines.append(f"  Coverage locations[].geoPoint:   {stats['site_geo_coverage_pct']}")
    lines.append("")

    lines.append("=" * 70)
    lines.append("MISSING RATE PER VARIABLE")
    lines.append("-" * 70)
    for field, (count, pct_str) in stats['missing'].items():
        lines.append(f"  {field:35} {pct_str:8} ({count} missing out of {stats['n_records']})")
    lines.append("")

    lines.append("=" * 70)
    lines.append("INSTRUCTIONS FOR USING THIS DATA")
    lines.append("=" * 70)
    lines.append("")
    lines.append("1. Section 8.4 of the Data Dictionary:")
    lines.append("   Replace ‘(to be measured)’ with the values from the table at the top.")
    lines.append("")
    lines.append("2. The ‘Data Quality Alert’ column in the table in Section 6:")
    lines.append("   Compare your actual numbers with the estimates in the document and")
    lines.append("   update any that differ significantly.")
    lines.append("")
    lines.append("3. Update Section 5 (Methodological Notes): the sentence about")
    lines.append("   ‘empirical verification’ can now include the sample date.")
    lines.append("")
    lines.append("4. The file sample_profile_data.csv contains the raw data if you want")
    lines.append("   to perform additional analysis in Excel or pandas.")
    lines.append("")

    with open(filepath, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


# =====================================================================
# FUNCTION 6 - Saves raw data in CSV format for further analysis
# =====================================================================

def write_csv(records, filepath):
    """Save all extracted records to a CSV file."""
    if not records:
        return
    fieldnames = list(records[0].keys())

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
# MAIN - script entry point ###########
# =====================================================================

def main():
    print("=" * 70)
    print(" Sample Profiling — ClinicalTrials.gov API v2")
    print("=" * 70)
    print(f"\nGoal: Get {SAMPLE_SIZE} trials and analyze them.")

    records = []
    page_token = None
    pages_fetched = 0

    start_time = time.time()

    while len(records) < SAMPLE_SIZE:
        pages_fetched += 1
        print(f"Page {pages_fetched}: obtaining up to {PAGE_SIZE} trials"
              f"(total so far: {len(records)})...")

        studies, page_token = fetch_page(page_token)
        if not studies:
            print("  [INFO] No trials found; Stop.")
            break

        for s in studies:
            try:
                rec = extract_features(s)
                records.append(rec)
                if len(records) >= SAMPLE_SIZE:
                    break
            except Exception as e:
                print(f"  [WARN] Error parsing a trial: {e}")

        if not page_token:
            print("  [INFO] There are no more pages, interrupt.")
            break

        time.sleep(SLEEP_BETWEEN_REQUESTS)

    elapsed = time.time() - start_time
    print(f"\nGet {len(records)} trial in {elapsed:.1f} seconds.")
    print("Analyzing the sample...\n")

    # Analisi
    stats = analyze_sample(records)

    # Output
    report_path = os.path.join(OUTPUT_DIR, "sample_profile_report.txt")
    csv_path = os.path.join(OUTPUT_DIR, "sample_profile_data.csv")

    write_report(stats, report_path)
    write_csv(records, csv_path)

    print("=" * 70)
    print(" COMPLETED")
    print("=" * 70)
    print(f"\nFiles generated in the current folder:")
    print(f"  - {report_path}")
    print(f"  - {csv_path}")
    print(f"\nOpen sample_profile_report.txt to view the results.")
    print(f"The raw data is in sample_profile_data.csv (useful for Excel).\n")


if __name__ == "__main__":
    main()
