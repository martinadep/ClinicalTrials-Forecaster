import json
import hashlib
import datetime

def stable_payload_hash(study):
    normalized = json.dumps(study, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def parse_date(date_str):
    """Normalize date strings into YYYY-MM-DD or None."""
    if not date_str:
        return None
    if isinstance(date_str, dict):
        date_str = date_str.get("date") or date_str.get("value") or None
    if not isinstance(date_str, str):
        return None
    s = date_str.strip()
    try:
        if len(s) == 10:
            datetime.date.fromisoformat(s)
            return s
        if len(s) == 7:
            return s + "-01"
        if len(s) == 4:
            return s + "-01-01"
        dt = datetime.date.fromisoformat(s)
        return dt.isoformat()
    except Exception:
        return None

def extract_trial_fields(study):
    """Extract normalized fields used by bronze tables from one study payload."""
    protocol = study.get("protocolSection", {})
    identification = protocol.get("identificationModule", {})
    status = protocol.get("statusModule", {})
    design = protocol.get("designModule", {})
    sponsor = protocol.get("sponsorCollaboratorsModule", {})
    eligibility = protocol.get("eligibilityModule", {})
    derived = study.get("derivedSection", {})
    description = protocol.get("descriptionModule", {})

    design_info = design.get("designInfo", {})
    enrollment = design.get("enrollmentInfo", {})
    
    condition_browse = derived.get("conditionBrowseModule", {})
    mesh_terms_list = condition_browse.get("meshes", []) 
    lead_sponsor_class = sponsor.get("leadSponsor", {}).get("class")

    return {
        "nct_id": identification.get("nctId"),
        "payload_hash": stable_payload_hash(study),
        "brief_title": identification.get("briefTitle"),
        "brief_summary": description.get("briefSummary"),
        "official_title": identification.get("officialTitle"),
        "acronym": identification.get("acronym"),
        
        "conditions": json.dumps(protocol.get("conditionsModule", {}).get("conditions", [])),
        "mesh_conditions": json.dumps(mesh_terms_list), 
        
        "keywords": protocol.get("conditionsModule", {}).get("keywords"),
        "study_type": design.get("studyType"),
        "phases": design.get("phases"),
        "allocation": design_info.get("allocation"),
        "intervention_model": design_info.get("interventionModel"),
        "primary_purpose": design_info.get("primaryPurpose"),
        "enrollment_count": enrollment.get("count"),
        "enrollment_type": enrollment.get("type"),
        "overall_status": status.get("overallStatus"),
        
        "start_date": parse_date(status.get("startDateStruct")),
        "primary_completion_date": parse_date(status.get("primaryCompletionDateStruct")),
        "completion_date": parse_date(status.get("completionDateStruct")),
        "study_first_post_date": parse_date(status.get("studyFirstPostDateStruct")),
        "last_update_post_date": parse_date(status.get("lastUpdatePostDateStruct")),
        
        "lead_sponsor_class": lead_sponsor_class,
        "organization_class": identification.get("organization", {}).get("class"),
        "collaborator_names": [c.get("name") for c in sponsor.get("collaborators", []) if c.get("name")],
        "responsible_party": sponsor.get("responsibleParty"),
        "eligibility_criteria": eligibility.get("eligibilityCriteria"),
        "healthy_volunteers": eligibility.get("healthyVolunteers"),
        "sex": eligibility.get("sex"),
        "minimum_age": eligibility.get("minimumAge"),
        "maximum_age": eligibility.get("maximumAge"),
        "locations": json.dumps(protocol.get("contactsLocationsModule", {}).get("locations", [])),
        "version_holder": derived.get("miscInfoModule", {}).get("versionHolder"),
    }