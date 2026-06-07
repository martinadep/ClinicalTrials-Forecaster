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

    design_info = design.get("designInfo", {})
    enrollment = design.get("enrollmentInfo", {})

    return {
        "nct_id": identification.get("nctId"),
        "payload_hash": stable_payload_hash(study),
        "brief_title": identification.get("briefTitle"),
        "official_title": identification.get("officialTitle"),
        "acronym": identification.get("acronym"),
        "conditions": protocol.get("conditionsModule", {}).get("conditions"),
        "keywords": protocol.get("conditionsModule", {}).get("keywords"),
        "study_type": design.get("studyType"),
        "phases": design.get("phases"),
        "allocation": design_info.get("allocation"),
        "intervention_model": design_info.get("interventionModel"),
        "primary_purpose": design_info.get("primaryPurpose"),
        "enrollment_count": enrollment.get("count"),
        "enrollment_type": enrollment.get("type"),
        "overall_status": status.get("overallStatus"),
        "start_date": parse_date(status.get("startDateStruct", {}).get("date") if status.get("startDateStruct") else status.get("startDateStruct")),
        "primary_completion_date": parse_date(status.get("primaryCompletionDateStruct", {}).get("date") if status.get("primaryCompletionDateStruct") else None),
        "completion_date": parse_date(status.get("completionDateStruct", {}).get("date") if status.get("completionDateStruct") else None),
        "study_first_post_date": parse_date(status.get("studyFirstPostDateStruct", {}).get("date") if status.get("studyFirstPostDateStruct") else None),
        "last_update_post_date": parse_date(status.get("lastUpdatePostDateStruct", {}).get("date") if status.get("lastUpdatePostDateStruct") else None),
        "lead_sponsor": sponsor.get("leadSponsor"),
        "organization_class": identification.get("organization", {}).get("class") if identification.get("organization") else None,
        "responsible_party": sponsor.get("responsibleParty"),
        "eligibility_criteria": eligibility.get("eligibilityCriteria"),
        "healthy_volunteers": eligibility.get("healthyVolunteers"),
        "sex": eligibility.get("sex"),
        "minimum_age": eligibility.get("minimumAge"),
        "maximum_age": eligibility.get("maximumAge"),
        "locations": protocol.get("contactsLocationsModule", {}).get("locations"),
        "version_holder": derived.get("miscInfoModule", {}).get("versionHolder") if derived.get("miscInfoModule") else None,
    }