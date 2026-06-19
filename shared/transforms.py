"""Reusable field-normalization helpers shared by bronze->silver and silver->gold jobs."""
import datetime
import re

_AGE_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*(year|month|day|week)s?\s*$", re.IGNORECASE)

_UNIT_TO_YEARS = {
    "year": 1.0,
    "month": 1.0 / 12,
    "week": 7.0 / 365,
    "day": 1.0 / 365,
}


def parse_age_to_years(age_str):
    """Parse strings like '18 Years', '6 Months', '30 Days', 'N/A' into numeric years (None if unparseable)."""
    if not age_str or not isinstance(age_str, str):
        return None
    match = _AGE_RE.match(age_str)
    if not match:
        return None
    value, unit = match.groups()
    return round(float(value) * _UNIT_TO_YEARS[unit.lower()], 4)


def normalize_date(date_str):
    """Normalize a ClinicalTrials.gov date string (YYYY, YYYY-MM, or YYYY-MM-DD) into YYYY-MM-DD, or None."""
    if not date_str or not isinstance(date_str, str):
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
    except Exception:
        return None
    return None
