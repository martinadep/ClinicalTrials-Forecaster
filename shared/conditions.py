import re

NON_DIAGNOSTIC_TERMS = [
    "Healthy",
    "Healthy Volunteers",
    "Healthy Volunteer",
    "Healthy Subjects",
    "Healthy Participants",
    "Healthy Aging",
    "Aging",
    "Pregnancy",
    "Pharmacokinetics",
    "Bioequivalence",
    "Anesthesia",
    "General Anesthesia",
    "Nerve Block",
    "Contraception",
]

_NON_DIAGNOSTIC_RE = re.compile(
    "|".join(re.escape(term) for term in NON_DIAGNOSTIC_TERMS), re.IGNORECASE
)


def has_non_diagnostic_condition(condition_strings):
    """True if any raw condition string matches a non-diagnostic term (case-insensitive substring match).

    condition_strings: iterable of raw free-text condition strings for one trial
    (e.g. protocolSection.conditionsModule.conditions, or bronze.trials.conditions).
    """
    if not condition_strings:
        return False
    return any(_NON_DIAGNOSTIC_RE.search(s) for s in condition_strings if s)