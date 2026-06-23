"""
Single source of truth for the `has_non_diagnostic_condition` flag, shared by
the silver->gold ETL (computed from bronze raw condition strings) and the
model (recomputed at inference from the caller's input conditions), so the
two can never drift apart.

Keyword list approved 2026-06-23, derived from profiling the raw condition
strings of trials with no MeSH id at all (data_exploration/conditions_profile.md).
Trigger rule: ANY matching term present in a trial's raw conditions fires the
flag, regardless of whether other (real-diagnosis) conditions are also listed --
simpler than detecting "no real diagnosis present" and just as accurate in
practice, since these trials almost always list only non-diagnostic terms.
"""
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