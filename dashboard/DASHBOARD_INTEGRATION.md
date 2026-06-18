# Dashboard Integration Guide

**For:** the whole team
**Owner of the dashboard:** Caterina
**Status:** dashboard works end-to-end with placeholder (fake) data. This document explains the **two and only two** points that must be wired to the real model, and the **data contract** both sides must agree on.

---

## 1. What the dashboard already does (do NOT change this)

The dashboard (`dashboard/app.py`, built with Streamlit) is complete in form:

1. Collects the user's trial inputs (condition, study type, phase, target enrollment, candidate regions).
2. Validates that a condition and at least one region are selected.
3. Calls a prediction function, one result per candidate site.
4. Sorts the results by predicted recruitment velocity (highest first).
5. Shows a ranked table and a map of the sites.

Steps 2, 4 and 5 are final. **Only the data sources in steps 1 and 3 are fake** and must be replaced.

---

## 2. The two points to wire

### Point A — the list of conditions

In `app.py` there is a placeholder list:

```python
CONDITIONS = ["Diabetes", "Hypertension", ...]   # 15 fake entries
```

**Replace with:** the real list of condition terms that exist in the gold data
(ideally the normalized MeSH terms). The dashboard only needs a Python list of
strings. Whoever owns the gold layer can export this list (e.g. a
`SELECT DISTINCT condition` from the gold table, or the MeSH vocabulary used in
feature engineering).

This guarantees the user can only pick conditions that actually exist in the
data — no free-text synonyms, no typos, no mismatches.

### Point B — the prediction function

In `app.py` there is a placeholder function:

```python
def predict_sites(condition, study_type, phase, enrollment, regions):
    # currently returns random numbers
```

**Replace with:** a real call to the trained model, via one of the two options
in section 4. Whatever the option, the function must keep the **same inputs**
and return the **same output shape** described in section 3. If it does, nothing
else in the dashboard needs to change.

---

## 3. The data contract (the important part — agree on this together)

The dashboard and the model must agree on exactly what is sent and what comes back.

### What the dashboard SENDS (the user inputs)

| field        | type            | example                        |
|--------------|-----------------|--------------------------------|
| condition    | string          | "Diabetes"                     |
| study_type   | string          | "Interventional"               |
| phase        | string          | "Phase 3"                      |
| enrollment   | integer         | 400                            |
| regions      | list of strings | ["Italy", "Germany"]           |

### What the dashboard EXPECTS BACK (the ranking)

A list of sites. **Each site must have these exact fields:**

| field                                   | type   | used for           |
|-----------------------------------------|--------|--------------------|
| Site                                    | string | the table          |
| Region                                  | string | the table          |
| Predicted velocity (patients/month)     | number | sorting + table    |
| lat                                     | number | the map            |
| lon                                     | number | the map            |

> The fake function already returns this exact shape. The goal is simply: make
> the real model return the same shape. The model implements Step 5 (one
> inference row per candidate site → predicted velocity); the dashboard just
> displays and sorts it.

---

## 4. Two ways to connect (team decision)

### Option 1 — via the inference API (recommended; matches the planned design)

The dashboard calls the FastAPI `ml_api.py` endpoint over HTTP, sending the user
inputs and receiving the ranked sites as JSON. Dashboard and model stay separate
programs. This is the "inference API → dashboard" path from the project plan.

- Pros: full independence; Caterina and the model owner only need to agree on
  the contract in section 3.
- Needs: the API must be running, and its JSON response must match section 3.

### Option 2 — import the model directly

The dashboard loads the saved model weights and calls the prediction function in
the same process. Simpler for a quick demo, but couples the dashboard to the
model code.

**Either way, the contract in section 3 is what makes it work.**

---

## 5. How we can work in parallel until then

- The dashboard is fully testable **now** with the fake function — no need to
  wait for the model.
- The model owner can build/test the model on a few hand-made gold rows.
- Final integration = swap Point A and Point B, check the contract holds, run
  one demo input end-to-end.

## 6. Open questions for the team

1. Option 1 (API) or Option 2 (direct import)?
2. Who exports the real conditions list (Point A), and in what format?
3. Do the model's output field names match section 3 exactly? If not, we align
   the names **before** integration day, not during it.
