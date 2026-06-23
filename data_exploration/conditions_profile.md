# Conditions / MeSH profile
Profiles how trial conditions are represented across bronze/silver/gold, to support designing a manual mapping into ~10-15 therapeutic-area categories.
## Where conditions live
- `bronze.trials.conditions` (JSONB array) -- raw free-text condition strings, exactly as ClinicalTrials.gov reports them (one trial can list multiple).
- `bronze.trials.mesh_conditions` (JSONB array of `{id, term}`) -- MeSH-coded condition terms from the API's `derivedSection.conditionBrowseModule.meshes`, backfilled from `bronze.raw_trials.payload`.
- `gold.dim_mesh_conditions` (id -> name lookup table) and `gold.trial_features.mesh_conditions_ids` (TEXT[] of MeSH ids per trial) -- the MeSH ids actually used downstream (site history, the ML pipeline).
- `gold.site_conditions_history` -- (country, city, zip, mesh_condition_id) -> trial count, i.e. condition history per site.
## Raw conditions (`bronze.trials.conditions`)
- Trials profiled: 18505
- Distinct raw condition strings: 12473
- Conditions per trial: min=1, median=1.0, p99=7.0, max=114

**Sample raw values (as stored):**
- `['Branch Retinal Vein Occlusion', 'Macular Edema']`
- `['Primary Hypercholesterolemia']`
- `['Healthy']`
- `['Pulmonary Embolism']`
- `['Acne Vulgaris']`
- `['Depressive Disorder, Major']`
- `['Healthy']`
- `['Epilepsy']`
- `['Dental Plaque', 'Tooth Discoloration']`
- `['Warts']`
- `['Chronic Kidney Diseases']`
- `['Burns']`
- `['Anxiety']`
- `['SARS-CoV 2']`
- `['Metabolic Syndrome', 'Insulin Sensitivity']`
- `['Food Selection']`
- `['Healthy']`
- `['Graft vs Host Disease']`
- `['Osteosarcoma']`
- `['Specific Work Inhibition']`

**Top 40 most frequent raw condition strings:**

| condition | count |
|---|---|
| Healthy | 472 |
| Obesity | 295 |
| Breast Cancer | 242 |
| Pain | 171 |
| Hypertension | 170 |
| HIV Infections | 168 |
| Depression | 156 |
| Asthma | 144 |
| Diabetes Mellitus, Type 2 | 144 |
| Prostate Cancer | 139 |
| Healthy Volunteers | 131 |
| Stroke | 126 |
| Schizophrenia | 122 |
| Coronary Artery Disease | 121 |
| Diabetes | 114 |
| Cancer | 107 |
| Heart Failure | 105 |
| Anxiety | 98 |
| Cardiovascular Diseases | 94 |
| Rheumatoid Arthritis | 91 |
| Influenza | 87 |
| Colorectal Cancer | 83 |
| Lung Cancer | 82 |
| COVID-19 | 82 |
| Type 2 Diabetes Mellitus | 81 |
| Parkinson Disease | 78 |
| Multiple Sclerosis | 76 |
| Covid19 | 76 |
| Type 2 Diabetes | 75 |
| Atrial Fibrillation | 73 |
| Leukemia | 72 |
| HIV | 71 |
| Diabetes Mellitus | 67 |
| Lymphoma | 66 |
| Overweight | 65 |
| Chronic Obstructive Pulmonary Disease | 64 |
| Osteoarthritis | 64 |
| Postoperative Pain | 62 |
| Quality of Life | 62 |
| Aging | 62 |

## MeSH-coded conditions (`bronze.trials.mesh_conditions`)
- Coverage: 14559/18505 trials have a MeSH value (78.7%)
- MeSH terms per trial: min=0, median=1.0, p99=6.0, max=40

**Sample MeSH values (as stored, `{id, term}` pairs):**
- `[{'id': 'D012516', 'term': 'Osteosarcoma'}]`
- `[{'id': 'D007249', 'term': 'Inflammation'}]`
- `[{'id': 'D011225', 'term': 'Pre-Eclampsia'}]`
- `[{'id': 'D015352', 'term': 'Dry Eye Syndromes'}, {'id': 'D000080343', 'term': 'Meibomian Gland Dysfunction'}]`
- `[{'id': 'D006967', 'term': 'Hypersensitivity'}]`
- `[{'id': 'D001321', 'term': 'Autistic Disorder'}, {'id': 'D007802', 'term': 'Language'}]`
- `[{'id': 'D004831', 'term': 'Epilepsies, Myoclonic'}, {'id': 'D012640', 'term': 'Seizures'}, {'id': 'D004827', 'term': 'Epilepsy'}, {'id': 'D001927', 'term': 'Brain Diseases'}]`
- `[{'id': 'D055752', 'term': 'Small Cell Lung Carcinoma'}]`
- `[{'id': 'D006467', 'term': 'Hemophilia A'}]`
- `[{'id': 'D010342', 'term': 'Patient Acceptance of Health Care'}]`
- `[{'id': 'D013272', 'term': 'Stomach Diseases'}]`
- `[{'id': 'D006937', 'term': 'Hypercholesterolemia'}]`
- `[{'id': 'D011565', 'term': 'Psoriasis'}]`
- `[{'id': 'D009369', 'term': 'Neoplasms'}]`
- `[{'id': 'D013313', 'term': 'Stress Disorders, Post-Traumatic'}, {'id': 'D014947', 'term': 'Wounds and Injuries'}, {'id': 'D040921', 'term': 'Stress Disorders, Traumatic'}, {'id': 'D001008', 'term': 'Anxiety Disorders'}]`
- `[{'id': 'D001943', 'term': 'Breast Neoplasms'}]`
- `[{'id': 'D009765', 'term': 'Obesity'}]`
- `[{'id': 'D000086382', 'term': 'COVID-19'}, {'id': 'D045169', 'term': 'Severe Acute Respiratory Syndrome'}]`
- `[{'id': 'D012178', 'term': 'Retinopathy of Prematurity'}]`
- `[{'id': 'D024821', 'term': 'Metabolic Syndrome'}]`

## MeSH dimension table (`gold.dim_mesh_conditions`)
- Distinct MeSH ids: 2586

**Top 40 MeSH conditions by trial count:**

| mesh_condition_id | mesh_condition_name | n_trials |
|---|---|---|
| D009765 | Obesity | 350 |
| D001943 | Breast Neoplasms | 317 |
| D003924 | Diabetes Mellitus, Type 2 | 312 |
| D009043 | Motor Activity | 310 |
| D010146 | Pain | 289 |
| D009369 | Neoplasms | 277 |
| D003863 | Depression | 247 |
| D003920 | Diabetes Mellitus | 242 |
| D001008 | Anxiety Disorders | 186 |
| D015658 | HIV Infections | 183 |
| D000086382 | COVID-19 | 181 |
| D020521 | Stroke | 178 |
| D010149 | Pain, Postoperative | 172 |
| D029424 | Pulmonary Disease, Chronic Obstructive | 164 |
| D011471 | Prostatic Neoplasms | 162 |
| D006333 | Heart Failure | 149 |
| D006973 | Hypertension | 148 |
| D002289 | Carcinoma, Non-Small-Cell Lung | 142 |
| D001249 | Asthma | 139 |
| D003324 | Coronary Artery Disease | 134 |
| D010300 | Parkinson Disease | 131 |
| D008175 | Lung Neoplasms | 121 |
| D007249 | Inflammation | 117 |
| D000163 | Acquired Immunodeficiency Syndrome | 111 |
| D015179 | Colorectal Neoplasms | 111 |
| D002318 | Cardiovascular Diseases | 110 |
| D003922 | Diabetes Mellitus, Type 1 | 108 |
| D012559 | Schizophrenia | 106 |
| D050177 | Overweight | 106 |
| D015470 | Leukemia, Myeloid, Acute | 103 |
| D007333 | Insulin Resistance | 102 |
| D000544 | Alzheimer Disease | 94 |
| D010190 | Pancreatic Neoplasms | 93 |
| D059350 | Chronic Pain | 92 |
| D008223 | Lymphoma | 91 |
| D008545 | Melanoma | 89 |
| D009362 | Neoplasm Metastasis | 86 |
| D010051 | Ovarian Neoplasms | 86 |
| D060825 | Cognitive Dysfunction | 86 |
| D007938 | Leukemia | 86 |

## `gold.trial_features.mesh_conditions_ids` (what the model/pipeline sees)
- Coverage: 12770/16268 trials have at least one MeSH id (78.5%)
- MeSH ids per trial: min=1, median=1.0, p99=7.0, max=40

## Trials with no MeSH terms at all -- what are their raw conditions?
- 3946/18505 trials (21.3%) have no MeSH terms at all (`mesh_conditions` null or empty).
- Their raw condition strings split into two distinct groups: (1) **non-diagnosis terms** that have nothing to map -- healthy-volunteer/PK/method studies (`Healthy`, `Healthy Volunteers`, `Anesthesia`, `Pharmacokinetics`, `Bioequivalence`, `Surgery`, ...) -- these likely want a dedicated "Healthy / no condition" bucket rather than a keyword rule; and (2) **genuine diagnoses the API just didn't resolve to MeSH** (`HIV`, `Solid Tumors`, `Plaque Psoriasis`, `Acute Myocardial Infarction`, `Kidney Transplantation`, ...) -- these are readable and can still get an ordinary keyword rule.

**Top 40 raw condition strings among no-MeSH trials:**

| condition | count |
|---|---|
| Healthy | 380 |
| Healthy Volunteers | 98 |
| Healthy Subjects | 33 |
| Anesthesia | 29 |
| HIV | 29 |
| Healthy Participants | 27 |
| Aging | 25 |
| Healthy Volunteer | 25 |
| Pregnancy | 21 |
| Pharmacokinetics | 18 |
| Surgery | 18 |
| Quality of Life | 15 |
| Blood Pressure | 15 |
| Solid Tumors | 13 |
| Stress | 12 |
| Unspecified Adult Solid Tumor, Protocol Specific | 12 |
| Plaque Psoriasis | 11 |
| Bioequivalence | 11 |
| Contraception | 11 |
| Cognitive Function | 10 |
| Hepatic Impairment | 10 |
| Kidney Transplantation | 9 |
| HIV-1 Infection | 9 |
| Cognition | 9 |
| Liver Transplantation | 9 |
| Colonoscopy | 9 |
| General Anesthesia | 9 |
| Cardiac Surgery | 8 |
| Accidental Falls | 8 |
| Sleep | 8 |
| Healthy Aging | 8 |
| Solid Tumor | 8 |
| Psoriasis Vulgaris | 8 |
| Nerve Block | 8 |
| Diabetic Macular Edema | 8 |
| Advanced Solid Tumors | 7 |
| Mood | 7 |
| Acute Myocardial Infarction | 7 |
| Acute Respiratory Failure | 7 |
| Dental Anxiety | 7 |

## Normalization issues (case/duplicate variants in raw strings)
Raw strings that collapse to the same value once lowercased (case inconsistency in the source data):

| normalized | variant count | variants |
|---|---|---|
| post-acute covid-19 syndrome | 3 | ['Post-acute Covid-19 Syndrome', 'Post-acute COVID-19 Syndrome', 'Post-Acute COVID-19 Syndrome'] |
| analgesia, patient-controlled | 2 | ['Analgesia, Patient-controlled', 'Analgesia, Patient-Controlled'] |
| castration-resistant prostate cancer | 2 | ['Castration-resistant Prostate Cancer', 'Castration-Resistant Prostate Cancer'] |
| b-cell lymphoma | 2 | ['B-cell Lymphoma', 'B-Cell Lymphoma'] |
| beta-thalassemia | 2 | ['Beta-thalassemia', 'Beta-Thalassemia'] |
| cancer of the prostate | 2 | ['Cancer of the Prostate', 'Cancer of the PROSTATE'] |
| angioimmunoblastic t-cell lymphoma | 2 | ['Angioimmunoblastic T-cell Lymphoma', 'Angioimmunoblastic T-Cell Lymphoma'] |
| catheter-related infections | 2 | ['Catheter-related Infections', 'Catheter-Related Infections'] |
| chemotherapy-induced nausea and vomiting | 2 | ['Chemotherapy-induced Nausea and Vomiting', 'Chemotherapy-Induced Nausea and Vomiting'] |
| chemotherapy-induced peripheral neuropathy | 2 | ['Chemotherapy-induced Peripheral Neuropathy', 'Chemotherapy-Induced Peripheral Neuropathy'] |
| chronic hepatitis b | 2 | ['Chronic Hepatitis b', 'Chronic Hepatitis B'] |
| copd | 2 | ['Copd', 'COPD'] |
| copd exacerbation acute | 2 | ['Copd Exacerbation Acute', 'COPD Exacerbation Acute'] |
| covid-19 | 2 | ['Covid-19', 'COVID-19'] |
| allergic rhinitis | 2 | ['Allergic Rhinitis', 'ALLERGIC RHINITIS'] |

Note: MeSH ids sidestep most of this -- the same condition reported with different free-text casing/spelling/abbreviation (e.g. "Type 2 Diabetes" vs "T2DM") generally maps to a single MeSH id, which is the main reason this project switched from free-text keyword mapping to MeSH ids.
