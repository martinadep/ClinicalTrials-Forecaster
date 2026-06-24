"""
Manually curated mapping of the top-300 MeSH condition ids (by trial count in
gold.trial_features) to ~15 therapeutic-area categories, approved 2026-06-23.

Editable artifact: AREA_MESH_MAP is grouped by area exactly like the proposal
that was reviewed -- to reassign an id, cut its (id, name) tuple from one
area's list and paste it into another's. An id can appear in more than one
area's list (e.g. cancers that are also reproductive-health relevant); the
multi-hot encoding in features.py sets every area a trial's MeSH ids touch.

Ids from the top-300 NOT present in any area list fall through to the
`area_other` column built in features.py -- mostly generic study descriptors
("Motor Activity", "Patient Satisfaction") and symptoms/outcomes that aren't
diagnoses, rather than ids someone forgot to classify.
"""

AREAS = [
    "oncology",
    "cardiovascular",
    "metabolic_endocrine",
    "neurological",
    "psychiatric",
    "infectious",
    "respiratory",
    "gastrointestinal",
    "musculoskeletal",
    "renal_urological",
    "immunological_rheumatological",
    "ophthalmological",
    "hematological",
    "womens_reproductive_health",
    "dermatological",
    "pain",
]

AREA_MESH_MAP = {
    "oncology": [
        ("D001943", "Breast Neoplasms"), ("D009369", "Neoplasms"),
        ("D011471", "Prostatic Neoplasms"), ("D002289", "Carcinoma, Non-Small-Cell Lung"),
        ("D008175", "Lung Neoplasms"), ("D015179", "Colorectal Neoplasms"),
        ("D015470", "Leukemia, Myeloid, Acute"), ("D010190", "Pancreatic Neoplasms"),
        ("D008223", "Lymphoma"), ("D008545", "Melanoma"), ("D007938", "Leukemia"),
        ("D009362", "Neoplasm Metastasis"), ("D010051", "Ovarian Neoplasms"),
        ("D009101", "Multiple Myeloma"), ("D015451", "Leukemia, Lymphocytic, Chronic, B-Cell"),
        ("D008228", "Lymphoma, Non-Hodgkin"), ("D006258", "Head and Neck Neoplasms"),
        ("D002292", "Carcinoma, Renal Cell"), ("D054198", "Precursor Cell Lymphoblastic Leukemia-Lymphoma"),
        ("D008224", "Lymphoma, Follicular"), ("D006528", "Carcinoma, Hepatocellular"),
        ("D013274", "Stomach Neoplasms"), ("D001932", "Brain Neoplasms"),
        ("D016403", "Lymphoma, Large B-Cell, Diffuse"), ("D005909", "Glioblastoma"),
        ("D009190", "Myelodysplastic Syndromes"), ("D003110", "Colonic Neoplasms"),
        ("D012004", "Rectal Neoplasms"), ("D006689", "Hodgkin Disease"),
        ("D020522", "Lymphoma, Mantle-Cell"), ("D002583", "Uterine Cervical Neoplasms"),
        ("D012509", "Sarcoma"), ("D000077195", "Squamous Cell Carcinoma of Head and Neck"),
        ("D018442", "Lymphoma, B-Cell, Marginal Zone"), ("D005910", "Glioma"),
        ("D002051", "Burkitt Lymphoma"), ("D004938", "Esophageal Neoplasms"),
        ("D001749", "Urinary Bladder Neoplasms"), ("D008113", "Liver Neoplasms"),
        ("D009196", "Myeloproliferative Disorders"), ("D019337", "Hematologic Neoplasms"),
        ("D007680", "Kidney Neoplasms"), ("D016400", "Lymphoma, Large-Cell, Immunoblastic"),
        ("D016543", "Central Nervous System Neoplasms"), ("D009447", "Neuroblastoma"),
        ("D000077216", "Carcinoma, Ovarian Epithelial"), ("D055752", "Small Cell Lung Carcinoma"),
        ("D016889", "Endometrial Neoplasms"), ("D016410", "Lymphoma, T-Cell, Cutaneous"),
        ("D001752", "Blast Crisis"), ("D009182", "Mycosis Fungoides"),
        ("D000230", "Adenocarcinoma"), ("D016393", "Lymphoma, B-Cell"),
        ("D008258", "Waldenstrom Macroglobulinemia"), ("D012751", "Sezary Syndrome"),
        ("D055728", "Primary Myelofibrosis"), ("D002277", "Carcinoma"),
        ("D015466", "Leukemia, Myeloid, Chronic-Phase"), ("D009837", "Oligodendroglioma"),
        ("D005770", "Gastrointestinal Neoplasms"), ("D015477", "Leukemia, Myelomonocytic, Chronic"),
        ("D001254", "Astrocytoma"), ("D015464", "Leukemia, Myelogenous, Chronic, BCR-ABL Positive"),
        ("D018316", "Gliosarcoma"), ("D005185", "Fallopian Tube Neoplasms"),
    ],
    "cardiovascular": [
        ("D006333", "Heart Failure"), ("D006973", "Hypertension"),
        ("D003324", "Coronary Artery Disease"), ("D002318", "Cardiovascular Diseases"),
        ("D001281", "Atrial Fibrillation"), ("D009203", "Myocardial Infarction"),
        ("D050197", "Atherosclerosis"), ("D054058", "Acute Coronary Syndrome"),
        ("D003327", "Coronary Disease"), ("D006331", "Heart Diseases"),
        ("D017202", "Myocardial Ischemia"), ("D058729", "Peripheral Arterial Disease"),
        ("D007022", "Hypotension"), ("D013927", "Thrombosis"),
        ("D001145", "Arrhythmias, Cardiac"), ("D011655", "Pulmonary Embolism"),
        ("D014652", "Vascular Diseases"), ("D020246", "Venous Thrombosis"),
        ("D006330", "Heart Defects, Congenital"), ("D001024", "Aortic Valve Stenosis"),
        ("D007511", "Ischemia"), ("D017544", "Aortic Aneurysm, Abdominal"),
        ("D006323", "Heart Arrest"), ("D000072657", "ST Elevation Myocardial Infarction"),
        ("D006976", "Hypertension, Pulmonary"),
    ],
    "metabolic_endocrine": [
        ("D009765", "Obesity"), ("D003924", "Diabetes Mellitus, Type 2"),
        ("D003920", "Diabetes Mellitus"), ("D003922", "Diabetes Mellitus, Type 1"),
        ("D050177", "Overweight"), ("D007333", "Insulin Resistance"),
        ("D024821", "Metabolic Syndrome"), ("D015431", "Weight Loss"),
        ("D044342", "Malnutrition"), ("D006937", "Hypercholesterolemia"),
        ("D017719", "Diabetic Foot"), ("D011236", "Prediabetic State"),
        ("D018149", "Glucose Intolerance"), ("D009767", "Obesity, Morbid"),
        ("D006949", "Hyperlipidemias"), ("D006943", "Hyperglycemia"),
        ("D007003", "Hypoglycemia"), ("D050171", "Dyslipidemias"),
        ("D063766", "Pediatric Obesity"),
    ],
    "neurological": [
        ("D020521", "Stroke"), ("D010300", "Parkinson Disease"),
        ("D000544", "Alzheimer Disease"), ("D060825", "Cognitive Dysfunction"),
        ("D009103", "Multiple Sclerosis"), ("D000377", "Agnosia"),
        ("D013119", "Spinal Cord Injuries"), ("D003704", "Dementia"),
        ("D002547", "Cerebral Palsy"), ("D004827", "Epilepsy"),
        ("D000083242", "Ischemic Stroke"), ("D000070642", "Brain Injuries, Traumatic"),
        ("D008881", "Migraine Disorders"), ("D001930", "Brain Injuries"),
        ("D000690", "Amyotrophic Lateral Sclerosis"), ("D009422", "Nervous System Diseases"),
        ("D012640", "Seizures"), ("D010291", "Paresis"),
        ("D020529", "Multiple Sclerosis, Relapsing-Remitting"),
    ],
    "psychiatric": [
        ("D003863", "Depression"), ("D001008", "Anxiety Disorders"),
        ("D012559", "Schizophrenia"), ("D019966", "Substance-Related Disorders"),
        ("D013313", "Stress Disorders, Post-Traumatic"), ("D003865", "Depressive Disorder, Major"),
        ("D001289", "Attention Deficit Disorder with Hyperactivity"), ("D011618", "Psychotic Disorders"),
        ("D000437", "Alcoholism"), ("D000067877", "Autism Spectrum Disorder"),
        ("D001714", "Bipolar Disorder"), ("D009293", "Opioid-Related Disorders"),
        ("D001523", "Mental Disorders"), ("D014029", "Tobacco Use Disorder"),
        ("D013315", "Stress, Psychological"), ("D002189", "Marijuana Abuse"),
        ("D003866", "Depressive Disorder"), ("D001321", "Autistic Disorder"),
        ("D019052", "Depression, Postpartum"), ("D019970", "Cocaine-Related Disorders"),
        ("D001068", "Feeding and Eating Disorders"), ("D003693", "Delirium"),
        ("D009771", "Obsessive-Compulsive Disorder"), ("D000098647", "Generalized Anxiety Disorder"),
    ],
    "infectious": [
        ("D015658", "HIV Infections"), ("D000086382", "COVID-19"),
        ("D000163", "Acquired Immunodeficiency Syndrome"), ("D007251", "Influenza, Human"),
        ("D018805", "Sepsis"), ("D007239", "Infections"), ("D008288", "Malaria"),
        ("D006526", "Hepatitis C"), ("D014376", "Tuberculosis"), ("D012772", "Shock, Septic"),
        ("D018352", "Coronavirus Infections"), ("D016778", "Malaria, Falciparum"),
        ("D013530", "Surgical Wound Infection"), ("D000386", "AIDS-Related Complex"),
        ("D019698", "Hepatitis C, Chronic"), ("D001424", "Bacterial Infections"),
        ("D006509", "Hepatitis B"), ("D012749", "Sexually Transmitted Diseases"),
        ("D014777", "Virus Diseases"),
    ],
    "respiratory": [
        ("D029424", "Pulmonary Disease, Chronic Obstructive"), ("D001249", "Asthma"),
        ("D020181", "Sleep Apnea, Obstructive"), ("D003550", "Cystic Fibrosis"),
        ("D012131", "Respiratory Insufficiency"), ("D053120", "Respiratory Aspiration"),
        ("D011014", "Pneumonia"), ("D012891", "Sleep Apnea Syndromes"),
        ("D008171", "Lung Diseases"), ("D012128", "Respiratory Distress Syndrome"),
        ("D004417", "Dyspnea"), ("D053717", "Pneumonia, Ventilator-Associated"),
        ("D065631", "Rhinitis, Allergic"), ("D012141", "Respiratory Tract Infections"),
        ("D000860", "Hypoxia"),
    ],
    "gastrointestinal": [
        ("D065626", "Non-alcoholic Fatty Liver Disease"), ("D003093", "Colitis, Ulcerative"),
        ("D015212", "Inflammatory Bowel Diseases"), ("D003424", "Crohn Disease"),
        ("D043183", "Irritable Bowel Syndrome"), ("D008107", "Liver Diseases"),
        ("D005764", "Gastroesophageal Reflux"), ("D003248", "Constipation"),
        ("D014839", "Vomiting"), ("D003967", "Diarrhea"), ("D008103", "Liver Cirrhosis"),
        ("D003680", "Deglutition Disorders"), ("D005234", "Fatty Liver"),
        ("D001064", "Appendicitis"),
    ],
    "musculoskeletal": [
        ("D010003", "Osteoarthritis"), ("D020370", "Osteoarthritis, Knee"),
        ("D010024", "Osteoporosis"), ("D055948", "Sarcopenia"),
        ("D009140", "Musculoskeletal Diseases"), ("D018908", "Muscle Weakness"),
    ],
    "renal_urological": [
        ("D051436", "Renal Insufficiency, Chronic"), ("D007676", "Kidney Failure, Chronic"),
        ("D051437", "Renal Insufficiency"), ("D058186", "Acute Kidney Injury"),
        ("D007674", "Kidney Diseases"), ("D014549", "Urinary Incontinence"),
        ("D014552", "Urinary Tract Infections"), ("D007172", "Erectile Dysfunction"),
        ("D011470", "Prostatic Hyperplasia"), ("D053201", "Urinary Bladder, Overactive"),
        ("D014550", "Urinary Incontinence, Stress"),
        # also oncology -- urological cancers
        ("D001749", "Urinary Bladder Neoplasms"), ("D007680", "Kidney Neoplasms"),
    ],
    "immunological_rheumatological": [
        ("D001172", "Arthritis, Rheumatoid"), ("D006967", "Hypersensitivity"),
        ("D006086", "Graft vs Host Disease"), ("D008180", "Lupus Erythematosus, Systemic"),
        ("D001168", "Arthritis"), ("D015535", "Arthritis, Psoriatic"),
        ("D012595", "Scleroderma, Systemic"), ("D001327", "Autoimmune Diseases"),
    ],
    "ophthalmological": [
        ("D015352", "Dry Eye Syndromes"), ("D002386", "Cataract"),
        ("D005901", "Glaucoma"), ("D008268", "Macular Degeneration"),
        ("D009216", "Myopia"), ("D009798", "Ocular Hypertension"),
        ("D003930", "Diabetic Retinopathy"),
    ],
    "hematological": [
        ("D000740", "Anemia"), ("D006470", "Hemorrhage"),
        ("D000755", "Anemia, Sickle Cell"), ("D006467", "Hemophilia A"),
    ],
    "womens_reproductive_health": [
        ("D047928", "Premature Birth"), ("D007246", "Infertility"),
        ("D011085", "Polycystic Ovary Syndrome"), ("D011225", "Pre-Eclampsia"),
        ("D004715", "Endometriosis"), ("D001942", "Breast Feeding"),
        # also oncology -- reproductive-tract cancers
        ("D001943", "Breast Neoplasms"), ("D010051", "Ovarian Neoplasms"),
        ("D002583", "Uterine Cervical Neoplasms"), ("D000077216", "Carcinoma, Ovarian Epithelial"),
        ("D016889", "Endometrial Neoplasms"), ("D005185", "Fallopian Tube Neoplasms"),
        # also psychiatric
        ("D019052", "Depression, Postpartum"),
    ],
    "dermatological": [
        ("D011565", "Psoriasis"), ("D003876", "Dermatitis, Atopic"),
        ("D000152", "Acne Vulgaris"), ("D004485", "Eczema"), ("D012393", "Rosacea"),
    ],
    "pain": [
        ("D010146", "Pain"), ("D010149", "Pain, Postoperative"),
        ("D059350", "Chronic Pain"), ("D059787", "Acute Pain"),
        ("D017116", "Low Back Pain"), ("D019547", "Neck Pain"),
        ("D001416", "Back Pain"), ("D020069", "Shoulder Pain"),
        ("D009209", "Myofascial Pain Syndromes"), ("D009437", "Neuralgia"),
        ("D005356", "Fibromyalgia"),
    ],
}


def _build_mesh_to_areas():
    mapping = {}
    for area, pairs in AREA_MESH_MAP.items():
        for mesh_id, _name in pairs:
            mapping.setdefault(mesh_id, []).append(area)
    return mapping


MESH_TO_AREAS = _build_mesh_to_areas()