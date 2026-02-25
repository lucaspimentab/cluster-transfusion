from __future__ import annotations

from typing import Dict, List

STAY_BUCKET_MOD = 50

ID_TIME_COLS = [
    "subject_id",
    "stay_id",
    "tbin",
    "charttime_bin",
    "intime",
    "outtime",
    "hadm_id",
]

DEMOGRAPHIC_COLS = ["age", "gender", "weight_kg", "height_cm", "bmi"]

VITAL_COLS = [
    "heart_rate",
    "sbp",
    "dbp",
    "mbp",
    "resp_rate",
    "temperature",
    "spo2",
    "glucose_vital",
]

LAB_COLS = [
    "hemoglobin",
    "hematocrit",
    "platelets",
    "pt",
    "ptt",
    "fibrinogen",
    "d_dimer",
    "lactate",
    "glucose_lab",
    "creatinine",
    "urea",
    "magnesium",
    "phosphorus",
    "potassium",
    "sodium",
    "albumin",
    "hba1c",
    "alt",
    "ast",
    "bilirubin_total",
    "bilirubin_direct",
    "bilirubin_indirect",
    "bilirubin_neonatal",
    "alkaline_phosphatase",
    "ggt",
    "ldh",
    "troponin_i",
    "ckmb",
    "methemoglobin",
    "mpv",
    "hba2",
    "hbs",
    "lymphocytes_abs",
    "crp",
    "ferritin",
    "ntprobnp",
    "ck_total",
]

THERAPY_COLS = [
    "norepinephrine_rate_mcgkgmin",
    "epinephrine_rate_mcgkgmin",
    "dopamine_rate_mcgkgmin",
    "dobutamine_rate_mcgkgmin",
    "vasopressin_rate_unitsmin_or_equiv",
    "vasopressin_rate_raw",
    "vasopressin_rate_norm",
    "phenylephrine_rate_mcgkgmin",
    "milrinone_rate_mcgkgmin",
    "any_vasopressor",
    "nee_mcgkgmin",
    "ventilation_on",
    "imv_on",
    "tracheostomy_flag",
    "rrt_on",
    "rrt_type",
]

TRANSFUSION_COLS = [
    "rbc_transfusion_flag",
    "rbc_amount_ml_event",
    "rbc_totalamount_ml_icu",
]

SCORE_COLS = ["sofa", "sofa_delta", "sapsii", "apsiii"]

FLUID_COLS = ["urineoutput_24h_ml", "fluid_balance_daily_ml", "fluid_balance_total_ml"]

COMORBID_COLS = [
    "myocardial_infarct",
    "congestive_heart_failure",
    "peripheral_vascular_disease",
    "cerebrovascular_disease",
    "dementia",
    "chronic_pulmonary_disease",
    "rheumatic_disease",
    "peptic_ulcer_disease",
    "mild_liver_disease",
    "diabetes_without_cc",
    "diabetes_with_cc",
    "renal_disease",
    "malignant_cancer",
    "severe_liver_disease",
    "metastatic_solid_tumor",
    "aids",
]

REQUIRED_COLUMNS_BY_STEP: Dict[int, List[str]] = {
    1: ID_TIME_COLS + VITAL_COLS + LAB_COLS,
    2: ID_TIME_COLS + VITAL_COLS + LAB_COLS + DEMOGRAPHIC_COLS,
    3: ID_TIME_COLS + VITAL_COLS + LAB_COLS + DEMOGRAPHIC_COLS + THERAPY_COLS,
    4: ID_TIME_COLS + VITAL_COLS + LAB_COLS + DEMOGRAPHIC_COLS + THERAPY_COLS + TRANSFUSION_COLS,
    5: ID_TIME_COLS + VITAL_COLS + LAB_COLS + DEMOGRAPHIC_COLS + THERAPY_COLS + TRANSFUSION_COLS + SCORE_COLS,
    6: ID_TIME_COLS + VITAL_COLS + LAB_COLS + DEMOGRAPHIC_COLS + THERAPY_COLS + TRANSFUSION_COLS + SCORE_COLS + FLUID_COLS,
    7: ID_TIME_COLS + VITAL_COLS + LAB_COLS + DEMOGRAPHIC_COLS + THERAPY_COLS + TRANSFUSION_COLS + SCORE_COLS + FLUID_COLS + COMORBID_COLS,
    8: ID_TIME_COLS + VITAL_COLS + LAB_COLS + DEMOGRAPHIC_COLS + THERAPY_COLS + TRANSFUSION_COLS + SCORE_COLS + FLUID_COLS + COMORBID_COLS,
}

STEP_NAMES = {
    1: "step01_labs_vitals",
    2: "step02_demographics",
    3: "step03_therapies",
    4: "step04_transfusion",
    5: "step05_scores",
    6: "step06_fluids",
    7: "step07_comorbidities",
    8: "step08_engineered_features",
}
