# Gold-layer EDA profile

## Row counts

- `gold.trial_features`: 13242 rows
- `gold.site_history`: 30343 rows

## Target: `target_velocity`

#### `target_velocity` (numeric)

- non-null count: 13242 | null: 0 (0.0%)
- mean: 10.09 | min: 0 | max: 149
- median (p50): 3.661 | p25: 1.23 | p75: 10.15 | p99: 99.32

## `gold.trial_features` (13242 rows)

#### `nct_id` (categorical/text)

- distinct values: 13242 | null: 0 (0.0%)
- note: high-cardinality column, showing top 20 values

| value | count |
|---|---|
| NCT02086617 | 1 |
| NCT01393457 | 1 |
| NCT04409938 | 1 |
| NCT03499184 | 1 |
| NCT05139563 | 1 |
| NCT04470882 | 1 |
| NCT00000224 | 1 |
| NCT00073281 | 1 |
| NCT00000710 | 1 |
| NCT01284816 | 1 |
| NCT04118257 | 1 |
| NCT01110252 | 1 |
| NCT00482560 | 1 |
| NCT03761329 | 1 |
| NCT00000234 | 1 |
| NCT00890552 | 1 |
| NCT01792674 | 1 |
| NCT07583537 | 1 |
| NCT06689007 | 1 |
| NCT06774287 | 1 |
| ... (13222 more distinct values) | |

#### `study_type` (categorical/text)

- distinct values: 1 | null: 0 (0.0%)

| value | count |
|---|---|
| INTERVENTIONAL | 13242 |

#### `primary_purpose` (categorical/text)

- distinct values: 11 | null: 0 (0.0%)

| value | count |
|---|---|
| TREATMENT | 8110 |
| PREVENTION | 1479 |
| OTHER | 868 |
| SUPPORTIVE_CARE | 798 |
| BASIC_SCIENCE | 758 |
| DIAGNOSTIC | 513 |
| HEALTH_SERVICES_RESEARCH | 315 |
| UNKNOWN | 259 |
| SCREENING | 94 |
| DEVICE_FEASIBILITY | 44 |
| ECT | 4 |

#### `lead_sponsor_class` (categorical/text)

- distinct values: 8 | null: 0 (0.0%)

| value | count |
|---|---|
| OTHER | 8831 |
| INDUSTRY | 3532 |
| NIH | 315 |
| OTHER_GOV | 279 |
| FED | 150 |
| NETWORK | 112 |
| INDIV | 20 |
| UNKNOWN | 3 |

#### `sex` (categorical/text)

- distinct values: 3 | null: 0 (0.0%)

| value | count |
|---|---|
| ALL | 11273 |
| FEMALE | 1272 |
| MALE | 697 |

#### `phase` (categorical/text)

- distinct values: 7 | null: 0 (0.0%)

| value | count |
|---|---|
| NA | 6765 |
| PHASE1 | 2065 |
| PHASE2 | 2020 |
| PHASE3 | 1163 |
| PHASE4 | 1073 |
| EARLY_PHASE1 | 150 |
| UNKNOWN | 6 |

#### `enrollment_count` (numeric)

- non-null count: 13242 | null: 0 (0.0%)
- mean: 166.8 | min: 0 | max: 5.965e+04
- median (p50): 59 | p25: 28 | p75: 130 | p99: 1757

#### `n_sites` (numeric)

- non-null count: 13242 | null: 0 (0.0%)
- mean: 6.816 | min: 1 | max: 1003
- median (p50): 1 | p25: 1 | p75: 2 | p99: 116

#### `num_conditions` (numeric)

- non-null count: 13242 | null: 0 (0.0%)
- mean: 1.704 | min: 1 | max: 114
- median (p50): 1 | p25: 1 | p75: 2 | p99: 7

#### `duration_months` (numeric)

- non-null count: 13242 | null: 0 (0.0%)
- mean: 24.88 | min: 0 | max: 329.8
- median (p50): 17.05 | p25: 6.96 | p75: 34.99 | p99: 120

#### `avg_site_exp` (numeric)

- non-null count: 13242 | null: 0 (0.0%)
- mean: 40.8 | min: 1 | max: 448
- median (p50): 16 | p25: 4 | p75: 47.69 | p99: 263

#### `avg_site_vel` (numeric)

- non-null count: 13242 | null: 0 (0.0%)
- mean: 12.51 | min: 0 | max: 148
- median (p50): 10.01 | p25: 6.166 | p75: 15.21 | p99: 60.86

#### `target_velocity` (numeric)

- non-null count: 13242 | null: 0 (0.0%)
- mean: 10.09 | min: 0 | max: 149
- median (p50): 3.661 | p25: 1.23 | p75: 10.15 | p99: 99.32

## `gold.site_history` (30343 rows)

#### `country` (categorical/text)

- distinct values: 163 | null: 0 (0.0%)
- note: high-cardinality column, showing top 20 values

| value | count |
|---|---|
| United States | 7529 |
| Germany | 2506 |
| Japan | 2358 |
| France | 2162 |
| Canada | 1120 |
| United Kingdom | 1067 |
| China | 937 |
| Italy | 691 |
| Spain | 677 |
| Poland | 653 |
| Russia | 638 |
| Brazil | 602 |
| Turkey (Türkiye) | 491 |
| India | 482 |
| Belgium | 480 |
| Argentina | 476 |
| Netherlands | 461 |
| South Korea | 446 |
| Australia | 399 |
| Sweden | 332 |
| ... (143 more distinct values) | |

#### `city` (categorical/text)

- distinct values: 11520 | null: 0 (0.0%)
- note: high-cardinality column, showing top 20 values

| value | count |
|---|---|
| London | 169 |
| Seoul | 132 |
| Moscow | 127 |
| Toronto | 121 |
| São Paulo | 119 |
| Berlin | 117 |
| Montreal | 106 |
| Warsaw | 97 |
| Buenos Aires | 93 |
| Tokyo | 90 |
| Saint Petersburg | 88 |
| Istanbul | 87 |
| Dallas | 79 |
| Houston | 78 |
| New York | 78 |
| Cairo | 77 |
| Chicago | 71 |
| Los Angeles | 70 |
| Santiago | 67 |
| Budapest | 65 |
| ... (11500 more distinct values) | |

#### `state` (categorical/text)

- distinct values: 1654 | null: 0 (0.0%)
- note: high-cardinality column, showing top 20 values

| value | count |
|---|---|
| N/A | 15760 |
| California | 867 |
| Florida | 639 |
| Texas | 571 |
| Ontario | 418 |
| New York | 367 |
| Ohio | 346 |
| Pennsylvania | 342 |
| Illinois | 302 |
| North Carolina | 255 |
| Quebec | 234 |
| Michigan | 213 |
| New Jersey | 207 |
| Georgia | 194 |
| Tokyo | 180 |
| Washington | 173 |
| Massachusetts | 170 |
| Maryland | 169 |
| Virginia | 168 |
| Tennessee | 161 |
| ... (1634 more distinct values) | |

#### `zip` (categorical/text)

- distinct values: 20330 | null: 0 (0.0%)
- note: high-cardinality column, showing top 20 values

| value | count |
|---|---|
| N/A | 5929 |
| D5160R00005 | 73 |
| D3250C00057 | 29 |
| 4000 | 14 |
| 2100 | 13 |
| 4600 | 12 |
| 6000 | 12 |
| 1000 | 12 |
| 5000 | 12 |
| 3100 | 11 |
| 0 | 11 |
| 2000 | 10 |
| 3000 | 10 |
| 8000 | 10 |
| 11000 | 9 |
| 2400 | 9 |
| 7500 | 9 |
| 10000 | 9 |
| 34000 | 9 |
| 10400 | 9 |
| ... (20310 more distinct values) | |

#### `facility_name` (categorical/text)

- distinct values: 18189 | null: 0 (0.0%)
- note: high-cardinality column, showing top 20 values

| value | count |
|---|---|
| Research Site | 3457 |
| Novartis Investigative Site | 1339 |
| GSK Investigational Site | 1273 |
| Nycomed Deutschland GmbH | 1069 |
| Pfizer Investigational Site | 625 |
| Novo Nordisk Investigational Site | 404 |
| For additional information regarding investigative sites for this trial, contact 1-877-CTLILLY (1-877-285-4559, 1-317-615-4559) Mon - Fri from 9 AM to 5 PM Eastern Time (UTC/GMT - 5 hours, EST), or speak with your personal physician. | 398 |
| Boehringer Ingelheim Investigational Site | 372 |
| Altana Pharma/Nycomed | 234 |
| Local Institution | 200 |
| Atea Study Site | 131 |
| ImClone Investigational Site | 124 |
| Shionogi Research Site | 78 |
| Furiex Research Site | 71 |
| Altana Pharma/Nycomed Investigational Site | 62 |
| Galderma Investigational Site | 52 |
| ALTANA Pharma | 41 |
| Pharmacosmos Investigational Site | 38 |
| AURORA Investigative Center | 37 |
| Novartis Investigator Site | 36 |
| ... (18169 more distinct values) | |

#### `latitude` (numeric)

- non-null count: 30343 | null: 0 (0.0%)
- mean: 34.05 | min: -53.16 | max: 69.65
- median (p50): 40.64 | p25: 30.59 | p75: 48.8 | p99: 60.17

#### `longitude` (numeric)

- non-null count: 30343 | null: 0 (0.0%)
- mean: -5.776 | min: -166.1 | max: 178.4
- median (p50): 2.159 | p25: -75.16 | p75: 19.99 | p99: 145

#### `n_trials` (numeric)

- non-null count: 30343 | null: 0 (0.0%)
- mean: 3.484 | min: 1 | max: 448
- median (p50): 1 | p25: 1 | p75: 2 | p99: 35.58

#### `avg_velocity` (numeric)

- non-null count: 30343 | null: 0 (0.0%)
- mean: 16.79 | min: 0 | max: 149
- median (p50): 8.95 | p25: 0.1668 | p75: 22.67 | p99: 117.3

#### `last_year` (numeric)

- non-null count: 23586 | null: 6757 (22.3%)
- mean: 2015 | min: 1970 | max: 2026
- median (p50): 2016 | p25: 2010 | p75: 2020 | p99: 2025

## Flags (model-relevant warnings)

- `gold.trial_features.nct_id`: HIGH CARDINALITY: 13242 distinct values
- `gold.trial_features.study_type`: CONSTANT (all non-null values = 'INTERVENTIONAL')
- `gold.trial_features.enrollment_count`: SURPRISING DISTRIBUTION: max (59651.00) is >10x p99 (1757.34) -- likely extreme outliers
- `gold.trial_features.num_conditions`: SURPRISING DISTRIBUTION: max (114.00) is >10x p99 (7.00) -- likely extreme outliers
- `gold.site_history.country`: HIGH CARDINALITY: 163 distinct values
- `gold.site_history.city`: HIGH CARDINALITY: 11520 distinct values
- `gold.site_history.state`: HIGH CARDINALITY: 1654 distinct values
- `gold.site_history.zip`: HIGH CARDINALITY: 20330 distinct values
- `gold.site_history.facility_name`: HIGH CARDINALITY: 18189 distinct values
- `gold.site_history.n_trials`: SURPRISING DISTRIBUTION: max (448.00) is >10x p99 (35.58) -- likely extreme outliers
