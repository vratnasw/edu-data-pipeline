#!/usr/bin/env bash
# Pull all 31 kidsdata.org sub-section summary pages across 5 top categories.
# Run serially; each takes 3-15 min. Total estimate: 2-4 hours.
set -e
cd "$(dirname "$0")/.."
PY="python scripts/collect_kidsdata_topic.py"

# Family Economics (cat 37, 5 sub-sections)
$PY --summary-url 'https://www.kidsdata.org/topic/38/family-income-and-poverty/summary'   --output-prefix kd_econ_income
$PY --summary-url 'https://www.kidsdata.org/topic/39/food-security/summary'                --output-prefix kd_econ_food
$PY --summary-url 'https://www.kidsdata.org/topic/40/homelessness/summary'                --output-prefix kd_econ_homeless
$PY --summary-url 'https://www.kidsdata.org/topic/41/housing-affordability-and-resources/summary' --output-prefix kd_econ_housing
$PY --summary-url 'https://www.kidsdata.org/topic/43/unemployment/summary'                --output-prefix kd_econ_unemploy

# Physical Health (cat 44, 18 sub-sections)
$PY --summary-url 'https://www.kidsdata.org/topic/45/asthma/summary'             --output-prefix kd_health_asthma
$PY --summary-url 'https://www.kidsdata.org/topic/46/breastfeeding/summary'      --output-prefix kd_health_breastfed
$PY --summary-url 'https://www.kidsdata.org/topic/47/cancer/summary'             --output-prefix kd_health_cancer
$PY --summary-url 'https://www.kidsdata.org/topic/49/deaths/summary'             --output-prefix kd_health_deaths
$PY --summary-url 'https://www.kidsdata.org/topic/50/dental-care/summary'        --output-prefix kd_health_dental
$PY --summary-url 'https://www.kidsdata.org/topic/51/health-care/summary'        --output-prefix kd_health_healthcare
$PY --summary-url 'https://www.kidsdata.org/topic/97/health-status/summary'      --output-prefix kd_health_status
$PY --summary-url 'https://www.kidsdata.org/topic/52/hospital-use/summary'       --output-prefix kd_health_hospital
$PY --summary-url 'https://www.kidsdata.org/topic/53/immunizations/summary'      --output-prefix kd_health_immune
$PY --summary-url 'https://www.kidsdata.org/topic/54/infant-mortality/summary'   --output-prefix kd_health_infmort
$PY --summary-url 'https://www.kidsdata.org/topic/55/injuries/summary'           --output-prefix kd_health_injury
$PY --summary-url 'https://www.kidsdata.org/topic/56/low-birthweight-and-preterm-births/summary' --output-prefix kd_health_lbwpt
$PY --summary-url 'https://www.kidsdata.org/topic/57/nutrition/summary'          --output-prefix kd_health_nutrition
$PY --summary-url 'https://www.kidsdata.org/topic/58/physical-fitness/summary'   --output-prefix kd_health_fitness
$PY --summary-url 'https://www.kidsdata.org/topic/59/prenatal-care/summary'      --output-prefix kd_health_prenatal
$PY --summary-url 'https://www.kidsdata.org/topic/60/teen-births/summary'        --output-prefix kd_health_teenbirth
$PY --summary-url 'https://www.kidsdata.org/topic/86/teen-sexual-health/summary' --output-prefix kd_health_teenshealth
$PY --summary-url 'https://www.kidsdata.org/topic/61/weight/summary'             --output-prefix kd_health_weight

# COVID-19 (cat 104, 1 sub-section)
$PY --summary-url 'https://www.kidsdata.org/topic/105/family-experiences-during-the-covid-19-pandemic/summary' --output-prefix kd_covid_family

# Environmental Health (cat 79, 3 sub-sections)
$PY --summary-url 'https://www.kidsdata.org/topic/80/air-quality/summary'    --output-prefix kd_env_air
$PY --summary-url 'https://www.kidsdata.org/topic/81/lead-poisoning/summary' --output-prefix kd_env_lead
$PY --summary-url 'https://www.kidsdata.org/topic/83/water-quality/summary'  --output-prefix kd_env_water

# Special Health Care Needs (cat 12, 4 sub-sections)
$PY --summary-url 'https://www.kidsdata.org/topic/14/characteristics-of-children-with-special-needs/summary' --output-prefix kd_shcn_chars
$PY --summary-url 'https://www.kidsdata.org/topic/13/access-to-services-for-children-with-special-needs/summary' --output-prefix kd_shcn_access
$PY --summary-url 'https://www.kidsdata.org/topic/15/impacts-of-special-health-care-needs-on-children-and-families/summary' --output-prefix kd_shcn_impacts
$PY --summary-url 'https://www.kidsdata.org/topic/17/quality-of-care-for-children-with-special-health-care-needs/summary' --output-prefix kd_shcn_quality

echo "ALL 31 SUMMARIES DONE"
