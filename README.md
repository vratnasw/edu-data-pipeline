# edu-data-pipeline

**Open-source data pipeline for academic research on US public education.**
Collects, harmonizes, and stores **California-focused** education + economic
+ environmental + housing + safety + social + political + infrastructure
data in [Cloudflare R2](https://www.cloudflare.com/products/r2/) for
reproducible academic research. The full master panel parquet is published
to a public-read R2 bucket so anyone can query it via DuckDB without an
account.

This repo backfills the seven downstream layers of the
[edu-* research stack](#downstream-consumers).

## Data sources (all open / public-domain)

| Group | Source | Official URL | License |
|-------|--------|--------------|---------|
| **Economic** | BEA county GDP/personal income | [apps.bea.gov/api](https://apps.bea.gov/api/) | US public data |
| | BLS county unemployment (LAUS) | [bls.gov/lau](https://www.bls.gov/lau/) | US public data |
| | Census ACS 5-year | [api.census.gov](https://api.census.gov/) | US public data |
| | Census SAIPE school-district poverty | [census.gov/programs-surveys/saipe](https://www.census.gov/programs-surveys/saipe.html) | US public data |
| | FHFA House Price Index | [fhfa.gov/HPI](https://www.fhfa.gov/HPI) | US public data |
| | Zillow ZORI rent index | [zillow.com/research](https://www.zillow.com/research/data/) | Free use w/ attribution |
| | IRS SOI county-level AGI | [irs.gov/statistics/soi-tax-stats-county-data](https://www.irs.gov/statistics/soi-tax-stats-county-data) | US public data |
| | CA SCO local government finance | [bythenumbers.sco.ca.gov](https://bythenumbers.sco.ca.gov/) | CA public records |
| | CA BOE property tax | [boe.ca.gov/dataportal](https://www.boe.ca.gov/dataportal/) | CA public records |
| | CA EDD labor market | [labormarketinfo.edd.ca.gov](https://labormarketinfo.edd.ca.gov/) | CA public records |
| **Health** | CDC PLACES (tract estimates) | [data.cdc.gov](https://data.cdc.gov/) | US public data |
| | CA Healthy Kids Survey (district aggregates) | [chks.wested.org](https://chks.wested.org/) | Public aggregate |
| | CA CDPH county indicators | [cdph.ca.gov](https://www.cdph.ca.gov/) | CA public records |
| **Environment** | CalEnviroScreen 4.0 | [oehha.ca.gov/calenviroscreen](https://oehha.ca.gov/calenviroscreen) | CA public records |
| | EPA AQS (PM2.5, ozone) | [aqs.epa.gov/aqsweb/airdata](https://aqs.epa.gov/aqsweb/airdata) | US public data |
| | EPA TRI | [epa.gov/toxics-release-inventory](https://www.epa.gov/toxics-release-inventory-tri-program) | US public data |
| | NOAA GHCN-Daily | [ncei.noaa.gov/cdo-web](https://www.ncei.noaa.gov/cdo-web) | US public data |
| | FEMA NFHL flood layer | [msc.fema.gov](https://msc.fema.gov/) | US public data |
| **Housing** | HUD AFFH-T | [huduser.gov/datasets/affh](https://www.huduser.gov/portal/datasets/affh.html) | US public data |
| | HUD LIHTC | [huduser.gov/datasets/lihtc](https://www.huduser.gov/portal/datasets/lihtc.html) | US public data |
| | Opportunity Insights neighborhood | [opportunityinsights.org/data](https://opportunityinsights.org/data/) | CC-BY |
| **Safety** | CA DOJ OpenJustice | [openjustice.doj.ca.gov](https://openjustice.doj.ca.gov/) | CA public records |
| | OJJDP juvenile justice | [ojjdp.gov/ojstatbb](https://www.ojjdp.gov/ojstatbb/) | US public data |
| **Social** | CA DHCS Medi-Cal | [dhcs.ca.gov](https://www.dhcs.ca.gov/dataandstats/statistics/) | CA public records |
| | CA CDSS CalFresh/CalWORKs | [cdss.ca.gov/Data-Portal](https://www.cdss.ca.gov/inforesources/Data-Portal) | CA public records |
| **Political** | CA SOS election results | [sos.ca.gov/elections](https://www.sos.ca.gov/elections/) | CA public records |
| | CA LAO budget analyses | [lao.ca.gov/Publications](https://lao.ca.gov/Publications) | CA public records |
| | FollowTheMoney education finance | [followthemoney.org](https://www.followthemoney.org/) | Free for academic use |
| **Infrastructure** | FCC broadband deployment | [fcc.gov/broadband-data](https://www.fcc.gov/general/broadband-deployment-data) | US public data |
| | NCES facilities (FRSS) | [nces.ed.gov/surveys/frss](https://nces.ed.gov/surveys/frss/) | US public data |
| | USAC E-Rate funding | [opendata.usac.org/E-Rate](https://opendata.usac.org/E-Rate/) | US public data |
| **Education** | SEDA (Stanford) district scores | [edopportunity.org/getdata](https://edopportunity.org/getdata/) | CC-BY academic |
| | NCES Common Core of Data | [nces.ed.gov/ccd](https://nces.ed.gov/ccd/files.asp) | US public data |
| | EdFacts state assessments | [ed.gov/EDFacts](https://www.ed.gov/about/ed-overview/EDFacts) | US public data |
| | OI college mobility (mrc_table2) | [opportunityinsights.org/data](https://opportunityinsights.org/data/) | CC-BY |

## R2 storage layout

```
edu-research-data/
├── raw/<source>/<year>.parquet              # untouched downloads
├── processed/<source>/<year>.parquet        # cleaned, CA-filtered
├── processed/joined/county_panel.parquet
├── processed/joined/tract_joined.parquet
├── processed/joined/proximity_joined.parquet
├── processed/joined/master_panel.parquet    # private master
├── public/master_panel.parquet              # public-read mirror
└── public/data_dictionary.json              # column index
```

## Setup

```bash
# 1. Configure credentials
cp .env.example .env
# Fill in R2_* and the API keys you need (BEA / BLS / Census / NOAA / FTM)

# 2. Install
pip install -e .

# 3. Run a dry-run to check source URLs and missing keys
python scripts/run_pipeline.py --all --dry-run

# 4. Run a smoke test (just 2 high-priority groups)
python scripts/run_pipeline.py --economic --education

# 5. Run the full pipeline
python scripts/run_pipeline.py --all

# 6. Validate
python scripts/validate_pipeline.py
```

## Querying the public master panel (no credentials needed)

```python
import duckdb
con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# Replace <R2_ACCOUNT> with the account that hosts the bucket
PUBLIC_PANEL = "https://pub-<R2_ACCOUNT>.r2.dev/public/master_panel.parquet"

df = con.execute(f"""
    SELECT year_num,
            AVG(caaspp_math_met_pct) AS avg_math,
            AVG(chronic_absenteeism_rate) AS avg_absent
    FROM '{PUBLIC_PANEL}'
    GROUP BY year_num
    ORDER BY year_num
""").df()
print(df)
```

## Downstream consumers

This pipeline keeps these sibling repos fed:

- [`edu-causal-rl`](https://github.com/vratnasw/edu-causal-rl) — RD/DiD/IV/SCM
- [`edu-spatial-rl`](https://github.com/vratnasw/edu-spatial-rl) — spatial econometrics
- [`edu-gnn`](https://github.com/vratnasw/edu-gnn) — hierarchical temporal GNN
- [`edu-world-model`](https://github.com/vratnasw/edu-world-model) — ensemble simulator
- [`edu-rl-agent`](https://github.com/vratnasw/edu-rl-agent) — constrained MORL
- [`edu-discovery`](https://github.com/vratnasw/edu-discovery) — paper figure assembly

## Pipeline architecture

```
Collectors (raw download → R2 raw/)
   │
   └─ Processors (clean + standardize → R2 processed/)
        │
        ├─ County joiner   (county-keyed sources → spine)
        ├─ Tract spatial joiner   (point-in-polygon → district aggregate)
        ├─ Proximity joiner   (distance to TRI / LIHTC, IDW)
        │
        └─ Master panel  (left-joins on cds + year, hierarchical impute,
                              column quality, public ACL, data dictionary)
```

Run order is enforced by `scripts/run_pipeline.py` — joiners always
execute after their input processors complete.

## Testing

```bash
pip install -e ".[dev]"
pytest tests/ -q
```

All R2 tests use [`moto`](https://docs.getmoto.org/) to stand up a fake S3
backend — no real credentials needed.

## Data dictionary summary

| Group | # variables | Geographic resolution | Years |
|-------|-------------|----------------------|-------|
| Economic | ~50 | County / metro | 2010–2023 |
| Health | ~15 | Census tract / county | 2010–2023 |
| Environment | ~30 | Census tract / monitor / facility | 2010–2023 |
| Housing | ~20 | Tract / project point | 2010–2023 |
| Safety | ~10 | County / jurisdiction | 2010–2023 |
| Social | ~10 | County | 2010–2023 |
| Political | ~10 | County / state | 2010–2023 |
| Infrastructure | ~10 | County / school | 2014–2023 |
| Education | ~50 | District / school | 2010–2023 |

Run `python scripts/run_pipeline.py --all` then check
`logs/data_dictionary.json` for the live, comprehensive dictionary.

## License

MIT — code only. Data sources are subject to their own (open) licenses, all
documented in the table above.
