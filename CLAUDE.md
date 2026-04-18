# IPUMS DHS Microdata Skill

A Claude Code skill that queries IPUMS DHS individual-level survey microdata to answer questions about health and demographics in low- and middle-income countries. The tool translates plain-language questions into IPUMS API extract requests, computes weighted statistics, and returns formatted results with full replication documentation.

## Setup

- IPUMS API key must be set as environment variable: `IPUMS_API_KEY`
- The IPUMS collection string for DHS is `"dhs"`
- Python 3.9+
- Install dependencies: `pip install -r requirements.txt`
- IPUMS DHS account with country-level data access approvals at https://www.idhsdata.org

## Reference Files

**Codebooks** — Variable names, labels, and topic groups, scraped from the IPUMS DHS variable browser. One file per unit of analysis:
- `references/dhs_codebook_women.md` — 9,270 variables
- `references/dhs_codebook_children.md` — 11,146 variables
- `references/dhs_codebook_births.md` — 7,899 variables
- `references/dhs_codebook_men.md` — 3,741 variables
- `references/dhs_codebook_household_members.md` — 2,284 variables

**Availability and DHS source mapping:**
- `references/dhs_availability.json` — Per-variable country/year availability and DHS source variable names for all IPUMS DHS variables. Enables smart sample selection (jumping directly to the correct survey year) and maps between DHS recode names (e.g., hc70) and IPUMS variable names (e.g., HWCHAZWHO).

**Indicator methodology:**
- `references/dhs_guide_indicators.json` — 274 indicators extracted from the Guide to DHS Statistics (DHS-8). Each entry includes definitions, numerators/denominators, DHS variable names, computation type (proportion, mean, median, life table), missing value handling rules, and documentation of changes across DHS phases.

**Sample IDs:**
- `references/dhs_sample_ids.md` — Sample IDs from the IPUMS API.

## Scripts

- `scripts/ipums_dhs.py` — Core CLI with three commands: `samples`, `search`, and `table`. Handles extract submission, DDI parsing, weighted statistics, z-score scaling, survey fallback, XLSX output, and replication citations.
- `scripts/scrape_variables.py` — Scrapes the IPUMS DHS variable browser to generate codebook files and availability data. Run with `--availability` to also fetch per-variable detail pages for country/year availability and DHS source names.
- `scripts/extract_guide_indicators.py` — Extracts indicator definitions from the Guide to DHS Statistics PDF into structured JSON.

## How the IPUMS Extract API Works

Extracts are asynchronous: submit a request specifying samples and variables, poll until status is "completed", then download.

There is no metadata API for DHS microdata. Variable names and descriptions must be looked up in the codebook files or on the IPUMS DHS website. Sample IDs can be listed via `ipumspy.get_all_sample_info("dhs")`.

## Sample ID Format

Sample IDs follow the pattern: `{country_code}{year}{unit_suffix}`

| Suffix | Unit | Weight Variable |
|--------|------|----------------|
| ir | Women | PERWEIGHT |
| kr | Children under 5 | PERWEIGHT |
| pr | Household Members | HHWEIGHT |
| br | Births | PERWEIGHT |
| mr | Men | PERWEIGHTMN |
| lr | Calendar Months | — |

Use DVWEIGHT when analyzing domestic violence module variables.

Examples: `ke2022ir` (Kenya 2022 Women), `ng2018kr` (Nigeria 2018 Children)

You cannot combine units in a single extract. PERWEIGHT does not need dividing by 1,000,000 in IPUMS DHS (unlike raw DHS files).

## Extract Submission

ipumspy's `submit_extract()` does not work for DHS as of v0.7.0. It sends `attachedCharacteristics: []` which the DHS API rejects. The script uses raw `requests.post()` instead:

```python
import requests, os

payload = {
    "description": "...",
    "dataFormat": "csv",
    "dataStructure": {"rectangular": {"on": "P"}},
    "samples": {"ke2022ir": {}},
    "variables": {"HWHAZWHO": {}, "PERWEIGHT": {}},
    "collection": "dhs",
    "version": 2,
}
resp = requests.post(
    "https://api.ipums.org/extracts",
    params={"collection": "dhs", "version": 2},
    json=payload,
    headers={"Authorization": os.environ["IPUMS_API_KEY"]},
)
extract_id = resp.json()["number"]
```

Poll status: `GET https://api.ipums.org/extracts/{id}` with same params/headers until `status == "completed"`.

Download URL is at `response["downloadLinks"]["data"]["url"]`. The file is `.csv.gz` — open with `gzip.open(path, "rt")`, not zipfile.

If ipumspy releases a new version, test whether `submit_extract()` works for `collection="dhs"` before assuming this workaround is still needed.

ipumspy is still used for:
- `get_all_sample_info("dhs")` to list available samples
- `readers` module to read downloaded data

## DHS File to IPUMS Unit Mapping

When the Guide to DHS Statistics references a DHS file type, map it to the IPUMS unit:

| DHS File | IPUMS Unit |
|----------|-----------|
| IR | women |
| KR | children |
| PR | household_members |
| HR | household_members |
| MR | men |
| BR | births |

## Key References

- IPUMS DHS: https://www.idhsdata.org
- IPUMS API docs: https://developer.ipums.org/docs/v2/
- ipumspy docs: https://ipumspy.readthedocs.io/
- DHS variable browser: https://www.idhsdata.org/idhs-action/variables/group
- Guide to DHS Statistics (DHS-8): https://www.dhsprogram.com/pubs/pdf/DHSG1/Guide_to_DHS_Statistics_DHS-8.pdf
- What Every IPUMS-DHS User Should Know: https://www.idhsdata.org/idhs/user_know.shtml

## Known Limitations

- **Mortality rates (NMR, IMR, U5MR), TFR, and MMR** cannot be computed — they require specialized demographic methods. Direct users to DHS StatCompiler or published DHS reports.
- **Standard errors and confidence intervals** are not computed. Results are weighted point estimates only.
- **Universe restrictions** are partially handled through IPUMS NIU coding, but the tool does not apply explicit universe filters beyond DDI missing value detection.
- **Household-level statistics** require `--filter HHLINENO=1` to avoid overweighting large households.
- **Older surveys** may have different respondent universes (e.g., ever-married women only). Compare across years with caution.
- **Country access** must be approved per-country through the DHS Program. A 403 error means the user needs to request access at https://www.idhsdata.org.
