# IPUMS DHS Microdata Skill

A Claude Code skill that queries IPUMS DHS individual-level survey microdata to answer questions about health and demographics in low- and middle-income countries. The tool translates plain-language questions into IPUMS API extract requests, computes weighted statistics, and returns formatted results with full replication documentation.

## Setup

- IPUMS API key must be set as environment variable: `IPUMS_API_KEY`
- The IPUMS collection string for DHS is `"dhs"`
- Python 3.9+
- Install dependencies: `pip install -r requirements.txt`
- IPUMS DHS account with country-level data access approvals at https://www.idhsdata.org

## Reference Files

**Stata indicator index:**
- `references/dhs_stata_indicators.json` — 1,225 indicators from the DHS Code Share Project (github.com/DHSProgram/DHS-Indicators-Stata). Each entry includes the Stata variable name, human-readable label, chapter, source `.do` file, DHS file type, and (for ~85% of indicators) pre-resolved `dhs_variables` and `ipums_variables` lists. This is the primary lookup for variable identification. The ~15% without resolved variables (187 indicators) are mostly domestic violence indicators whose computation code isn't in the public repo, demographic rates computed via life table methods, and xlsx/code mismatches. These fall back to codebook search.

**Availability and DHS source mapping:**
- `references/dhs_availability.json` — Per-variable country/year availability and DHS source variable names for all IPUMS DHS variables. Enables smart sample selection (jumping directly to the correct survey year) and maps between DHS recode names (e.g., hc70) and IPUMS variable names (e.g., HWCHAZWHO).

**Codebooks** — Variable names, labels, and topic groups, scraped from the IPUMS DHS variable browser. One file per unit of analysis (~16,500 unique variables total; counts below include cross-unit duplicates):
- `references/dhs_codebook_women.md` — 9,270 variables
- `references/dhs_codebook_children.md` — 11,146 variables
- `references/dhs_codebook_births.md` — 7,899 variables
- `references/dhs_codebook_men.md` — 3,741 variables
- `references/dhs_codebook_household_members.md` — 2,284 variables

**Sample IDs:**
- `references/dhs_sample_ids.md` — Sample IDs from the IPUMS API.

## Scripts

- `scripts/ipums_dhs.py` — Core CLI with three commands: `samples`, `search`, and `table`. Handles extract submission, DDI parsing, weighted statistics, z-score scaling, survey fallback, XLSX output, and replication citations.
- `scripts/scrape_variables.py` — Scrapes the IPUMS DHS variable browser to generate codebook files and availability data. Run with `--availability` to also fetch per-variable detail pages for country/year availability and DHS source names.
- `scripts/build_stata_indicator_index.py` — Converts the DHS Code Share IndicatorList.xlsx to `dhs_stata_indicators.json`. Run once when the xlsx is updated.
- `scripts/extract_stata_dhs_vars.py` — Clones the DHS-Indicators-Stata repo and enriches each indicator in the JSON with per-indicator DHS recode variable names and IPUMS mappings via BFS dependency tracing through the Stata .do files.

## How the IPUMS Extract API Works

Extracts are asynchronous: submit a request specifying samples and variables, poll until status is "completed", then download. The script uses raw `requests.post()` to `https://api.ipums.org/extracts` because ipumspy's `submit_extract()` does not work for DHS as of v0.7.0 (it sends `attachedCharacteristics: []` which the DHS API rejects). If ipumspy releases a new version, test whether `submit_extract()` works for `collection="dhs"` before assuming this workaround is still needed.

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

## DHS File to IPUMS Unit Mapping

When dhs_stata_indicators.json or other DHS references specify a file type, map it to the IPUMS unit:

| DHS File | IPUMS Unit |
|----------|-----------|
| IR | women |
| KR | children |
| PR | household_members |
| HR | household_members |
| MR | men |
| BR | births |

## Cross-Reference with StatCompiler

After computing IPUMS microdata results, the skill also queries the StatCompiler skill (/dhs-data) to retrieve the official published DHS indicator value for the same country and survey year. This serves as built-in validation — if both sources agree, the computation is likely correct. If they differ, the skill notes the discrepancy and suggests possible reasons. The StatCompiler skill must be installed at `~/.claude/skills/dhs-data`. See: https://github.com/stevenbrownstone-givewell/claude-statcompiler

## Key References

- IPUMS DHS: https://www.idhsdata.org
- IPUMS API docs: https://developer.ipums.org/docs/v2/
- DHS variable browser: https://www.idhsdata.org/idhs-action/variables/group
- Guide to DHS Statistics (DHS-8): https://www.dhsprogram.com/pubs/pdf/DHSG1/Guide_to_DHS_Statistics_DHS-8.pdf
- What Every IPUMS-DHS User Should Know: https://www.idhsdata.org/idhs/user_know.shtml

## Known Limitations

- **Mortality rates (NMR, IMR, U5MR), TFR, and MMR** cannot be computed — they require specialized demographic methods. Direct users to DHS StatCompiler or published DHS reports.
- **Standard errors and confidence intervals** are not computed. Results are weighted point estimates only.
- **Universe restrictions** are extracted from DHS Code Share `.do` files into `dhs_stata_indicators.json` (`universe_restrictions` field) and applied as `--filter` flags. The `--filter` argument supports equality and inequality operators (`=`, `>=`, `<=`, `>`, `<`). Always single-quote filter specs with `>=`/`<=` to prevent shell redirection.
- **Household-level statistics** require `--filter HHLINENO=1` to avoid overweighting large households.
- **Older surveys** may have different respondent universes (e.g., ever-married women only). Compare across years with caution.
- **Country access** must be approved per-country through the DHS Program. A 403 error means the user needs to request access at https://www.idhsdata.org.
