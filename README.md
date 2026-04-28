# IPUMS DHS Microdata Skill

A [Claude Code](https://docs.anthropic.com/en/docs/claude-code) skill that queries IPUMS DHS individual-level survey microdata to answer plain-language questions about health and demographics in low- and middle-income countries.

This skill extends the [StatCompiler skill](https://github.com/stevenbrownstone-givewell/claude-statcompiler) by accessing ~16,500 unique harmonized variables from IPUMS DHS instead of StatCompiler's pre-computed indicators. This enables custom cross-tabulations, breakdowns by any available variable, and access to variables that StatCompiler doesn't offer.

Ask a question like **"What is the stunting rate by wealth quintile in Kenya?"** and the skill will identify the correct variable, download the microdata from the IPUMS API, compute weighted statistics, and return formatted results with full replication documentation.

## What it does

1. **Finds the right variable** from 1,225 DHS Code Share indicators (with pre-resolved IPUMS variable names) and ~16,500 unique IPUMS DHS variables via searchable codebooks
2. **Downloads survey microdata** from the IPUMS DHS API for the correct country and survey year
3. **Computes weighted statistics** — proportions, means, medians, and cross-tabulations with automatic missing value handling
4. **Returns formatted results** with replication documentation, and exports to Excel if requested
5. **Cross-references with StatCompiler** to validate computed results against official published DHS indicator values

## Quick Start

### Prerequisites

- Python 3.9+
- An [IPUMS DHS account](https://www.idhsdata.org) with data access approved for the countries you want to query
- An IPUMS API key ([request one here](https://account.ipums.org/api_keys))

### Setup

```bash
git clone https://github.com/ally-rome/ipums-dhs-skill.git ~/.claude/skills/dhs-ipums
pip install -r ~/.claude/skills/dhs-ipums/requirements.txt
export IPUMS_API_KEY=your_api_key_here
```

### Companion skill (recommended)

For automatic cross-referencing of results against published DHS values, also install the StatCompiler skill:

```bash
git clone https://github.com/stevenbrownstone-givewell/claude-statcompiler.git ~/.claude/skills/dhs-data
```

### Usage with Claude Code

Install as a Claude Code skill, then invoke with `/dhs-ipums`:

- `/dhs-ipums stunting rate by wealth quintile in Kenya`
- `/dhs-ipums percentage of households in Malawi with electricity`
- `/dhs-ipums vaccination coverage in Tanzania, save to xlsx`
- `/dhs-ipums contraceptive use by education level in Nigeria`
- `/dhs-ipums median years of education for women in Ghana by wealth quintile`
- `/dhs-ipums compare stunting across Kenya surveys over time, save to xlsx`

To get an Excel file with formatted tables and replication documentation, include "save to xlsx" or "save to Excel" in your question.

### Direct CLI usage

```bash
# List available surveys for a country
python3 scripts/ipums_dhs.py samples KE

# Search for variables by keyword
python3 scripts/ipums_dhs.py search "height for age"

# Compute weighted statistics with breakdown
python3 scripts/ipums_dhs.py table \
  --country KE --survey latest \
  --variables HWHAZWHO --unit children \
  --by WEALTHQ --below -2

# Household-level statistic (filter to household heads)
python3 scripts/ipums_dhs.py table \
  --country MW --survey latest \
  --variables ELECTRCHH --unit household_members \
  --filter HHLINENO=1

# Weighted median
python3 scripts/ipums_dhs.py table \
  --country NG --survey latest \
  --variables EDYRTOTAL --unit women \
  --median --by WEALTHQ

# Save to Excel
python3 scripts/ipums_dhs.py table \
  --country NG --survey latest \
  --variables UNMETNEED3 --unit women \
  --by EDUCLVL --output results.xlsx

# Trend over time
python3 scripts/ipums_dhs.py table \
  --country GH --survey all \
  --variables HWHAZWHO --unit children \
  --below -2
```

## Project Structure

```
ipums-dhs-skill/
├── CLAUDE.md                   # Project context for Claude Code
├── SKILL.md                    # Instructions for Claude Code skill usage
├── requirements.txt            # Python dependencies
├── scripts/
│   ├── ipums_dhs.py                 # Core CLI (samples, search, table commands)
│   ├── scrape_variables.py          # Scrapes IPUMS DHS variable browser + availability
│   ├── build_stata_indicator_index.py  # Converts IndicatorList.xlsx → JSON
│   └── extract_stata_dhs_vars.py    # Enriches JSON with DHS vars via .do file BFS
├── references/
│   ├── dhs_codebook_women.md          # 9,270 variables
│   ├── dhs_codebook_children.md       # 11,146 variables
│   ├── dhs_codebook_births.md         # 7,899 variables
│   ├── dhs_codebook_men.md            # 3,741 variables
│   ├── dhs_codebook_household_members.md  # 2,284 variables
│   ├── dhs_stata_indicators.json      # 1,225 indicators with IPUMS mappings
│   ├── dhs_availability.json          # Per-variable availability + DHS source names
│   └── dhs_sample_ids.md             # Sample IDs from IPUMS API
└── data/                       # Cached extracts (gitignored)
```

## How It Works

### Variable selection

The skill uses two approaches to find the right variable:

**Stata indicator index (primary):** The skill first searches `dhs_stata_indicators.json`, which contains 1,225 indicators from the [DHS Code Share Project](https://github.com/DHSProgram/DHS-Indicators-Stata). Each indicator has a human-readable label, its source `.do` file, DHS file type, and — for ~85% of indicators — pre-resolved `dhs_variables` (DHS recode names) and `ipums_variables` (IPUMS variable names). When a match is found, the IPUMS variable names are used directly for the extract with no additional lookup.

**Codebook search (fallback):** For variables not covered by the Stata indicator index — country-specific modules, less common indicators, or exploratory analysis — the skill searches the IPUMS codebook files (`references/dhs_codebook_{unit}.md`) by keyword. If the user specified a unit, it searches only that codebook. If not, it searches all five and shows the user which units have relevant variables, asking which to use. It confirms the variable with the user before proceeding.

### Survey selection and fallback

When `--survey latest` is used, the script:

1. Checks `dhs_availability.json` for the newest survey year that has all requested variables
2. Submits the extract to that specific survey — no trial-and-error
3. If the availability data doesn't cover a variable, falls back to trying each survey from newest to oldest
4. If a variable isn't available in any survey, reports which surveys were tried and suggests checking the IPUMS website
5. If the API returns 403, reports that the user's IPUMS account doesn't have access to that country's data

### Automatic data handling

- **Missing value detection:** Parses the DDI XML codebook that accompanies each extract to identify sentinel codes — NIU (not in universe), missing, flagged, don't know, out of plausible limits, etc. These are removed before computing statistics.
- **Z-score auto-scaling:** DHS anthropometric variables (e.g., HWHAZWHO for height-for-age) are stored as integers × 100. The script detects this from the DDI label and value range and auto-divides by 100 so that thresholds like `--below -2` work correctly.
- **Categorical vs continuous detection:** If more than half of a variable's unique values have DDI labels, it's treated as categorical (full frequency table). Otherwise it's treated as continuous (weighted mean, median, or proportion below threshold).
- **Household head filtering:** For household-level statistics (e.g., "percentage of households with electricity"), `--filter HHLINENO=1` filters to one row per household, preventing large households from being overweighted.

### Cross-reference with StatCompiler

After computing results from IPUMS microdata, the skill also queries the [StatCompiler skill](https://github.com/stevenbrownstone-givewell/claude-statcompiler) (`/dhs-data`) to retrieve the official published DHS indicator value for the same country and survey year. This serves as built-in validation — if both sources produce similar values, the computation is likely correct. If values differ significantly, possible reasons are noted (different indicator definitions, universe restrictions, or survey years). The StatCompiler skill must be installed at `~/.claude/skills/dhs-data` for this feature to work.

### Output

- **Terminal output:** Formatted tables with value codes and labels (e.g., "Poorest (1)"), an Overall row for breakdown tables, and a replication block with source, sample, weight, variable links, missing codes excluded, and data transformations applied.
- **Excel output (`--output`):** Formatted XLSX with bordered tables, a title row, replication block, missing values summary with per-code counts, and a comparability warning for multi-year outputs.

## Limitations

- **Mortality rates (NMR, IMR, U5MR), TFR, and MMR** cannot be computed — they require specialized demographic methods. Use [DHS StatCompiler](https://www.statcompiler.com) for these.
- **Standard errors and confidence intervals** are not computed. Results are weighted point estimates only.
- **Universe restrictions** are partially handled through IPUMS NIU coding but not explicitly enforced beyond DDI missing value detection.
- **Older surveys** may have different respondent universes (e.g., ever-married women only). The XLSX output includes a comparability warning for multi-year results.
- **Country access** is per-country through the DHS Program. A 403 error means access hasn't been approved at https://www.idhsdata.org.
- **Extract processing time** varies from 30 seconds to several minutes depending on IPUMS server load.

## How the Reference Data Was Built

The reference files in `references/` were built through a multi-step data pipeline. All steps are reproducible using the scripts in `scripts/`.

### Variable codebooks

The five codebook files were scraped from the [IPUMS DHS variable browser](https://www.idhsdata.org/idhs-action/variables/group) using `scripts/scrape_variables.py`. The scraper opens a session for each unit of analysis, discovers all topic groups from the sidebar, and parses each group page to extract variable names, labels, and preselected status. Groups are paginated at 60 variables per page; the scraper follows pagination automatically.

### Variable availability and DHS source names

The `--availability` flag on the scraper triggers a second pass that visits each variable's individual detail page (e.g., `https://www.idhsdata.org/idhs-action/variables/HWHAZWHO`). From each page it parses:

- **Country/year availability** — which surveys contain the variable, extracted from the `#availability_section`
- **DHS source variable name** — the original DHS recode variable name (e.g., `HW70`), parsed from the `<span>` tag after the variable heading

This data is stored in `dhs_availability.json` and enables two features: smart sample selection (jumping directly to the right survey year) and mapping between DHS recode names and IPUMS variable names.

The full availability scrape visits ~16,500 individual variable pages at 0.3 seconds each and takes approximately 8 hours.

### Stata indicator index

1,225 indicator definitions were extracted from the [DHS Code Share Project](https://github.com/DHSProgram/DHS-Indicators-Stata) in two steps:

1. `scripts/build_stata_indicator_index.py` converts the IndicatorList.xlsx (one sheet per chapter, available from the Code Share Project) into `dhs_stata_indicators.json` with fields: `stata_var`, `label`, `chapter`, `do_file`, `dhs_file`, `notes`.

2. `scripts/extract_stata_dhs_vars.py` clones the Stata repo, builds a variable dependency graph for each `.do` file using regex parsing of `gen`/`replace`/`recode`/`foreach`/`forvalues` statements, and runs a BFS per indicator to trace which raw DHS recode variables (e.g. `h3`, `h5`, `h7`) feed into each output variable. It then reverse-maps those DHS names to IPUMS variable names using `dhs_availability.json`. Both root-level and `DHS8/` subfolder versions of each `.do` file are merged into a single dependency graph.

### Regenerating reference data

```bash
# Re-scrape codebooks and availability (takes ~8 hours for all units)
python3 scripts/scrape_variables.py all --availability

# Rebuild Stata indicator index (requires IndicatorList.xlsx from DHS Code Share Project)
python3 scripts/build_stata_indicator_index.py
python3 scripts/extract_stata_dhs_vars.py
```

## References

- [IPUMS DHS](https://www.idhsdata.org) — Harmonized DHS microdata
- [DHS Code Share Project (Stata)](https://github.com/DHSProgram/DHS-Indicators-Stata) — Official indicator computation code
- [Guide to DHS Statistics (DHS-8)](https://www.dhsprogram.com/pubs/pdf/DHSG1/Guide_to_DHS_Statistics_DHS-8.pdf) — Official indicator definitions and methodology
- [What Every IPUMS-DHS User Should Know](https://www.idhsdata.org/idhs/user_know.shtml) — Essential guidance on weights, household statistics, and universe restrictions
- [IPUMS API Documentation](https://developer.ipums.org/docs/v2/) — Extract API reference

## Citation

If you use this tool in published work, cite the data source and methodology:

> Data: IPUMS DHS, University of Minnesota, www.idhsdata.org
>
> Indicator definitions: DHS Code Share Project, ICF. github.com/DHSProgram/DHS-Indicators-Stata
>
> Methodology: Guide to DHS Statistics, DHS-8 (2023). ICF, Rockville, Maryland, USA.
