# IPUMS DHS Microdata Skill

A [Claude Code] skill that queries IPUMS DHS individual-level survey microdata to answer plain-language questions about health and demographics in low- and middle-income countries.

This skill extends the [StatCompiler skill](https://github.com/stevenbrownstone-givewell/claude-statcompiler) by accessing 34,340+ harmonized variables from IPUMS DHS instead of StatCompiler's pre-computed indicators. This enables custom cross-tabulations, breakdowns by any available variable, and access to variables that StatCompiler doesn't offer.

Ask a question like **"What is the stunting rate by wealth quintile in Kenya?"** and the skill will identify the correct variable, download the microdata from the IPUMS API, compute weighted statistics, and return formatted results with full replication documentation.

## What it does

1. **Finds the right variable** from 34,340 IPUMS DHS variables using the "Guide to DHS Statistics" and searchable codebooks
2. **Downloads survey microdata** from the IPUMS DHS API for the correct country and survey year
3. **Computes weighted statistics** — proportions, means, medians, and cross-tabulations with automatic missing value handling
4. **Returns formatted results** with replication documentation, and exports to Excel if requested

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
│   ├── ipums_dhs.py            # Core CLI (samples, search, table commands)
│   ├── scrape_variables.py     # Scrapes IPUMS DHS variable browser + availability
│   └── extract_guide_indicators.py  # Extracts indicators from Guide to DHS Statistics PDF
├── references/
│   ├── dhs_codebook_women.md          # 9,270 variables
│   ├── dhs_codebook_children.md       # 11,146 variables
│   ├── dhs_codebook_births.md         # 7,899 variables
│   ├── dhs_codebook_men.md            # 3,741 variables
│   ├── dhs_codebook_household_members.md  # 2,284 variables
│   ├── dhs_guide_indicators.json      # 274 indicators from Guide to DHS Statistics
│   ├── dhs_availability.json          # Per-variable availability + DHS source names
│   └── dhs_sample_ids.md             # Sample IDs from IPUMS API
└── data/                       # Cached extracts (gitignored)
```

## How It Works

### Variable selection

The skill uses two approaches to find the right variable, trying the Guide first:

**Guide-backed (standard indicators):** Most standard DHS indicators — stunting, contraceptive use, vaccination, water/sanitation, anemia, etc. — are covered by the Guide to DHS Statistics. The skill searches `dhs_guide_indicators.json` for the user's topic, finding the indicator definition, DHS variable names, computation type, DHS file type, and missing value handling rules. It then maps DHS variable names to IPUMS names using `dhs_availability.json` (e.g., `hc70` → `HWCHAZWHO`) and maps DHS file types to IPUMS units (IR=women, KR=children, PR=household_members, HR=household_members, MR=men, BR=births).

**Codebook search (everything else):** For variables not in the Guide — country-specific modules, less common indicators, or exploratory analysis — the skill searches the IPUMS codebook files (`references/dhs_codebook_{unit}.md`) by keyword. If the user specified a unit, it searches only that codebook. If not, it searches all five and shows the user which units have relevant variables, asking which to use. It confirms the variable with the user before proceeding.

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
- **DHS source variable name** — the original DHS recode variable name (e.g., `HW70`), parsed from the `<p>` tag after the variable heading

This data is stored in `dhs_availability.json` and enables two features: smart sample selection (jumping directly to the right survey year) and mapping between DHS recode names and IPUMS variable names.

The full availability scrape visits ~16,500 individual variable pages at 0.3 seconds each and takes approximately 8 hours.

### Guide to DHS Statistics indicators

274 indicator definitions were extracted from the [Guide to DHS Statistics (DHS-8)](https://www.dhsprogram.com/pubs/pdf/DHSG1/Guide_to_DHS_Statistics_DHS-8.pdf) using `scripts/extract_guide_indicators.py`. The script extracts text from the 834-page PDF, identifies indicator pages by their consistent structure (Definition, Coverage, Numerators, Denominators, Variables, Calculation, Missing Values, Notes, Changes over Time), and parses each into structured JSON. It also classifies each indicator's computation type (proportion, mean, median, or life table).

### Regenerating reference data

```bash
# Re-scrape codebooks and availability (takes ~8 hours for all units)
python3 scripts/scrape_variables.py all --availability

# Re-extract Guide indicators (requires the Guide PDF and pdftotext from poppler-utils)
# Install poppler: brew install poppler (Mac) or apt install poppler-utils (Linux)
python3 scripts/extract_guide_indicators.py path/to/Guide_to_DHS_Statistics_DHS-8.pdf
```

## References

- [IPUMS DHS](https://www.idhsdata.org) — Harmonized DHS microdata
- [Guide to DHS Statistics (DHS-8)](https://www.dhsprogram.com/pubs/pdf/DHSG1/Guide_to_DHS_Statistics_DHS-8.pdf) — Official indicator definitions and methodology
- [What Every IPUMS-DHS User Should Know](https://www.idhsdata.org/idhs/user_know.shtml) — Essential guidance on weights, household statistics, and universe restrictions
- [IPUMS API Documentation](https://developer.ipums.org/docs/v2/) — Extract API reference

## Citation

If you use this tool in published work, cite both the tool and the data source:

> Data: IPUMS DHS, University of Minnesota, www.idhsdata.org
>
> Methodology: Guide to DHS Statistics, DHS-8 (2023). ICF, Rockville, Maryland, USA.

