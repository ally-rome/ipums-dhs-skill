# IPUMS DHS Microdata Skill

A [Claude Code](https://docs.anthropic.com/en/docs/claude-code) skill that answers plain-language questions about health and demographics in low- and middle-income countries using DHS survey microdata.

This skill extends the [StatCompiler skill](https://github.com/stevenbrownstone-givewell/claude-statcompiler), which returns pre-computed official DHS indicators. This skill accesses the underlying individual-level survey data (~16,500 unique IPUMS DHS variables), enabling custom cross-tabulations, breakdowns by any available variable, and access to variables that StatCompiler doesn't offer.

## Examples

```
/dhs-ipums stunting rate by wealth quintile in Kenya
/dhs-ipums percentage of households in Malawi with electricity
/dhs-ipums contraceptive use by education level in Nigeria, save to xlsx
/dhs-ipums median age at first marriage for women in Bangladesh by education
/dhs-ipums compare stunting across Ghana surveys over time, save to xlsx
```

Include "save to xlsx" to get an Excel file with formatted tables and replication documentation.

## Setup

### Prerequisites

- Python 3.9+
- An [IPUMS DHS account](https://www.idhsdata.org) with data access approved for the countries you want to query
- An IPUMS API key ([request one here](https://account.ipums.org/api_keys))

### Installation

```bash
# Install this skill
git clone https://github.com/ally-rome/ipums-dhs-skill.git ~/.claude/skills/dhs-ipums
pip install -r ~/.claude/skills/dhs-ipums/requirements.txt
export IPUMS_API_KEY=your_api_key_here

# Also install the StatCompiler skill (recommended, for cross-referencing results)
git clone https://github.com/stevenbrownstone-givewell/claude-statcompiler.git ~/.claude/skills/dhs-data
```

## How It Works

### Finding the right variable

The skill searches 1,225 indicators from the [DHS Code Share Project](https://github.com/DHSProgram/DHS-Indicators-Stata), each with a human-readable label (e.g., "Stunted child under 5 years"), the IPUMS variable name, the DHS file type, and the source Stata `.do` file. When a match is found, the IPUMS variable is used directly — no manual lookup needed.

If the topic isn't covered by those 1,225 indicators (country-specific modules, uncommon variables, exploratory analysis), the skill falls back to keyword-searching IPUMS codebook files covering all ~16,500 variables.

A pre-scraped availability index (`dhs_availability.json`) maps DHS recode names to IPUMS names and stores which countries/years have each variable, so the script jumps directly to the right survey without trial-and-error API calls.

### Computing statistics

The script downloads microdata via the IPUMS DHS extract API, then:

- **Detects and removes missing values** by parsing the DDI XML codebook for sentinel codes (NIU, missing, flagged, don't know, etc.)
- **Auto-scales z-score variables** — DHS anthropometric measures stored as integers × 100 are detected and rescaled so thresholds like "below -2 SD" work correctly
- **Auto-detects variable type** — categorical variables get full frequency tables; continuous variables get weighted means, medians, or proportions below a threshold
- **Filters for household-level statistics** — `--filter HHLINENO=1` counts each household once, preventing large households from being overweighted
- **Computes weighted statistics** using the correct DHS survey weights (PERWEIGHT, HHWEIGHT, or PERWEIGHTMN depending on the unit)

### Cross-referencing with StatCompiler

After presenting IPUMS results, the skill queries the [StatCompiler skill](https://github.com/stevenbrownstone-givewell/claude-statcompiler) for the same indicator, country, and survey year to validate the output. The StatCompiler skill must be installed at `~/.claude/skills/dhs-data` for this to work.

### Output

Terminal output includes formatted tables with value codes and labels, an Overall row for breakdowns, and a replication block citing the source, sample, weight, variable links, and missing codes excluded.

Excel output (`save to xlsx`) adds bordered tables, a missing values summary with per-code counts, and a comparability warning for multi-year results.

## Direct CLI Usage

The Python script can also be used without Claude Code:

```bash
# List available surveys
python3 scripts/ipums_dhs.py samples KE

# Search for variables
python3 scripts/ipums_dhs.py search "height for age"

# Compute statistics
python3 scripts/ipums_dhs.py table \
  --country KE --survey latest \
  --variables HWHAZWHO --unit children \
  --by WEALTHQ --below -2 --output results.xlsx
```

Run `python3 scripts/ipums_dhs.py table --help` for all options including `--median`, `--filter`, `--survey all`, and more.

## Project Structure

```
ipums-dhs-skill/
├── CLAUDE.md                          # Project context for Claude Code
├── SKILL.md                           # Skill instructions and workflow
├── requirements.txt                   # Python dependencies
├── scripts/
│   ├── ipums_dhs.py                   # Core CLI (samples, search, table)
│   ├── scrape_variables.py            # Scrapes IPUMS DHS variable browser
│   ├── build_stata_indicator_index.py # Converts IndicatorList.xlsx → JSON
│   └── extract_stata_dhs_vars.py      # Enriches JSON with DHS vars from .do files
├── references/
│   ├── dhs_stata_indicators.json      # 1,225 indicators with IPUMS mappings
│   ├── dhs_availability.json          # Per-variable availability + DHS source names
│   ├── dhs_codebook_*.md             # Variable codebooks (one per unit of analysis)
│   └── dhs_sample_ids.md             # Sample IDs from IPUMS API
└── data/                              # Cached extracts (gitignored)
```

## Limitations

- **Mortality rates (NMR, IMR, U5MR), TFR, and MMR** cannot be computed — they require specialized demographic methods. Use [DHS StatCompiler](https://www.statcompiler.com) for these.
- **Standard errors and confidence intervals** are not computed. Results are weighted point estimates only.
- **Universe restrictions** are partially handled through IPUMS NIU coding but not explicitly enforced beyond DDI missing value detection.
- **Older surveys** may have different respondent universes (e.g., ever-married women only). The XLSX output includes a comparability warning for multi-year results.
- **Country access** is per-country through the DHS Program. A 403 error means access hasn't been approved at https://www.idhsdata.org.
- **Extract processing time** varies from 30 seconds to several minutes depending on IPUMS server load.

## How the Reference Data Was Built

All reference files are reproducible. See `CLAUDE.md` for technical details on the data pipeline.

**Variable codebooks and availability:** Scraped from the [IPUMS DHS variable browser](https://www.idhsdata.org/idhs-action/variables/group) — group pages for variable names/labels, individual detail pages for country/year availability and DHS source variable names.

**Stata indicator index:** Extracted from the [DHS Code Share Project](https://github.com/DHSProgram/DHS-Indicators-Stata). The IndicatorList.xlsx provides indicator metadata; the `.do` files provide the actual computation logic. A BFS dependency tracer resolves which DHS recode variables feed into each indicator and maps them to IPUMS names.

```bash
# Regenerate all reference data
python3 scripts/scrape_variables.py all --availability    # ~8 hours
python3 scripts/build_stata_indicator_index.py             # seconds
python3 scripts/extract_stata_dhs_vars.py                  # ~1 minute
```

## References

- [IPUMS DHS](https://www.idhsdata.org) — Harmonized DHS microdata
- [DHS Code Share Project](https://github.com/DHSProgram/DHS-Indicators-Stata) — Official indicator computation code
- [Guide to DHS Statistics (DHS-8)](https://www.dhsprogram.com/pubs/pdf/DHSG1/Guide_to_DHS_Statistics_DHS-8.pdf) — Indicator definitions and methodology
- [What Every IPUMS-DHS User Should Know](https://www.idhsdata.org/idhs/user_know.shtml) — Weights, household statistics, universe restrictions
- [IPUMS API Documentation](https://developer.ipums.org/docs/v2/) — Extract API reference

## Citation

If you use this tool in published work, cite the data source and methodology:

> Data: IPUMS DHS, University of Minnesota, www.idhsdata.org
>
> Indicator definitions: DHS Code Share Project, ICF. github.com/DHSProgram/DHS-Indicators-Stata
>
> Methodology: Guide to DHS Statistics, DHS-8 (2023). ICF, Rockville, Maryland, USA.
