# IPUMS DHS Microdata

Trigger: /dhs-ipums

Compute custom statistics from IPUMS DHS survey microdata using natural language. Use when the user wants DHS health or demographics data broken down by custom variables, cross-tabulations not available in StatCompiler, or Excel output with replication documentation.

## Examples
- stunting rate by wealth quintile in Kenya
- contraceptive use by education level in Nigeria
- vaccination coverage by urban/rural in Ghana
- compare stunting across DRC surveys over time
- wasting rate by region in Ethiopia
- bed net use by wealth in Malawi
- percentage of households with electricity in Malawi
- median years of education for women in Nigeria

## What it does

1. **Identifies the right variables** by first searching 1,225 DHS Code Share indicators (with per-indicator DHS variable names and IPUMS mappings already resolved) and then searching ~16,500 unique IPUMS DHS variable names across five units of analysis (women, children, births, household members, men)
2. **Finds the right survey** for the requested country, using pre-scraped availability data to jump directly to the correct survey year
3. **Submits an extract request** to the IPUMS API and waits for the data (typically 30-60 seconds)
4. **Downloads and loads the microdata** with automatic missing-value detection from the DDI codebook
5. **Computes weighted statistics** using the correct DHS survey weights
6. **Presents a formatted table** with human-readable labels and full replication documentation

## CLI commands
```bash
# List available surveys for a country
python3 scripts/ipums_dhs.py samples KE

# Search for variables by keyword
python3 scripts/ipums_dhs.py search "height for age"

# Compute a table with weighted statistics
python3 scripts/ipums_dhs.py table \
  --country <COUNTRY_CODE> \
  --survey latest \
  --variables <IPUMS_VARIABLE_FROM_STEP_1> \
  --unit <UNIT_FROM_DHS_FILE_MAPPING> \
  --by <BREAKDOWN_VARIABLE_FROM_STEP_4> \
  --below <THRESHOLD>
```

### table command arguments

Required: `--country`, `--variables`, `--unit` (women, children, births, household_members, men)

Optional:
- `--survey`: latest (default), all (for trends), a year (2014), or a sample ID (ke2014kr)
- `--by`: cross-tabulate by this variable
- `--below`: compute proportion below threshold (e.g. -2 for stunting); forces continuous mode
- `--median`: compute weighted median instead of weighted mean for continuous variables
- `--filter`: filter rows before computing (e.g. `--filter HHLINENO=1` for household heads only)
- `--weight`: override default weight (usually auto-selected per unit)
- `--scale`: manually set scale divisor (usually auto-detected)
- `--missing-ge`: manually set missing-value threshold (usually auto-detected from DDI)
- `--output`: save results to XLSX file with formatted tables, replication block, and missing values summary
- `--plot`: generate a chart
- `--no-ddi-filter`: skip automatic DDI missing value detection

The script auto-detects whether a variable is categorical or continuous from the DDI codebook. Categorical variables (where most values have labeled codes) show a full weighted frequency table. Continuous variables show a weighted mean (or median with `--median`), or a proportion if `--below` is specified.

## How to answer a question

Follow these steps in order for every new question. Do not skip steps or use cached knowledge from previous queries.

1. **Search dhs_stata_indicators.json first.** This file has 1,225 indicators from the DHS Code Share Project with per-indicator DHS variable names and IPUMS mappings already resolved. Search it by matching the user's topic against the `label` field:

   ```python
   import json
   data = json.loads(open('references/dhs_stata_indicators.json').read())
   query = "stunting"  # replace with topic keyword
   matches = [d for d in data if query.lower() in (d.get('label') or '').lower()]
   for m in matches[:10]:
       print(f"{m['stata_var']:30s} | {m['label']}")
       print(f"  dhs_file={m['dhs_file']}  do_file={m['do_file']}")
       print(f"  ipums_variables={m.get('ipums_variables', [])[:5]}")
       print(f"  dhs_variables={m.get('dhs_variables', [])[:5]}")
   ```

   **If the indicator has `ipums_variables` populated**, use those directly as the IPUMS variable names for the extract — no further lookup needed.

   **If the indicator has only `dhs_variables`** (no IPUMS mapping), look up each DHS variable name in dhs_availability.json:

   ```python
   import json
   avail = json.loads(open('references/dhs_availability.json').read())
   dhs_var = 'v511'  # replace with the DHS variable name from dhs_variables
   for ipums_name, entry in avail.items():
       if (entry.get('dhs_source') or '').lower() == dhs_var.lower():
           print(ipums_name)
   ```

   **Map `dhs_file` to the IPUMS unit:** IR=women, KR=children, PR=household_members, HR=household_members, MR=men, BR=births. This mapping is authoritative — do not let CLI examples or other documentation override it.

   **Note the `do_file` field** — you will need it for the replication block in the output.

   **Do not skip this step.** Do not use codebook keyword search or prior knowledge as a substitute when a matching indicator is found here.

2. **Fall back to codebook search.** If no match in dhs_stata_indicators.json, search the codebook files in references/dhs_codebook_{unit}.md. If the user specifies a unit, search that codebook. If not, search all five and ask which unit to use.

3. **Confirm the variable.** Show the user what variable you found and briefly describe what it measures. If you found multiple plausible variables, list them and ask. If there's one obvious choice, state what you're using and proceed.

4. **Search the same unit's codebook for the breakdown variable** (e.g. wealth quintile, education, urban/rural). Don't assume breakdown variable names are the same across units — WEALTHQ in children vs WEALTHQHH in household_members.

   **Household-level statistics:** When the user asks about households (e.g., "percentage of households with improved water," "household electricity access"), use the household_members unit with `--filter HHLINENO=1` to keep only household heads. Without this filter, each household is counted once per member, overweighting large households. This applies to any indicator where the unit of interest is the household, not the individual. (See: https://www.idhsdata.org/idhs/user_know.shtml)

5. **Run the table command** with --survey latest. The script handles missing values, z-score scaling, survey fallback, and availability lookup automatically.

6. **Present results** by showing the EXACT tables the script outputs. Do not collapse, combine, regroup, or rename categories. Do not add calculated columns by summing categories together. Do not round N values. Do not add a summary paragraph interpreting or combining the results. Present only the tables as the script outputs them.

   **Source attribution in the replication block:**
   - If the indicator was found in dhs_stata_indicators.json (Step 1), include:
     `Source methodology: DHS Code Share Project (https://github.com/DHSProgram/DHS-Indicators-Stata), [do_file name]`
   - If the indicator was found via codebook search (Step 2), include:
     `Source methodology: IPUMS DHS variable codebook (https://www.idhsdata.org)`

7. **Cross-reference with StatCompiler.** After presenting the IPUMS results, also query the StatCompiler skill (/dhs-data) for the same indicator, country, and survey year. Request the most detailed breakdown available — if the IPUMS query used a breakdown by wealth, education, urban/rural, or region, request the same breakdown from StatCompiler. If StatCompiler can't match the exact breakdown, request the national-level value instead. Present both results so the user can compare:

   /dhs-data [indicator] in [country] [year] by [breakdown if available]

   Format the comparison as: 'StatCompiler reports X% for [indicator] in [country] [year]. Our IPUMS microdata computation shows Y%.' If the values are within a few percentage points, note that this validates the computation. If they differ, note the discrepancy and suggest possible reasons (different indicator definitions, different universe restrictions, different survey year). If StatCompiler doesn't have the indicator, note that and skip this step.

## Indicators this tool cannot compute

Some standard DHS indicators require specialized demographic methods beyond weighted tabulations. For these, direct the user to DHS StatCompiler (https://www.statcompiler.com) or published DHS reports:

- **Child mortality rates (NMR, IMR, U5MR)**: Require synthetic cohort life table calculations from birth history data. The raw data is available (KIDALIVE, KIDAGEDIEDIMP in the births unit) but this tool does not implement the demographic methods needed.
- **Total fertility rate (TFR)**: A period rate computed from age-specific birth rates. Not available as a pre-computed variable.
- **Maternal mortality ratio (MMR)**: Requires the sisterhood method. Rarely collected and complex to analyze.

The tool CAN compute related but simpler measures — for example, the proportion of births where the child died (KIDALIVE), or the number of children ever born (CHEB). Make clear to the user that these are not the same as the standard published rates.

## Country codes

| Code | Country | Code | Country | Code | Country |
|------|---------|------|---------|------|---------|
| CD | DRC | GH | Ghana | NG | Nigeria |
| KE | Kenya | UG | Uganda | TZ | Tanzania |
| MW | Malawi | ET | Ethiopia | BF | Burkina Faso |
| ML | Mali | NE | Niger | MZ | Mozambique |
| IA | India | BD | Bangladesh | PK | Pakistan |
| RW | Rwanda | SN | Senegal | SL | Sierra Leone |
| NP | Nepal | ZM | Zambia | ZW | Zimbabwe |

DHS uses some non-standard codes (IA=India, NM=Namibia, BU=Burundi). Run samples command to verify.

## Data source

All data comes from IPUMS DHS (https://www.idhsdata.org), which provides harmonized microdata from the Demographic and Health Surveys. IPUMS DHS is funded by NICHD and maintained by the University of Minnesota.

## Limitations

- Extracts take from 30 seconds to a few minutes. Tell the user you're waiting for the data.
- Not all variables are in every survey. The script auto-falls back to older surveys.
- Standard errors and confidence intervals are not computed. Results are weighted point estimates only.
- If unsure about a variable or its interpretation, say so rather than guessing.
- Results should be sanity-checked against published DHS reports when possible.

## Cautions

**Universe comparability over time:** When showing results across multiple survey years (--survey all), always include a note that older surveys may have different respondent universes and results may not be directly comparable across all years. Check the `notes` field in dhs_stata_indicators.json for indicator-specific caveats, and consult the Guide to DHS Statistics (https://www.dhsprogram.com/pubs/pdf/DHSG1/Guide_to_DHS_Statistics_DHS-8.pdf) for documentation of known changes across DHS phases. (See also: https://www.idhsdata.org/idhs-action/faq)
