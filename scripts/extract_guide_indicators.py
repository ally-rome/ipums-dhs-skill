"""
Extract indicator definitions from the Guide to DHS Statistics (DHS-8) PDF.

Usage:
    python3 extract_guide_indicators.py path/to/Guide_to_DHS_Statistics_DHS-8.pdf

Outputs:
    references/dhs_guide_indicators.json

Requires:
    pdftotext (from poppler-utils)

The Guide to DHS Statistics defines every standard DHS indicator with:
- Definition, numerator, denominator
- DHS recode variable names (e.g., hc70, v312)
- Universe/coverage restrictions
- Missing value handling rules
- Computation method
- Notes on changes across DHS phases

This script parses the ~277 indicator pages into structured JSON for
use by the IPUMS DHS skill.
"""

import json
import re
import subprocess
import sys
from pathlib import Path

# Unicode circled numbers ①–⑬ (U+2460–U+246C) appear as PDF formatting
# artifacts when pdftotext renders footnote-style callout circles from the Guide.
_CIRCLED_CHARS = '①②③④⑤⑥⑦⑧⑨⑩⑪⑫⑬'
_CIRCLED_RE = re.compile(f'[{re.escape(_CIRCLED_CHARS)}]+')

# Every indicator page in the Guide follows an identical template. These are the
# bold section headings that appear on each page in this order. 'Definition' is
# always the first section, which is why a bare 'Definition' line is used below
# as the page-boundary detector — it reliably marks the start of every indicator.
# 'Coverage:' and 'Variables:' include the colon in the source text; is_section_header
# strips it so dict keys are consistent (e.g. 'Coverage' not 'Coverage:').
SECTIONS = [
    'Definition', 'Coverage:', 'Numerator', 'Denominator',
    'Variables:', 'Calculation', 'Handling of Missing Values',
    'Notes and Considerations', 'Changes over Time', 'References',
    'DHS-8 Tabulation', 'API Indicator'
]


def is_section_header(line):
    stripped = line.strip()
    for s in SECTIONS:
        if stripped.startswith(s):
            return s.rstrip(':')
    return None


def extract_indicators(pdf_path: str) -> list[dict]:
    """Extract all indicator pages from the Guide PDF."""
    result = subprocess.run(
        ['pdftotext', pdf_path, '-'],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        raise RuntimeError(f"pdftotext failed: {result.stderr}")

    lines = result.stdout.split('\n')

    # Each indicator page begins with a bold "Definition" heading that pdftotext
    # renders as a bare line containing only the word "Definition". This is the
    # only reliable page boundary marker — the Guide has no page numbers or fixed
    # section delimiters in the text stream.
    indicator_starts = []
    for i, line in enumerate(lines):
        if line.strip() == 'Definition':
            # Scan backwards from the "Definition" line to collect the indicator
            # title, which appears above it on the same page. We skip:
            #   - the running page header ("Guide to DHS Statistics…")
            #   - the publication date ("December 2023")
            #   - form-feed characters (\x0c) that pdftotext inserts at page breaks
            # Accumulation stops at a blank line or an earlier page-break, or
            # after collecting at most 8 lines (longest titles span ~4 lines).
            title_lines = []
            j = i - 1
            while j >= 0 and len(title_lines) < 8:
                l = lines[j].strip()
                if (l and not l.startswith('Guide to DHS')
                        and not l.startswith('December 2023')
                        and l != '\x0c' and not l.startswith('\x0c')):
                    title_lines.insert(0, l)
                elif title_lines:
                    break
                j -= 1
            title = ' '.join(title_lines)
            indicator_starts.append((i, title, j + 1))

    # Parse each indicator
    indicators = []
    for idx in range(len(indicator_starts)):
        def_line, title, title_start = indicator_starts[idx]

        if idx + 1 < len(indicator_starts):
            end_line = indicator_starts[idx + 1][2]
        else:
            end_line = len(lines)

        indicator_lines = lines[def_line:end_line]

        # Parse into sections
        sections = {}
        current_section = 'Definition'
        current_text = []

        for line in indicator_lines:
            stripped = line.strip()
            if (stripped.startswith('Guide to DHS')
                    or stripped.startswith('December 2023')
                    or stripped == '\x0c' or stripped.startswith('\x0c')):
                continue

            header = is_section_header(stripped)
            if header and header != current_section:
                sections[current_section] = '\n'.join(current_text).strip()
                current_section = header
                remainder = stripped[len(header):].strip().lstrip(':').strip()
                current_text = [remainder] if remainder else []
            else:
                current_text.append(stripped)

        if current_text:
            sections[current_section] = '\n'.join(current_text).strip()

        # Clean up definition prefix
        defn = sections.get('Definition', '')
        if defn.startswith('Definition\n'):
            defn = defn[len('Definition\n'):].strip()

        # Clean numerator/denominator prefixes
        num = sections.get('Numerator', '')
        for prefix in ['s:\n', 'Numerators:\n', 'Numerator:\n']:
            if num.startswith(prefix):
                num = num[len(prefix):].strip()

        denom = sections.get('Denominator', '')
        for prefix in ['s:\n', 'Denominators:\n', 'Denominator:\n']:
            if denom.startswith(prefix):
                denom = denom[len(prefix):].strip()

        # DHS variable names follow the pattern: a letter prefix (h/v/m/b/s/etc.)
        # followed by digits, e.g. hc70, v312, hv201. The regex captures these from
        # the Variables section, which lists the recode variables used in the
        # calculation (e.g. "hc70 Height/weight standard used").
        var_text = sections.get('Variables', '')
        dhs_vars = set()
        for match in re.finditer(r'\b([hHvVmMbBsS][a-zA-Z]*\d+[a-zA-Z]*)\b', var_text):
            dhs_vars.add(match.group(1).lower())

        # Classify computation type from the Calculation section text. The heuristics
        # are ordered by specificity: life table methods require demographic software
        # and cannot be reproduced by simple tabulation; median/mean/scalar are
        # straightforward; everything else defaults to proportion (the most common
        # DHS indicator type). The z-score guard on 'mean' prevents anthropometric
        # z-score indicators (which describe a mean z-score) from being classified as
        # 'mean' — they need scale-factor handling and are better treated as 'proportion'
        # when a --below threshold is applied.
        calc_text = sections.get('Calculation', '').lower()
        if 'life table' in calc_text or 'synthetic cohort' in calc_text or 'mortality rate' in calc_text:
            comp_type = 'life_table'
        elif 'median' in calc_text:
            comp_type = 'median'
        elif 'mean' in calc_text and 'z-score' not in title.lower():
            comp_type = 'mean'
        elif 'scalar' in calc_text:
            comp_type = 'scalar'
        else:
            comp_type = 'proportion'

        # The Coverage section names the DHS recode file the indicator comes from
        # (e.g. "IR file" for women, "KR file" for children). This maps directly to
        # the IPUMS unit of analysis via DHS_FILE_TO_UNIT in ipums_dhs.py.
        coverage = sections.get('Coverage', '')
        file_match = re.search(r'\b(IR|KR|PR|HR|MR|BR|CR)\s+file', coverage, re.IGNORECASE)
        dhs_file = file_match.group(1).upper() if file_match else ''

        indicators.append({
            'title': title,
            'definition': defn,
            'coverage': coverage,
            'dhs_file': dhs_file,
            'numerator': num,
            'denominator': denom,
            'variables': var_text,
            'dhs_variable_names': sorted(dhs_vars),
            'calculation': sections.get('Calculation', ''),
            'computation_type': comp_type,
            'missing_values': sections.get('Handling of Missing Values', ''),
            'notes': sections.get('Notes and Considerations', ''),
            'changes_over_time': sections.get('Changes over Time', ''),
        })

    return indicators


def _strip_circled(text: str) -> str:
    """Remove all circled number characters and collapse surrounding whitespace."""
    return _CIRCLED_RE.sub('', text).strip()


def clean_indicators(indicators: list[dict]) -> list[dict]:
    """
    Remove known PDF parsing artifacts from the extracted indicator list.

    Three passes:
      1. Strip circled number characters (①–⑬) from every string field.
         They appear as PDF footnote callouts and have no semantic meaning.
      2. For any entry whose title is now empty (it was nothing but circled
         numbers), attempt to reconstruct the title from the first sentence of
         the definition.  If the definition is also empty the entry is dropped.
      3. Remove the Example Indicator Page from the Guide's introduction.
         It starts with "Provides a more detailed definition of the indicators"
         and is not a real indicator.
    """
    # Pass 1: strip circled chars from all string fields and list-of-string fields
    for item in indicators:
        for key, val in item.items():
            if isinstance(val, str):
                item[key] = _strip_circled(val)
            elif isinstance(val, list):
                item[key] = [
                    _strip_circled(v) if isinstance(v, str) else v for v in val
                ]

    # Pass 2: reconstruct titles that collapsed to empty strings
    # When the title was purely circled numbers (e.g. "①"), stripping leaves "".
    # Use the first sentence of the definition as a fallback title so the entry
    # isn't lost — the circled-number title is a parsing artifact, not a signal
    # that the indicator data itself is bad.
    for item in indicators:
        if not item.get('title'):
            defn = item.get('definition', '')
            # Take text up to the first period or newline, capped at 160 chars
            first_sentence = re.split(r'[.\n]', defn)[0].strip()
            item['title'] = first_sentence[:160]

    # Pass 3: drop the example indicator page and any entry still without a title
    indicators = [
        item for item in indicators
        if item.get('title')
        and not item.get('definition', '').startswith(
            'Provides a more detailed definition of the indicators'
        )
    ]

    return indicators


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python3 extract_guide_indicators.py <path_to_guide_pdf>")
        sys.exit(1)

    pdf_path = sys.argv[1]
    output_path = Path(__file__).parent.parent / 'references' / 'dhs_guide_indicators.json'

    print(f"Extracting indicators from {pdf_path}...")
    indicators = extract_indicators(pdf_path)
    before = len(indicators)
    indicators = clean_indicators(indicators)
    removed = before - len(indicators)
    if removed:
        print(f"  Cleaned {removed} artifact entries (example page, circled-number titles)")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(indicators, indent=2))

    from collections import Counter
    types = Counter(i['computation_type'] for i in indicators)

    print(f"Extracted {len(indicators)} indicators")
    print(f"Computation types: {dict(types)}")
    print(f"Saved to {output_path}")
