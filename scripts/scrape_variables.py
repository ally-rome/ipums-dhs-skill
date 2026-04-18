"""
Scrape IPUMS DHS variable browser and output markdown codebooks.

Usage:
    python3 scripts/scrape_variables.py women
    python3 scripts/scrape_variables.py all

Availability scraping (--availability flag):
    Adds a second pass that visits each variable's detail page to record which
    country/year surveys contain it. This is slow (~0.6s/variable × 34k vars
    = ~6 hrs for all units). Run with --availability once to generate
    references/dhs_availability.json; subsequent runs can omit the flag.

    python3 scripts/scrape_variables.py children --availability
"""

import argparse
import json
import sys
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.idhsdata.org/idhs-action/variables/group"
VAR_URL = "https://www.idhsdata.org/idhs-action/variables"

UNITS = {
    "women": "Women",
    "men": "Men",
    "children": "Children (under 5)",
    "household_members": "Household Members",
    "births": "Births",
}

OUTPUT_DIR = Path(__file__).parent.parent / "references"
AVAILABILITY_PATH = OUTPUT_DIR / "dhs_availability.json"


def make_session(unit: str) -> requests.Session:
    """Create a session with the unit-of-analysis cookie set."""
    session = requests.Session()
    session.headers["User-Agent"] = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
    )
    # The IPUMS DHS variable browser is stateful: the unit_of_analysis query
    # parameter on the first GET sets a server-side session cookie that scopes
    # all subsequent variable-group and detail requests to that unit. Without
    # this initial handshake, the site returns variables for the default unit
    # (Women) regardless of the unit requested in later calls.
    resp = session.get(BASE_URL, params={"unit_of_analysis": unit})
    resp.raise_for_status()
    print(f"  Session set for unit={unit!r} (status {resp.status_code})")
    return session


def discover_groups(session: requests.Session, unit: str) -> list[tuple[str, str]]:
    """
    Return all (group_id, group_label) pairs from the sidebar of the landing page.
    """
    resp = session.get(BASE_URL, params={"unit_of_analysis": unit})
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    seen = {}
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "variables/group?id=" in href:
            group_id = href.split("?id=")[-1]
            label = a.get_text(strip=True)
            if group_id not in seen and label:
                seen[group_id] = label

    groups = list(seen.items())
    print(f"  Discovered {len(groups)} topic groups")
    return groups


def _parse_variable_rows(soup: BeautifulSoup) -> list[dict]:
    """Parse all variable rows from a single page of soup."""
    variables = []
    for row in soup.find_all("tr", class_="variables"):
        name_td = row.find("td", class_="mbasic")
        label_td = row.find("td", class_="labelColumn")
        if not name_td:
            continue

        name = name_td.get_text(strip=True)

        # Check preselected BEFORE modifying the tree
        preselected = bool(label_td and label_td.find("strong"))

        if label_td:
            # Strip the [preselected] marker so it doesn't appear in the label text
            for strong in label_td.find_all("strong"):
                strong.decompose()
            label = label_td.get_text(strip=True)
        else:
            label = ""

        variables.append({"name": name, "label": label, "preselected": preselected})
    return variables


def scrape_group(session: requests.Session, group_id: str) -> list[dict]:
    """
    Scrape all pages of a topic group and return list of variable dicts:
      {name, label, preselected}

    Groups are paginated at 60 variables per page. Page URLs use the format:
      /variables/group/{group_id}?page=N
    Pagination ends when there is no <a class="next_page"> link.
    """
    variables = []
    page = 1
    while True:
        url = f"{BASE_URL}/{group_id}"
        resp = session.get(url, params={"page": page})
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        page_vars = _parse_variable_rows(soup)
        variables.extend(page_vars)

        # The site renders an <a class="next_page"> link only when a further
        # page exists. When it's absent we've consumed all 60-variable pages
        # for this group.
        if not soup.find("a", class_="next_page"):
            break
        page += 1
        time.sleep(0.3)

    return variables


def scrape_variable_availability(session: requests.Session, var_name: str) -> dict:
    """
    Fetch a variable's detail page and parse availability and DHS source variable name.

    Returns {"availability": {country_name: [year, ...]}, "dhs_source": "HW70" or None}.
    The dhs_source is parsed from <span id="inactive_var_name">(NAME)</span> after the <h1>.
    Each <li> in #availability_section has the format:
      "Kenya:\\n      1993-C, 1998-C, 2022-C\\n    "
    The "-C" suffix is the DHS survey phase code, not the unit — strip it.
    """
    resp = session.get(f"{VAR_URL}/{var_name}")
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    # Every IPUMS DHS variable detail page has two <h1> elements:
    #   1. <h1 class="projectSubtitle">Demographic and Health Surveys</h1>
    #      — the site-wide banner, present on every page.
    #   2. <h1>HWHAZWHO</h1>  (no class attribute)
    #      — the IPUMS variable name for this specific variable.
    # Selecting with soup.find("h1") always returns the first one (the banner),
    # so we must filter to the classless <h1> to get the variable name.
    dhs_source = None
    h1 = next((h for h in soup.find_all("h1") if not h.get("class")), None)
    if h1:
        # Immediately after the variable-name <h1>, IPUMS renders the original
        # DHS source variable name in a span: <span id="inactive_var_name">(HW70)</span>.
        # This is the DHS recode name (e.g. "HW70") that maps back to the
        # variable names listed in the Guide to DHS Statistics, allowing us to
        # cross-reference IPUMS names with Guide indicator definitions.
        sib = h1.find_next_sibling("span")
        if sib:
            text = sib.get_text(strip=True)
            if text.startswith("(") and text.endswith(")"):
                dhs_source = text[1:-1]

    # The #availability_section div contains an <ul> with one <li> per country.
    # Each item's text looks like "Kenya: 1993-C, 1998-C, 2022-C" where the
    # letter suffix after the hyphen is the DHS survey phase (not the unit).
    # We split on ":" to separate country from years, then strip the phase suffix
    # by taking only the numeric part before the "-".
    avail: dict[str, list[int]] = {}
    div = soup.find(id="availability_section")
    if div:
        for li in div.find_all("li"):
            text = li.get_text(separator=" ", strip=True)
            if ":" not in text:
                continue
            country, years_str = text.split(":", 1)
            country = country.strip()
            years = []
            for part in years_str.split(","):
                part = part.strip()
                if part:
                    year_str = part.split("-")[0].strip()
                    if year_str.isdigit():
                        years.append(int(year_str))
            if years:
                avail[country] = sorted(set(years))

    return {"availability": avail, "dhs_source": dhs_source}


def _format_availability_notes(avail: dict[str, list[int]]) -> str:
    """
    Format availability as a compact Notes column entry showing latest year per country.
    e.g. "KE: 2022 · GH: 2022 · NG: 2018 · ET: 2019"
    Uses a country-name-to-IPUMS-code table for the 20 most common DHS countries;
    falls back to the first 2 letters of the country name for others.
    """
    COUNTRY_CODES = {
        "Afghanistan": "AF", "Albania": "AL", "Angola": "AO", "Armenia": "AM",
        "Azerbaijan": "AZ", "Bangladesh": "BD", "Benin": "BJ", "Bolivia": "BO",
        "Burkina Faso": "BF", "Burundi": "BU", "Cambodia": "KH", "Cameroon": "CM",
        "Chad": "TD", "Colombia": "CO", "Comoros": "KM",
        "Congo (Democratic Republic)": "CD", "Congo Brazzaville": "CG",
        "Cote d'Ivoire": "CI", "Dominican Republic": "DO", "Egypt": "EG",
        "Eswatini (Swaziland)": "SZ", "Ethiopia": "ET", "Gabon": "GA",
        "Gambia": "GM", "Ghana": "GH", "Guatemala": "GT", "Guinea": "GN",
        "Guyana": "GY", "Haiti": "HT", "Honduras": "HN", "India": "IA",
        "Indonesia": "ID", "Jordan": "JO", "Kazakhstan": "KZ", "Kenya": "KE",
        "Kyrgyz Republic": "KG", "Lesotho": "LS", "Liberia": "LR",
        "Madagascar": "MG", "Malawi": "MW", "Maldives": "MV", "Mali": "ML",
        "Mauritania": "MR", "Morocco": "MA", "Mozambique": "MZ", "Myanmar": "MM",
        "Namibia": "NM", "Nepal": "NP", "Niger": "NE", "Nigeria": "NG",
        "Pakistan": "PK", "Papua New Guinea": "PG", "Peru": "PE", "Philippines": "PH",
        "Rwanda": "RW", "Senegal": "SN", "Sierra Leone": "SL", "South Africa": "ZA",
        "Sudan": "SD", "Tajikistan": "TJ", "Tanzania": "TZ", "Timor-Leste": "TL",
        "Togo": "TG", "Tunisia": "TN", "Turkey": "TR", "Uganda": "UG",
        "Ukraine": "UA", "Uzbekistan": "UZ", "Vietnam": "VN", "Yemen": "YE",
        "Zambia": "ZM", "Zimbabwe": "ZW",
    }
    parts = []
    for country in sorted(avail):
        years = avail[country]
        code = COUNTRY_CODES.get(country, country[:2].upper())
        parts.append(f"{code}: {max(years)}")
    return " · ".join(parts)


def _avail_for(entry: dict) -> dict:
    """Extract {country: [years]} from either old or new format entry."""
    if isinstance(entry.get("availability"), dict):
        return entry["availability"]
    return entry


def enrich_availability(
    session: requests.Session,
    all_groups: list[tuple[str, str, list[dict]]],
    existing: dict,
) -> dict:
    """
    Second pass: visit each variable's detail page to collect availability.
    Skips variables already in `existing` (loaded from dhs_availability.json).
    Returns updated availability dict merged with existing.
    """
    avail = dict(existing)

    # Collect unique variable names not yet in cache
    all_vars = []
    seen_names: set[str] = set()
    for _, _, variables in all_groups:
        for v in variables:
            name = v["name"]
            if name not in avail and name not in seen_names:
                all_vars.append(name)
                seen_names.add(name)

    total = len(all_vars)
    print(f"\n  Fetching availability for {total} variables (skipped {len(existing)} cached)...")
    if total > 1000:
        hrs = total * 0.6 / 3600
        print(f"  Estimated time: {hrs:.1f} hours at ~0.6s/variable")

    for i, var_name in enumerate(all_vars):
        if (i + 1) % 100 == 0:
            print(f"    [{i+1}/{total}] {var_name} ...", flush=True)
        try:
            avail[var_name] = scrape_variable_availability(session, var_name)
        except Exception as e:
            print(f"    WARNING: {var_name} availability fetch failed: {e}")
            avail[var_name] = {}
        time.sleep(0.3)

    return avail


def scrape_unit(unit: str, scrape_avail: bool = False) -> None:
    """Scrape all variables for one unit and write references/dhs_codebook_{unit}.md."""
    label = UNITS[unit]
    print(f"\nScraping unit: {label} ({unit})")

    session = make_session(unit)
    groups = discover_groups(session, unit)

    all_groups: list[tuple[str, str, list[dict]]] = []
    total_vars = 0

    for i, (group_id, group_label) in enumerate(groups):
        print(f"  [{i+1}/{len(groups)}] {group_id}: {group_label} ...", end=" ", flush=True)
        try:
            variables = scrape_group(session, group_id)
            all_groups.append((group_id, group_label, variables))
            total_vars += len(variables)
            print(f"{len(variables)} vars")
        except Exception as e:
            print(f"ERROR: {e}")
            all_groups.append((group_id, group_label, []))
        time.sleep(0.3)  # polite crawl delay

    # Optional availability second pass
    availability: dict = {}
    if scrape_avail:
        existing = {}
        if AVAILABILITY_PATH.exists():
            existing = json.loads(AVAILABILITY_PATH.read_text())
        availability = enrich_availability(session, all_groups, existing)
        AVAILABILITY_PATH.write_text(json.dumps(availability, sort_keys=True, indent=2))
        print(f"  Saved availability for {len(availability)} variables → {AVAILABILITY_PATH}")
    elif AVAILABILITY_PATH.exists():
        availability = json.loads(AVAILABILITY_PATH.read_text())

    # Write markdown
    output_path = OUTPUT_DIR / f"dhs_codebook_{unit}.md"
    has_avail = bool(availability)
    lines = [
        f"# IPUMS DHS Variables — {label}\n",
        f"Unit of analysis: `{unit}`  \n",
        f"Total variables: {total_vars}  \n",
        f"Topic groups: {len(groups)}  \n",
        "\n",
        "**[preselected]** = IPUMS includes this variable in every extract automatically.  \n",
    ]
    if has_avail:
        lines.append(
            "**Availability** column shows latest survey year per country "
            "(2-letter IPUMS code). Full data in references/dhs_availability.json.  \n"
        )
    lines.append("\n")

    for group_id, group_label, variables in all_groups:
        lines.append(f"## {group_label}\n")
        lines.append(f"Group ID: `{group_id}`\n\n")
        if not variables:
            lines.append("_No variables found or scrape error._\n\n")
            continue
        if has_avail:
            lines.append("| Variable | Label | Notes | Availability |\n")
            lines.append("|----------|-------|-------|---------------|\n")
        else:
            lines.append("| Variable | Label | Notes |\n")
            lines.append("|----------|-------|-------|\n")
        for v in variables:
            notes = "[preselected]" if v["preselected"] else ""
            if has_avail:
                avail_str = _format_availability_notes(_avail_for(availability.get(v["name"], {})))
                lines.append(f"| `{v['name']}` | {v['label']} | {notes} | {avail_str} |\n")
            else:
                lines.append(f"| `{v['name']}` | {v['label']} | {notes} |\n")
        lines.append("\n")

    output_path.write_text("".join(lines))
    print(f"\n  Written to {output_path} ({total_vars} variables across {len(groups)} groups)")


def main():
    parser = argparse.ArgumentParser(
        description="Scrape IPUMS DHS variable browser into markdown codebooks."
    )
    parser.add_argument(
        "unit",
        choices=list(UNITS.keys()) + ["all"],
        help="Unit of analysis to scrape, or 'all' for every unit.",
    )
    parser.add_argument(
        "--availability",
        action="store_true",
        help=(
            "Also scrape each variable's detail page to record country/year availability. "
            "Slow (~0.6s/variable). Results cached in references/dhs_availability.json."
        ),
    )
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if args.unit == "all":
        for unit in UNITS:
            scrape_unit(unit, scrape_avail=args.availability)
    else:
        scrape_unit(args.unit, scrape_avail=args.availability)


if __name__ == "__main__":
    main()
