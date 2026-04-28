"""
Step 2 of the DHS Stata indicator pipeline.

DATA FLOW
  references/dhs_stata_indicators.json  (built by build_stata_indicator_index.py)
      ↓  clone github.com/DHSProgram/DHS-Indicators-Stata
      ↓  parse .do files → variable dependency graph
      ↓  BFS per indicator → raw DHS recode variable names
      ↓  reverse-lookup via references/dhs_availability.json
  same JSON enriched with two new fields per indicator:
      dhs_variables   — DHS recode names (e.g. ['h3', 'h5', 'h7'])
      ipums_variables — IPUMS variable names (e.g. ['VACCDPT1', 'VACCDPT3'])

WHY PER-INDICATOR RATHER THAN PER-FILE
  Every indicator in a .do file shares the same raw DHS variables at the file
  level, which is useless for disambiguation. This script builds a variable
  *dependency graph* for each file and traces -- per indicator -- exactly which
  raw DHS source variables are needed to compute that specific output variable.

BFS DEPENDENCY TRACING
  DoFileParser builds two structures from each .do file:
    created_vars  -- all Stata variable names defined by gen/replace/recode/etc.
    deps[var]     -- set of identifier tokens from the RHS of that var's defining
                    lines (after loop expansion and comment stripping)

  get_dhs_vars(stata_var) runs a BFS from the indicator's own name:
    - token matches _DHS_VAR_RE AND not in created_vars → raw DHS input, add to result
    - token is in created_vars → Stata intermediate, push onto queue
    - anything else (keyword, number, function name) → skip

  Example chain for ch_pent3_either:
    ch_pent3_either → dptsum (intermediate) → dpt1/dpt2/dpt3 (intermediates)
    → h3/h5/h7 (DHS recode variables, not created in the file) ✓

DHS RECODE VARIABLE PATTERNS
  Three forms are recognised by _DHS_VAR_RE:
    Standard:      1-3 letters + digits + optional letters  (h7, hv201, mv463aa)
    Birth history: same + _N subscript                      (m2k_1, m14_1, b3_01)
    Reshape stubs: same + bare trailing underscore          (hml22_, hml23_)
      Reshape stubs arise from 'reshape long hml22_ hml23_': after reshape, the
      stub name becomes the working variable. Trailing _ is stripped before the
      IPUMS lookup (hml22_ → hml22).

DHS8 SUBFOLDER SCANNING
  The repo has root-level .do files for older indicators and DHS8/ subfolders
  with updated versions for newer DHS-8 indicators. build_file_index collects
  ALL versions of each stem into a list; DoFileParser accepts a list of paths
  and merges both dependency graphs. Both old and new indicators from the same
  logical file are resolved in a single parser pass.

SUM/SUMMARIZE/MEAN TRACKING
  Some indicators are computed as:
      summarize {dhs_var} [conditions]
      gen {indicator} = r(mean)           <- no direct variable reference
  The parser tracks the most recently summarized variable in _last_summarized
  and injects it as a dep whenever a gen RHS contains 'r(' (Stata stored
  estimation result syntax). This resolves chains like:
      nt_ch_mean_haz → haz (intermediate) → hc70 (DHS source)

WHY 187 INDICATORS REMAIN EMPTY
  After all parsing improvements, 187 indicators (~15%) still have no
  dhs_variables. These fall into structural categories unreachable by .do parsing:
    69  DV_PRTNR/DV_VIOL -- variables listed in the xlsx but never created with
        gen/replace in the .do files (likely in a separate analysis script)
    26  FE_* -- fertility rates (ASFR, TFR, CBR, medians) computed by life-table
        demographic methods, not gen statements
     8  AM_* -- maternal/adult mortality rates (same; known limitation)
    15  FP_DISCONT -- discontinuation rates computed by stcompet survival analysis,
        which produces no individually-named variables
    17  "not matching FR" -- bad do_file reference in IndicatorList.xlsx
    52  miscellaneous -- name mismatches between xlsx and code, program define
        with runtime macro-assembled names (PH_SCHOL), and other edge cases
"""
import json
import os
import re
import subprocess
import sys
from collections import deque
from pathlib import Path

REPO_URL = "https://github.com/DHSProgram/DHS-Indicators-Stata.git"
DEFAULT_LOCAL_CLONE = Path("/tmp/DHS-Indicators-Stata")

INDICATORS_PATH = Path(__file__).parent.parent / "references" / "dhs_stata_indicators.json"
AVAILABILITY_PATH = Path(__file__).parent.parent / "references" / "dhs_availability.json"

# DHS recode variable patterns:
#   Standard:      1-3 letters, digits, optional multi-letter suffix  (h7, hv201, mv463aa)
#   Birth history: same but with _N subscript                         (m2k_1, m14_1, b3_01)
#   Reshape stubs: same but with bare trailing _                      (hml22_, hml23_)
_DHS_VAR_RE = re.compile(r"^[a-z]{1,3}[0-9]+[a-z]*(_[0-9]*)?$")

# All identifier-like tokens in Stata expressions
_IDENT_RE = re.compile(r"\b([a-zA-Z_][a-zA-Z0-9_]*)\b")

# Stata keywords that appear as bare tokens and are not variable names
_STATA_KEYWORDS = {
    "if", "in", "else", "end", "to", "by", "bys", "bysort",
    "gen", "generate", "replace", "recode", "drop", "keep",
    "sort", "merge", "append", "use", "save", "import", "export",
    "label", "var", "val", "values", "define",
    "foreach", "forv", "forvalues", "while", "local", "global",
    "cap", "capture", "qui", "quietly", "noi", "noisily",
    "sum", "summarize", "tab", "table", "reg", "probit", "logit",
    "preserve", "restore", "compress", "ren", "rename",
    "macro", "scalar", "matrix", "display", "di",
    "inlist", "inrange", "missing", "mi", "cond",
    "round", "min", "max", "abs", "log", "exp", "sqrt", "int", "floor", "ceil",
    "iw", "pw", "fw", "aw", "pweight", "fweight", "aweight", "iweight",
    "clonevar", "xtile", "egen",
    # single-letter Stata results/functions that look like DHS prefixes
    "r", "e", "c",
}

# Prefix-stripping: cap/qui/noi and by/bys prefixes before a command
_PREFIX_RE = re.compile(
    r"^(?:cap(?:ture)?\s+|qui(?:etly)?\s+|noi(?:sily)?\s+|bys?(?:ort)?\s+\S[^:]*:\s*)*",
    re.IGNORECASE,
)

# Variable type spec that can follow 'gen': byte int long float double str{n}
_TYPE_RE = re.compile(r"^(?:byte|int|long|float|double|str\d*)\s+", re.IGNORECASE)


def _extract_idents(text: str) -> set:
    """Return lowercase identifier tokens from text, minus Stata keywords."""
    tokens = set()
    for tok in _IDENT_RE.findall(text):
        low = tok.lower()
        if low not in _STATA_KEYWORDS:
            tokens.add(low)
    return tokens


class DoFileParser:
    """
    Parses one or more Stata .do files into a merged variable dependency graph,
    then answers per-indicator DHS variable queries.

    Accepts a list of paths so that root-level and DHS8/ versions of the same
    file can both contribute to the graph — root defines older indicators,
    DHS8/ defines newer ones.

    created_vars:    set of all variables defined across all versions
    deps[var]:       union of identifier tokens from all versions' gen/replace/recode lines
    replace_conds:   varname -> {str_value -> [condition_str]} for 'replace VAR=N if COND'
    _raw_keepdrops:  list of raw {type, condition, description} before resolution
    """

    def __init__(self, paths):
        if isinstance(paths, Path):
            paths = [paths]
        self.created_vars: set = set()
        self.deps: dict = {}   # varname -> set[str]
        self.replace_conds: dict = {}  # varname -> {str_val -> [condition_str]}
        self._raw_keepdrops: list = []  # pre-resolution keep/drop entries
        # Tracks the variable most recently passed to sum/summarize/mean so that
        # a subsequent 'gen Y = r(mean)' can be linked back to its source.
        self._last_summarized: str = ""
        for path in paths:
            try:
                text = path.read_text(encoding="utf-8", errors="replace")
            except OSError:
                continue
            self._parse(text)

    # ------------------------------------------------------------------
    # Text preprocessing
    # ------------------------------------------------------------------

    @staticmethod
    def _remove_block_comments(text: str) -> str:
        return re.sub(r"/\*.*?\*/", " ", text, flags=re.DOTALL)

    @staticmethod
    def _join_continuations(text: str) -> str:
        """Replace Stata line-continuation marker /// with a space."""
        return re.sub(r"///[^\n]*\n", " ", text)

    @staticmethod
    def _strip_line_comment(line: str) -> str:
        """Remove // comments and lines whose first non-space char is *."""
        stripped = line.lstrip()
        if stripped.startswith("*"):
            return ""
        idx = line.find("//")
        return line[:idx] if idx >= 0 else line

    # ------------------------------------------------------------------
    # Loop expansion
    # ------------------------------------------------------------------

    @classmethod
    def _expand_loops(cls, lines: list) -> list:
        """Expand foreach and forvalues loops (single-level only).

        DHS .do files use loops like 'foreach c in a b c d ... { replace h12`c'==1 }'
        to iterate over sub-variable suffixes. Without expansion, `c' appears as a
        token and the actual variable names (h12a, h12b, ...) are never seen.
        """
        result = []
        i = 0
        while i < len(lines):
            line = lines[i]

            # foreach VAR in ITEMS {
            m = re.match(r"\s*foreach\s+(\w+)\s+in\s+(.*?)\s*\{", line, re.IGNORECASE)
            if m:
                loop_var, items_str = m.group(1), m.group(2)
                items = items_str.split()
                body, i = cls._collect_body(lines, i + 1)
                for item in items:
                    for bline in body:
                        result.append(bline.replace(f"`{loop_var}'", item))
                continue

            # forvalues VAR = START/END  or  START(STEP)END
            m = re.match(
                r"\s*forv(?:alues)?\s+(\w+)\s*=\s*(\d+)(?:/|(?:\((\d+)\)))(\d+)\s*\{",
                line,
                re.IGNORECASE,
            )
            if m:
                loop_var = m.group(1)
                start = int(m.group(2))
                step = int(m.group(3)) if m.group(3) else 1
                end = int(m.group(4))
                body, i = cls._collect_body(lines, i + 1)
                for val in range(start, end + 1, step):
                    for bline in body:
                        result.append(bline.replace(f"`{loop_var}'", str(val)))
                continue

            result.append(line)
            i += 1
        return result

    @staticmethod
    def _collect_body(lines: list, start: int):
        """Collect lines inside a {}-delimited block, return (body_lines, next_i)."""
        body = []
        depth = 1
        i = start
        while i < len(lines) and depth > 0:
            bline = lines[i]
            depth += bline.count("{") - bline.count("}")
            if depth > 0:
                body.append(bline)
            i += 1
        return body, i

    # ------------------------------------------------------------------
    # Command parsing
    # ------------------------------------------------------------------

    def _record(self, target: str, ref_text: str) -> None:
        """Add target to created_vars and merge identifier tokens into its deps."""
        target = target.lower()
        self.created_vars.add(target)
        deps = _extract_idents(ref_text)
        # gen Y = r(mean) / round(r(mean),...) — result of last sum/summarize/mean
        if self._last_summarized and "r(" in ref_text:
            deps.add(self._last_summarized)
        self.deps.setdefault(target, set()).update(deps)

    def _parse_line(self, raw: str) -> None:
        # Strip leading whitespace before applying _PREFIX_RE, which anchors at ^.
        # Without this, tab-indented 'cap gen h54=hv205' fails to match the prefix.
        line = _PREFIX_RE.sub("", raw.strip()).strip()
        if not line:
            return

        # gen [TYPE] target = expr [if cond]
        m = re.match(r"gen(?:erate)?\s+(.+)", line, re.IGNORECASE)
        if m:
            rest = _TYPE_RE.sub("", m.group(1))          # strip optional type
            eq = rest.find("=")
            if eq > 0 and rest[eq - 1] not in ("!", "<", ">"):  # skip !=, <=, >=
                target = rest[:eq].strip()
                rhs = rest[eq + 1 :].strip()
                # Skip 'cap gen VAR = .' — Stata idiom to initialize a raw DHS source
                # variable to missing as a fallback for older surveys that lack it.
                # Recording this would add the DHS variable to created_vars, causing
                # the BFS to treat it as a Stata intermediate and find nothing.
                if re.match(r"^\w+$", target) and rhs != ".":
                    self._record(target, rhs)
            return

        # replace target = expr [if cond]
        m = re.match(r"replace\s+(\w+)\s*=\s*(.+)", line, re.IGNORECASE)
        if m:
            var = m.group(1).lower()
            rhs = m.group(2)
            # Track 'replace VAR=N if COND' for universe restriction resolution.
            # Only store simple numeric values (e.g. agegroup=1) — skips expressions.
            if_m = re.search(r"\s+if\s+(.+)$", rhs, re.IGNORECASE)
            if if_m:
                val_part = rhs[: if_m.start()].strip()
                cond_part = if_m.group(1).strip()
                if re.match(r"^-?\d+(\.\d+)?$", val_part):
                    self.replace_conds.setdefault(var, {}).setdefault(val_part, []).append(cond_part)
            self._record(m.group(1), m.group(2))
            return

        # recode src (map) [if cond], gen(target)
        # The optional \s* around the parens handles 'gen (ph_sani_type)' (space before paren)
        # which appears in multi-line recode blocks joined by ///.
        m = re.match(r"recode\s+(\w+)(.*)\bgen\s*\(\s*(\w+)\s*\)", line, re.IGNORECASE | re.DOTALL)
        if m:
            src, middle, target = m.group(1), m.group(2), m.group(3)
            # Include src and any if-condition variables as deps
            if_m = re.search(r"\bif\s+(.+?)(?:,|$)", middle, re.IGNORECASE)
            ref = src + (" " + if_m.group(1) if if_m else "")
            self._record(target, ref)
            return

        # clonevar target = source
        m = re.match(r"clonevar\s+(\w+)\s*=\s*(\w+)", line, re.IGNORECASE)
        if m:
            self._record(m.group(1), m.group(2))
            return

        # xtile target = expr [if cond], nq(N)
        m = re.match(r"xtile\s+(\w+)\s*=\s*(.+?)(?:\s*,)", line, re.IGNORECASE)
        if m:
            self._record(m.group(1), m.group(2))
            return

        # egen target = func(expr) [if cond]
        m = re.match(r"egen\s+(\w+)\s*=\s*\w+\((.+?)\)", line, re.IGNORECASE)
        if m:
            self._record(m.group(1), m.group(2))
            return

        # sum/summarize/mean VAR — record the summarized variable so that a
        # following 'gen Y = r(mean)' can be linked back to it via _record().
        m = re.match(r"(?:summarize|sum|mean)\s+(\w+)\b", line, re.IGNORECASE)
        if m:
            self._last_summarized = m.group(1).lower()
            return

    def _parse(self, text: str) -> None:
        text = self._remove_block_comments(text)
        text = self._join_continuations(text)
        self._extract_universe(text)  # scan raw lines for keep/drop before stripping
        lines = [self._strip_line_comment(l) for l in text.split("\n")]
        lines = self._expand_loops(lines)
        for line in lines:
            self._parse_line(line)

    # ------------------------------------------------------------------
    # Universe restriction extraction
    # ------------------------------------------------------------------

    def _extract_universe(self, text: str) -> None:
        """Scan raw (pre-stripped) lines for keep if / drop if with preceding comments.

        Operates on text after block-comment removal and continuation-joining so that
        * comment lines are still visible (they become blank after _strip_line_comment).
        Loop expansion is applied so loop-body keep/drop statements are captured too.
        """
        lines = text.split("\n")
        lines = self._expand_loops(lines)

        prev_comment: str = ""
        for line in lines:
            stripped = line.strip()

            # * comment line — capture as potential description for following keep/drop
            if stripped.startswith("*"):
                comment_text = re.sub(r"^\*+\s*", "", stripped).strip()
                if comment_text:
                    prev_comment = comment_text
                continue

            # Separate inline // comment from the command
            idx = stripped.find("//")
            inline_comment = stripped[idx + 2:].strip() if idx >= 0 else ""
            clean = stripped[:idx].strip() if idx >= 0 else stripped

            if not clean:
                continue  # blank line — preserve prev_comment

            # Strip prefixes (cap/qui/noi/by) so 'cap keep if ...' is matched
            clean_cmd = _PREFIX_RE.sub("", clean).strip()

            m = re.match(r"^(keep|drop)\s+if\s+(.+)", clean_cmd, re.IGNORECASE)
            if m:
                type_ = m.group(1).lower()
                condition = m.group(2).strip()
                description = inline_comment or prev_comment or None
                self._raw_keepdrops.append({
                    "type": type_,
                    "condition": condition,
                    "description": description,
                })
                # Don't reset prev_comment: adjacent keep if lines share a comment header
            else:
                prev_comment = ""  # non-keep/drop, non-comment resets context

    def _resolve_condition(self, condition: str) -> str:
        """Resolve intermediate variable == N patterns to underlying DHS conditions.

        Example: 'agegroup==1' becomes '(b19>=12 & b19<=23)' when the parser has
        seen 'replace agegroup=1 if b19>=12 & b19<=23'.

        Prefers candidates whose conditions contain DHS variable patterns (e.g. b19)
        over those referencing further Stata intermediates (e.g. age). Recurses one
        additional level so a two-hop chain (agegroup → age → b19) is fully resolved.
        """
        def _has_dhs_var(s: str) -> bool:
            return any(_DHS_VAR_RE.match(tok.lower()) for tok in _IDENT_RE.findall(s))

        def try_resolve(m):
            var = m.group(1).lower()
            val = m.group(2)
            if var not in self.replace_conds:
                return m.group(0)
            candidates = self.replace_conds[var].get(val, [])
            if not candidates:
                return m.group(0)
            dhs_cands = [c for c in candidates if _has_dhs_var(c)]
            chosen = dhs_cands[-1] if dhs_cands else candidates[-1]
            # One recursive pass to resolve any remaining intermediates
            further = re.sub(r"\b([a-zA-Z_]\w*)\s*==\s*(-?\d+)\b", try_resolve, chosen)
            return f"({further})"

        return re.sub(r"\b([a-zA-Z_]\w*)\s*==\s*(-?\d+)\b", try_resolve, condition)

    def get_universe_restrictions(self) -> list:
        """Return deduplicated, resolved universe restrictions for this .do file.

        All keep if / drop if statements are file-level and therefore shared by
        every indicator computed by that file.
        """
        seen: set = set()
        result = []
        for entry in self._raw_keepdrops:
            resolved = self._resolve_condition(entry["condition"])
            key = (entry["type"], resolved)
            if key in seen:
                continue
            seen.add(key)
            result.append({
                "type": entry["type"],
                "condition": resolved,
                "description": entry["description"],
            })
        return result

    # ------------------------------------------------------------------
    # Per-indicator DHS variable lookup
    # ------------------------------------------------------------------

    def get_dhs_vars(self, stata_var: str) -> list:
        """
        Return sorted list of DHS recode variable names that transitively feed
        into computing stata_var, tracing through Stata intermediates.
        """
        visited: set = set()
        queue = deque([stata_var.lower()])
        dhs_vars: set = set()

        while queue:
            var = queue.popleft()
            if var in visited:
                continue
            visited.add(var)

            for token in self.deps.get(var, set()):
                if token in visited:
                    continue
                if _DHS_VAR_RE.match(token) and token not in self.created_vars:
                    dhs_vars.add(token)
                elif token in self.created_vars:
                    queue.append(token)
                # else: Stata keyword, number literal, or other noise → skip

        return sorted(dhs_vars)


# ------------------------------------------------------------------
# Repo / file utilities (unchanged from v1)
# ------------------------------------------------------------------

def get_repo(local_path: Path) -> Path:
    if local_path.exists() and (local_path / ".git").exists():
        print(f"Using existing clone at {local_path}")
        return local_path
    print(f"Cloning {REPO_URL} -> {local_path} ...")
    subprocess.run(["git", "clone", "--depth=1", REPO_URL, str(local_path)], check=True)
    return local_path


def build_file_index(repo_root: Path) -> dict:
    """Map lowercase stem -> list[Path] for every .do file in the repo.

    Returns ALL versions (root-level and DHS8/ subfolder) under the same key
    so DoFileParser can merge both into one dependency graph. Root files define
    older indicators; DHS8/ files add newer DHS-8-only indicators. Both must be
    parsed together so that a single parser instance covers the full indicator set
    for that chapter.
    """
    index = {}
    for path in repo_root.rglob("*.do"):
        index.setdefault(path.stem.lower(), []).append(path)
    return index


def resolve_do_file(do_file_raw, file_index: dict):
    """Return list[Path] for the do file, or None if not found.

    Falls back to longest shared prefix when the xlsx name doesn't exactly match
    the repo filename (e.g. 'CM_RISK_women' → 'CM_RISK_WM.do').
    Minimum prefix length of 7 avoids false matches between short stems.
    """
    if not do_file_raw:
        return None
    stem = Path(do_file_raw).stem.lower()
    if stem in file_index:
        return file_index[stem]
    # Prefix-match fallback
    best, best_len = None, 6
    for key in file_index:
        n = 0
        for a, b in zip(stem, key):
            if a == b:
                n += 1
            else:
                break
        if n > best_len:
            best_len, best = n, file_index[key]
    return best


def build_dhs_to_ipums(availability: dict) -> dict:
    """Reverse map: uppercase DHS source name -> list of IPUMS variable names."""
    reverse = {}
    for ipums_var, meta in availability.items():
        src = meta.get("dhs_source", "")
        if src and isinstance(src, str):
            reverse.setdefault(src.upper(), []).append(ipums_var)
    return reverse


def second_pass_inherit(indicators, parser_cache, file_index, dhs_to_ipums):
    """
    For indicators still empty after the initial BFS, inherit dhs_variables from
    any known indicators encountered in their dep graph traversal.

    Example: ch_pent3_moth deps include ch_pent3_either (another indicator that
    already has dhs_variables). This pass copies those vars across.

    Runs repeatedly until no further changes are possible (handles chains).
    """
    ind_by_lower = {ind["stata_var"].lower(): ind for ind in indicators}

    def _ipums_for(dhs_vars):
        seen, result = set(), []
        for dv in dhs_vars:
            for iv in dhs_to_ipums.get(dv.rstrip("_").upper(), []):
                if iv not in seen:
                    seen.add(iv)
                    result.append(iv)
        return sorted(result)

    changed = True
    n_passes = 0
    while changed:
        changed = False
        n_passes += 1
        for indicator in indicators:
            stata_var = indicator["stata_var"]
            do_file_raw = indicator.get("do_file")
            if not do_file_raw:
                continue
            resolved = resolve_do_file(do_file_raw, file_index)
            if resolved is None:
                continue
            path_key = tuple(sorted(str(p) for p in resolved))
            parser = parser_cache.get(path_key)
            if parser is None:
                continue

            # BFS through dep graph; when we hit a known indicator that has
            # dhs_variables, collect them rather than recursing further into it.
            visited: set = set()
            queue = deque([stata_var.lower()])
            inherited: set = set()

            while queue:
                var = queue.popleft()
                if var in visited:
                    continue
                visited.add(var)
                for token in parser.deps.get(var, set()):
                    if token in visited:
                        continue
                    other = ind_by_lower.get(token)
                    if other and other.get("dhs_variables"):
                        inherited.update(other["dhs_variables"])
                        # Don't recurse into the indicator — its vars are already resolved
                    elif token in parser.created_vars:
                        queue.append(token)

            if not inherited:
                continue

            existing = set(indicator.get("dhs_variables", []))
            merged = existing | inherited
            if merged != existing:
                indicator["dhs_variables"] = sorted(merged)
                indicator["ipums_variables"] = _ipums_for(indicator["dhs_variables"])
                changed = True

    return n_passes


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

# Indicators to include in before/after comparison printout
COMPARE_VARS = [
    "ch_pent3_either",
    "ch_diar_antib",
    "nt_ch_stunt",
    "fp_cruse_pill",
    "we_decide_all",
]


def main():
    local_clone = Path(os.environ.get("DHS_STATA_REPO", str(DEFAULT_LOCAL_CLONE)))
    repo_root = get_repo(local_clone)

    with open(INDICATORS_PATH) as f:
        indicators = json.load(f)

    with open(AVAILABILITY_PATH) as f:
        availability = json.load(f)

    file_index = build_file_index(repo_root)
    dhs_to_ipums = build_dhs_to_ipums(availability)

    # Compute per-file DHS var sets for comparison (old approach)
    _file_dhs_cache: dict = {}

    def _per_file_dhs(do_file_raw: str) -> list:
        """Old approach: every DHS-pattern token in the entire .do file."""
        if do_file_raw not in _file_dhs_cache:
            resolved = resolve_do_file(do_file_raw, file_index)
            if resolved is None:
                _file_dhs_cache[do_file_raw] = []
            else:
                tokens: set = set()
                for path in resolved:
                    try:
                        text = path.read_text(encoding="utf-8", errors="replace")
                    except OSError:
                        continue
                    tokens.update(re.findall(r"\b([a-z]{1,3}[0-9]+[a-z]?)\b", text))
                _file_dhs_cache[do_file_raw] = sorted(tokens)
        return _file_dhs_cache[do_file_raw]

    # Cache DoFileParser instances per resolved path
    parser_cache: dict = {}

    for indicator in indicators:
        do_file_raw = indicator.get("do_file")
        stata_var = indicator.get("stata_var", "")

        if not do_file_raw or not stata_var:
            indicator["dhs_variables"] = []
            indicator["ipums_variables"] = []
            continue

        resolved = resolve_do_file(do_file_raw, file_index)
        if resolved is None:
            indicator["dhs_variables"] = []
            indicator["ipums_variables"] = []
            continue

        path_key = tuple(sorted(str(p) for p in resolved))
        if path_key not in parser_cache:
            parser_cache[path_key] = DoFileParser(resolved)

        parser = parser_cache[path_key]
        dhs_vars = parser.get_dhs_vars(stata_var)

        ipums_seen: set = set()
        ipums_vars = []
        for dv in dhs_vars:
            # Reshape stub vars (hml22_) have a trailing _ not in the IPUMS name.
            dv_key = dv.rstrip("_").upper()
            for iv in dhs_to_ipums.get(dv_key, []):
                if iv not in ipums_seen:
                    ipums_seen.add(iv)
                    ipums_vars.append(iv)

        indicator["dhs_variables"] = dhs_vars
        indicator["ipums_variables"] = sorted(ipums_vars)

    n_after_pass1 = sum(1 for ind in indicators if ind.get("dhs_variables"))

    # Second pass: inherit dhs_variables from other indicators in the dep chain
    n_passes = second_pass_inherit(indicators, parser_cache, file_index, dhs_to_ipums)
    n_after_pass2 = sum(1 for ind in indicators if ind.get("dhs_variables"))

    # Extract universe restrictions for every indicator from its .do file parser.
    # Restrictions are file-level (keep/drop statements apply to the whole file),
    # so all indicators sharing a do_file get the same list.
    for indicator in indicators:
        do_file_raw = indicator.get("do_file")
        if not do_file_raw:
            indicator["universe_restrictions"] = []
            continue
        resolved = resolve_do_file(do_file_raw, file_index)
        if resolved is None:
            indicator["universe_restrictions"] = []
            continue
        path_key = tuple(sorted(str(p) for p in resolved))
        parser = parser_cache.get(path_key)
        if parser is None:
            indicator["universe_restrictions"] = []
            continue
        indicator["universe_restrictions"] = parser.get_universe_restrictions()

    with open(INDICATORS_PATH, "w") as f:
        json.dump(indicators, f, indent=2)

    # Summary stats
    all_dhs: set = set()
    all_ipums: set = set()
    unresolved: set = set()
    for ind in indicators:
        all_dhs.update(ind.get("dhs_variables", []))
        all_ipums.update(ind.get("ipums_variables", []))
        if ind.get("do_file") and not ind.get("dhs_variables"):
            resolved_check = resolve_do_file(ind["do_file"], file_index)
            if not resolved_check:
                unresolved.add(ind["do_file"])

    n_empty = sum(1 for ind in indicators if not ind.get("dhs_variables"))

    print(f"\nTotal indicators:               {len(indicators)}")
    print(f"  Pass 1 (BFS + new regex):     {n_after_pass1} with DHS vars")
    print(f"  Pass 2 (inheritance, {n_passes} iter): +{n_after_pass2 - n_after_pass1} more")
    print(f"  Still empty:                  {n_empty}")
    print(f"Unique DHS variables found:     {len(all_dhs)}")
    print(f"Mapped to IPUMS names:          {len(all_ipums)}")
    if unresolved:
        print(f"\nDo files not found in repo ({len(unresolved)}):")
        for f in sorted(unresolved):
            print(f"  {f}")

    # Before/after comparison
    print("\n" + "=" * 70)
    print("BEFORE / AFTER comparison (per-file → per-indicator)")
    print("=" * 70)
    for sv in COMPARE_VARS:
        ind = next((x for x in indicators if x["stata_var"] == sv), None)
        if ind is None:
            continue
        old = _per_file_dhs(ind.get("do_file", ""))
        new = ind.get("dhs_variables", [])
        print(f"\n{sv}  [{ind['do_file']}]")
        print(f"  BEFORE ({len(old):2d}): {old}")
        print(f"  AFTER  ({len(new):2d}): {new}")

    # Universe restrictions summary
    n_with_restrictions = sum(1 for ind in indicators if ind.get("universe_restrictions"))
    print("\n" + "=" * 70)
    print(f"UNIVERSE RESTRICTIONS: {n_with_restrictions} indicators have restrictions")
    print("=" * 70)

    print("\n5 example indicators with restrictions:")
    examples = [ind for ind in indicators if ind.get("universe_restrictions")][:5]
    for ind in examples:
        print(f"  {ind['stata_var']}: {ind['label']}")
        for r in ind["universe_restrictions"]:
            desc = f"  [{r['description']}]" if r["description"] else ""
            print(f"    {r['type']} if {r['condition']}{desc}")

    print("\n5 do files where keep/drop statements were found:")
    seen_do_files: dict = {}
    for ind in indicators:
        for _r in ind.get("universe_restrictions", []):
            df = ind.get("do_file", "")
            if df and df not in seen_do_files:
                sample = ind.get("universe_restrictions", [])
                seen_do_files[df] = sample
            if len(seen_do_files) >= 5:
                break
        if len(seen_do_files) >= 5:
            break
    for df, restrictions in seen_do_files.items():
        conds = "; ".join(f"{r['type']} if {r['condition']}" for r in restrictions[:2])
        print(f"  {df}: {conds}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
