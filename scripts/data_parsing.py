import json
import os
import re
import time
from difflib import SequenceMatcher

import pandas as pd
from textblob import TextBlob

# This file lives in scripts/; repo root (Excel, CSV, columns_info) is one level up.
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)

# LCA Excel sources and outputs are in the project root
source_files = [
    os.path.join(PROJECT_DIR, 'LCA_Disclosure_Data_FY2025_Q1.xlsx'),
    os.path.join(PROJECT_DIR, 'LCA_Disclosure_Data_FY2025_Q2.xlsx'),
    os.path.join(PROJECT_DIR, 'LCA_Disclosure_Data_FY2025_Q3.xlsx'),
    os.path.join(PROJECT_DIR, 'LCA_Disclosure_Data_FY2025_Q4.xlsx'),
    os.path.join(PROJECT_DIR, 'LCA_Disclosure_Data_FY2026_Q1.xlsx'),
]

target_file = os.path.join(PROJECT_DIR, 'parsed_output.csv')
# Optional audit logs (gitignored via input-docs/)
INPUT_DOCS_DIR = os.path.join(PROJECT_DIR, "input-docs")
os.makedirs(INPUT_DOCS_DIR, exist_ok=True)
JOB_TITLE_CHANGES_LOG = os.path.join(INPUT_DOCS_DIR, "job_title_changes.json")
EMPLOYER_NAME_CHANGES_LOG = os.path.join(INPUT_DOCS_DIR, "employer_name_changes.json")
COLUMNS_INFO_FILE = os.path.join(PROJECT_DIR, 'columns_info.txt')

# Output columns: only those listed in columns_info.txt (exact names and order).
# Regardless of future Excel schema changes, parse and output only these columns.
with open(COLUMNS_INFO_FILE) as f:
    OUTPUT_COLUMNS = [line.strip() for line in f if line.strip()]

# Excel column -> Output column name
# Use original Excel column names everywhere; only H-1B_DEPENDENT/H_1B_DEPENDENT normalized
COLUMN_MAPPING = {
    'H-1B_DEPENDENT': 'H_1B_DEPENDENT',
    'H_1B_DEPENDENT': 'H_1B_DEPENDENT',
}

# Date columns in output (format as YYYY-MM-DD, no timestamp)
DATE_COLUMNS = [
    'RECEIVED_DATE',
    'DECISION_DATE',
    'BEGIN_DATE',
    'END_DATE',
]


def format_date_only(series):
    """Convert to date-only string (YYYY-MM-DD), strip any timestamp."""
    return pd.to_datetime(series, errors='coerce').dt.strftime('%Y-%m-%d').fillna('')


# Preserve Microsoft .NET skill token through punctuation stripping (must be letters-only for clean_field).
_DOTNET_PLACEHOLDER = "z9xdnetmkz9x"


def _mask_dotnet(text: str) -> str:
    """Replace .net (case-insensitive) so the dot is not stripped as punctuation."""
    return re.sub(r"(?i)\.net", _DOTNET_PLACEHOLDER, text)


def _unmask_dotnet(text: str) -> str:
    return text.replace(_DOTNET_PLACEHOLDER, ".net")


def normalize_title_text(title):
    """Normalize title text for similarity and spell-correction comparisons.

    Keeps & and .net consistent with clean_field / clean_job_title_field.
    """
    text = str(title).lower().strip()
    text = _mask_dotnet(text)
    text = re.sub(r"[^a-z0-9\s&]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    text = _unmask_dotnet(text)
    return text


def remove_suffix_for_similarity(title):
    """Drop simple trailing level suffixes so near-identical titles are not merged by level only."""
    return re.sub(r'\s+(i{1,3}|iv|v|vi{0,3}|[0-9]+|[a-z]|l[1-4])$', '', title).strip()


def find_similar_job_title_pairs(job_titles, min_similarity=0.97):
    """Find adjacent sorted job title pairs that are extremely similar."""
    unique_titles = sorted({
        str(title).strip()
        for title in job_titles
        if not pd.isna(title) and str(title).strip()
    })
    similar_pairs = []
    for idx in range(len(unique_titles) - 1):
        title_1 = unique_titles[idx]
        title_2 = unique_titles[idx + 1]
        base_1 = remove_suffix_for_similarity(title_1)
        base_2 = remove_suffix_for_similarity(title_2)
        if base_1 == base_2 and title_1 != title_2:
            continue
        similarity = SequenceMatcher(None, title_1, title_2).ratio()
        if similarity >= min_similarity and title_1 != title_2:
            similar_pairs.append((title_1, title_2, similarity))
    return similar_pairs


def choose_canonical_job_title(cluster_titles, title_counts, correction_cache):
    """Pick the best existing title in a similar-title cluster.

    TextBlob is used only to identify which existing title looks most correct.
    If spell-correction does not clearly favor an existing title, the most frequent
    title wins so we avoid inventing a new label.
    """
    support_counts = {title: 0 for title in cluster_titles}
    self_correcting_titles = []

    for title in cluster_titles:
        corrected = correction_cache.setdefault(title, normalize_title_text(str(TextBlob(title).correct())))
        if corrected == title:
            self_correcting_titles.append(title)
        if corrected in support_counts:
            support_counts[corrected] += 1

    candidate_pool = self_correcting_titles or list(cluster_titles)
    return max(
        candidate_pool,
        key=lambda title: (
            support_counts.get(title, 0),
            title_counts.get(title, 0),
            -len(title),
            title,
        ),
    )


def build_job_title_normalization_map(job_title_series, min_similarity=0.97):
    """Build old_title -> canonical_title mapping for near-duplicate titles."""
    titles = [
        str(title).strip()
        for title in job_title_series
        if not pd.isna(title) and str(title).strip()
    ]
    if not titles:
        return {}

    title_counts = pd.Series(titles).value_counts().to_dict()
    similar_pairs = find_similar_job_title_pairs(titles, min_similarity=min_similarity)
    if not similar_pairs:
        return {}

    parents = {title: title for title in title_counts}

    def find(title):
        while parents[title] != title:
            parents[title] = parents[parents[title]]
            title = parents[title]
        return title

    def union(left, right):
        left_root = find(left)
        right_root = find(right)
        if left_root != right_root:
            parents[right_root] = left_root

    for title_1, title_2, _similarity in similar_pairs:
        union(title_1, title_2)

    clusters = {}
    for title in title_counts:
        clusters.setdefault(find(title), []).append(title)

    correction_cache = {}
    normalization_map = {}
    for cluster_titles in clusters.values():
        if len(cluster_titles) < 2:
            continue
        canonical_title = choose_canonical_job_title(cluster_titles, title_counts, correction_cache)
        for title in cluster_titles:
            if title != canonical_title:
                normalization_map[title] = canonical_title

    return normalization_map


def find_similar_employer_name_pairs(employer_names, min_similarity=0.97):
    """Find adjacent sorted employer name pairs that are extremely similar."""
    unique_names = sorted({
        str(name).strip()
        for name in employer_names
        if not pd.isna(name) and str(name).strip()
    })
    similar_pairs = []
    for idx in range(len(unique_names) - 1):
        name_1 = unique_names[idx]
        name_2 = unique_names[idx + 1]
        similarity = SequenceMatcher(None, name_1, name_2).ratio()
        if similarity >= min_similarity and name_1 != name_2:
            similar_pairs.append((name_1, name_2, similarity))
    return similar_pairs


def choose_canonical_employer_name(cluster_names, name_counts, correction_cache):
    """Pick the best existing employer name in a similar-name cluster.

    TextBlob is used only to identify which existing name looks most correct.
    If spell-correction does not clearly favor an existing name, the most frequent
    name wins so we avoid inventing a new label.
    """
    support_counts = {name: 0 for name in cluster_names}
    self_correcting_names = []

    for name in cluster_names:
        corrected = correction_cache.setdefault(name, normalize_title_text(str(TextBlob(name).correct())))
        if corrected == name:
            self_correcting_names.append(name)
        if corrected in support_counts:
            support_counts[corrected] += 1

    candidate_pool = self_correcting_names or list(cluster_names)
    return max(
        candidate_pool,
        key=lambda name: (
            support_counts.get(name, 0),
            name_counts.get(name, 0),
            -len(name),
            name,
        ),
    )


def build_employer_name_normalization_map(employer_name_series, min_similarity=0.97):
    """Build old_name -> canonical_name mapping for near-duplicate employer names."""
    names = [
        str(name).strip()
        for name in employer_name_series
        if not pd.isna(name) and str(name).strip()
    ]
    if not names:
        return {}

    name_counts = pd.Series(names).value_counts().to_dict()
    similar_pairs = find_similar_employer_name_pairs(names, min_similarity=min_similarity)
    if not similar_pairs:
        return {}

    parents = {name: name for name in name_counts}

    def find(name):
        while parents[name] != name:
            parents[name] = parents[parents[name]]
            name = parents[name]
        return name

    def union(left, right):
        left_root = find(left)
        right_root = find(right)
        if left_root != right_root:
            parents[right_root] = left_root

    for name_1, name_2, _similarity in similar_pairs:
        union(name_1, name_2)

    clusters = {}
    for name in name_counts:
        clusters.setdefault(find(name), []).append(name)

    correction_cache = {}
    normalization_map = {}
    for cluster_names in clusters.values():
        if len(cluster_names) < 2:
            continue
        canonical_name = choose_canonical_employer_name(cluster_names, name_counts, correction_cache)
        for name in cluster_names:
            if name != canonical_name:
                normalization_map[name] = canonical_name

    return normalization_map


def remove_soc_from_job_title(job_title, soc_code):
    """Remove SOC code (and variants with spaces/dots/dashes) from job title when present.
    E.g. job_title='13 1081 02 logistics analysts', soc_code='13-1081.02' -> 'logistics analysts'
    """
    if pd.isna(job_title) or not str(job_title).strip():
        return job_title
    if pd.isna(soc_code) or not str(soc_code).strip():
        return job_title
    # Extract digit groups from SOC: "13-1081.02" -> ["13", "1081", "02"]
    parts = re.findall(r'\d+', str(soc_code))
    if not parts:
        return job_title
    # Build pattern to match SOC in job title (flexible whitespace: space, dot, dash normalized)
    # In job title after clean_field: "13 1081 02" (spaces). Match that sequence.
    pattern = r'\s*'.join(re.escape(p) for p in parts)
    # Remove the SOC pattern (with optional surrounding spaces) - from start, end, or middle
    text = str(job_title).strip()
    cleaned = re.sub(r'\s*' + pattern + r'\s*', ' ', text)
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    return cleaned


def clean_job_title_noise(title):
    """Remove trailing noise (IDs, codes) from job title. Preserves roman numerals and short levels (I, II, 2, 3)."""
    if pd.isna(title):
        return ''
    if not isinstance(title, str):
        title = str(title)
    text = str(title).strip()
    # 1. Two or more trailing numeric tokens -> remove all. Single trailing number kept (e.g. "data engineer 1").
    #    Also remove when followed by short alpha suffix (e.g. "118 2788 11 nc" -> remove " 118 2788 11 nc").
    #    Example: "senior specialty software engineer 017040 000902" -> remove " 017040 000902"
    text = re.sub(r'(\s+\d[\d.]*){2,}(\s+[a-z]{1,4})?\s*$', '', text)
    # 2. Trailing KBGFJG / KBOEYTEST codes: " kbgfjg191985 7", " kbgfjg132568 11"
    text = re.sub(r'\s+kbgfjg[a-z0-9]+(\s+\d+)?\s*$', '', text, flags=re.I)
    text = re.sub(r'\s+kboeytest[a-z0-9]+(\s+\d+)?\s*$', '', text, flags=re.I)
    # 3. Trailing long numeric IDs (5+ digits): " 00085767", " 00084644", " 017040"
    text = re.sub(r'\s+\d{5,}\s*$', '', text)
    # 4. Trailing decimal IDs: " 017040.001814", " 118.3936.3"
    text = re.sub(r'\s+\d{2,}\.\d[\d.]*\s*$', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


start_time = time.perf_counter()

# ========== PHASE 1: Header analysis ==========
print("Phase 1: Analyzing Excel file headers...")
existing_files = [f for f in source_files if os.path.exists(f)]
if not existing_files:
    raise FileNotFoundError(f"No Excel files found. Expected in: {PROJECT_DIR}")

file_headers = {}
for source_file in existing_files:
    try:
        headers = pd.read_excel(source_file, nrows=0).columns.tolist()
        file_headers[os.path.basename(source_file)] = headers
        print(f"  {os.path.basename(source_file)}: {len(headers)} columns")
    except Exception as e:
        print(f"  {os.path.basename(source_file)}: ERROR - {e}")
        raise

# Compare headers - treat H-1B_DEPENDENT/H_1B_DEPENDENT as same, LAWFIRM_BUSINESS_FEIN as optional
def normalize_col(c):
    return 'H-1B_DEPENDENT' if c == 'H_1B_DEPENDENT' else c

OPTIONAL_COLS = frozenset(['LAWFIRM_BUSINESS_FEIN'])

def core_headers(headers):
    return set(normalize_col(c) for c in headers) - OPTIONAL_COLS

basenames = list(file_headers.keys())
ref_core = core_headers(file_headers[basenames[0]])
header_mismatches = []

for fname in basenames[1:]:
    curr_core = core_headers(file_headers[fname])
    if curr_core != ref_core:
        header_mismatches.append({
            'file': fname,
            'missing': ref_core - curr_core,
            'extra': curr_core - ref_core,
        })

if header_mismatches:
    print("\n" + "=" * 60)
    print("HEADER MISMATCH DETECTED - Cannot proceed with parsing.")
    print("=" * 60)
    for m in header_mismatches:
        print(f"\n--- {m['file']} ---")
        if m['missing']:
            print(f"  Missing: {list(m['missing'])[:10]}")
        if m['extra']:
            print(f"  Extra: {list(m['extra'])[:10]}")
    print("\nPlease check the files manually.")
    raise SystemExit(1)

# Build canonical column set (union of all, normalized) - preserve order from first file
first_headers = file_headers[basenames[0]]
canonical_cols = []
seen = set()
for c in first_headers:
    nc = normalize_col(c)
    if nc not in seen:
        canonical_cols.append(nc)
        seen.add(nc)
for h in file_headers.values():
    for c in h:
        nc = normalize_col(c)
        if nc not in seen:
            canonical_cols.append(nc)
            seen.add(nc)

print(f"\nAll {len(basenames)} files have compatible headers.")
print("  (H-1B_DEPENDENT/H_1B_DEPENDENT aliased; LAWFIRM_BUSINESS_FEIN optional)")
print("Proceeding with parsing...\n")

# Build excel_col -> output_col mapping
rename_map = {}
for excel_col in canonical_cols:
    if excel_col in COLUMN_MAPPING:
        rename_map[excel_col] = COLUMN_MAPPING[excel_col]
    elif excel_col == 'H-1B_DEPENDENT':
        rename_map[excel_col] = 'H_1B_DEPENDENT'
    else:
        rename_map[excel_col] = excel_col

# Columns we need for filtering
canonical_set = set(canonical_cols)
for req in ['CASE_STATUS', 'VISA_CLASS', 'PW_UNIT_OF_PAY']:
    if req not in canonical_set:
        raise ValueError(f"Required column '{req}' not found. Available: {canonical_cols[:15]}...")

# ========== PHASE 2: Parse files ==========
print("Phase 2: Reading and parsing...")
data_frames = []
for source_file in existing_files:
    frame = pd.read_excel(source_file, dtype=str)
    # Normalize H_1B_DEPENDENT -> H-1B_DEPENDENT for consistent column names before rename
    if 'H_1B_DEPENDENT' in frame.columns and 'H-1B_DEPENDENT' not in frame.columns:
        frame = frame.rename(columns={'H_1B_DEPENDENT': 'H-1B_DEPENDENT'})
    # Add missing columns as empty (e.g. LAWFIRM_BUSINESS_FEIN in some files)
    for col in canonical_cols:
        if col not in frame.columns:
            frame[col] = ''
    # Select in canonical order, then rename (only H-1B_DEPENDENT -> H_1B_DEPENDENT; others keep Excel names)
    frame = frame[canonical_cols].copy()
    frame = frame.rename(columns=rename_map)
    data_frames.append(frame)
    print(f"  Read {len(frame)} rows from {os.path.basename(source_file)}")

data = pd.concat(data_frames, ignore_index=True)

print(f"Total rows read: {len(data)}")

# Filter: CASE_STATUS = "Certified", VISA_CLASS = "H-1B", PW_UNIT_OF_PAY = "Year"
status_col = rename_map.get('CASE_STATUS', 'CASE_STATUS')
filtered_data = data[
    (data[status_col].astype(str).str.strip().str.lower() == 'certified') &
    (data['VISA_CLASS'].astype(str).str.strip().str.upper() == 'H-1B') &
    (data['PW_UNIT_OF_PAY'].astype(str).str.strip().str.lower() == 'year')
]

print(f"Found {len(filtered_data)} matching rows after filtering")

# Select columns for output (from columns_info.txt)
output_data = filtered_data.copy()
for col in OUTPUT_COLUMNS:
    if col not in output_data.columns:
        output_data[col] = ''
output_data = output_data[OUTPUT_COLUMNS]

# Ensure CASE_NUMBER is first (original Excel column name)
if 'CASE_NUMBER' not in output_data.columns:
    output_data.insert(0, 'CASE_NUMBER', filtered_data['CASE_NUMBER'].values)
elif output_data.columns[0] != 'CASE_NUMBER':
    cols = ['CASE_NUMBER'] + [c for c in output_data.columns if c != 'CASE_NUMBER']
    output_data = output_data[cols]

# Format date columns as YYYY-MM-DD (no timestamp)
for col in DATE_COLUMNS:
    if col in output_data.columns:
        output_data[col] = format_date_only(output_data[col])

# Normalize country name for readability and space (replaces across all columns)
output_data = output_data.replace({
    'UNITED STATES OF AMERICA': 'USA',
    'United States of America': 'USA',
})

# ========== PHASE 3: Data quality ==========
print("Phase 3: Applying data quality rules...")

# Fill missing JOB_TITLE from SOC_TITLE (same row)
if 'JOB_TITLE' in output_data.columns and 'SOC_TITLE' in output_data.columns:
    missing_job_title = output_data['JOB_TITLE'].isna() | (output_data['JOB_TITLE'].astype(str).str.strip() == '')
    output_data.loc[missing_job_title, 'JOB_TITLE'] = output_data.loc[missing_job_title, 'SOC_TITLE']

# If JOB_TITLE has 0 alphabets (only numbers/spaces): replace with SOC_TITLE
if 'JOB_TITLE' in output_data.columns and 'SOC_TITLE' in output_data.columns:
    def has_no_letters(s):
        if pd.isna(s) or not str(s).strip():
            return False
        return not re.search(r'[a-zA-Z]', str(s))
    mask_no_letters = output_data['JOB_TITLE'].apply(has_no_letters)
    output_data.loc[mask_no_letters, 'JOB_TITLE'] = output_data.loc[mask_no_letters, 'SOC_TITLE']

# Wage correction rules (from migrations/0001c2_data_quality_part2.sql):
# 1) 8-digit values -> set to 0.01
# 2) 7-digit values -> multiply by 0.1
# Applied per-record; skip and log any record that fails (data quality issue)
if 'WAGE_RATE_OF_PAY_FROM' in output_data.columns:
    wage_numeric = pd.to_numeric(
        output_data['WAGE_RATE_OF_PAY_FROM'].astype(str).str.replace(',', '', regex=False).str.strip(),
        errors='coerce',
    )
    mask_8_digit = (wage_numeric >= 10000000) & (wage_numeric < 100000000)
    mask_7_digit = (wage_numeric >= 1000000) & (wage_numeric < 10000000)
    skipped_count = 0
    for idx in output_data.index:
        try:
            if mask_8_digit.loc[idx]:
                output_data.at[idx, 'WAGE_RATE_OF_PAY_FROM'] = '0.01'
            elif mask_7_digit.loc[idx]:
                corrected = wage_numeric.loc[idx] * 0.1
                output_data.at[idx, 'WAGE_RATE_OF_PAY_FROM'] = str(corrected)
        except Exception as e:
            skipped_count += 1
            try:
                orig = output_data.at[idx, 'WAGE_RATE_OF_PAY_FROM']
            except Exception:
                orig = '?'
            print(f"  Skipped wage correction for row {idx} (value={orig!r}): {e}")
    if skipped_count:
        print(f"  Wage correction: skipped {skipped_count} record(s) due to data quality issues.")


def clean_field(value, keep_ampersand=False):
    """Lowercase, trim, replace special chars with space, collapse multiple spaces.

    If keep_ampersand is True, & is preserved (for EMPLOYER_NAME / JOB_TITLE).
    """
    if pd.isna(value):
        return ""
    text = str(value).lower().strip()
    pattern = r"[^a-z0-9\s&]" if keep_ampersand else r"[^a-z0-9\s]"
    text = re.sub(pattern, " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def clean_job_title_field(value):
    """Like clean_field for JOB_TITLE: keep &, preserve .net (e.g. asp.net, .net developer)."""
    if pd.isna(value):
        return ""
    text = str(value).lower().strip()
    text = _mask_dotnet(text)
    text = re.sub(r"[^a-z0-9\s&]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    text = _unmask_dotnet(text)
    return text


# Apply clean_field to every output field except these (preserve exact chars: case, symbols, etc.)
COLUMNS_NO_CLEAN = frozenset(['CASE_NUMBER', 'SOC_CODE', 'BEGIN_DATE', 'END_DATE'])
for col in output_data.columns:
    if col in COLUMNS_NO_CLEAN:
        continue
    if col == "JOB_TITLE":
        output_data[col] = output_data[col].apply(clean_job_title_field)
    elif col == "EMPLOYER_NAME":
        output_data[col] = output_data[col].apply(lambda v: clean_field(v, keep_ampersand=True))
    else:
        output_data[col] = output_data[col].apply(clean_field)

# Capture job titles before SOC removal + noise cleanup (for change logging)
if 'JOB_TITLE' in output_data.columns and 'CASE_NUMBER' in output_data.columns:
    before_job_title_changes = output_data['JOB_TITLE'].copy()
    case_numbers = output_data['CASE_NUMBER'].copy()

# Remove SOC code from JOB_TITLE when present (e.g. "13 1081 02 logistics analysts" + SOC 13-1081.02 -> "logistics analysts")
if 'JOB_TITLE' in output_data.columns and 'SOC_CODE' in output_data.columns:
    output_data['JOB_TITLE'] = output_data.apply(
        lambda row: remove_soc_from_job_title(row['JOB_TITLE'], row['SOC_CODE']),
        axis=1,
    )

# Job title noise cleanup: remove trailing IDs/codes (00084644, kbgfjg184249, KBOEYTEST, etc.)
if 'JOB_TITLE' in output_data.columns:
    output_data['JOB_TITLE'] = output_data['JOB_TITLE'].apply(clean_job_title_noise)

# Near-duplicate title normalization using SequenceMatcher + TextBlob.
# Only maps to an existing title that already appears in the dataset.
if 'JOB_TITLE' in output_data.columns:
    job_title_map = build_job_title_normalization_map(output_data['JOB_TITLE'], min_similarity=0.97)
    if job_title_map:
        output_data['JOB_TITLE'] = output_data['JOB_TITLE'].apply(lambda title: job_title_map.get(title, title))
        print(f"  Near-duplicate job title normalization updated {len(job_title_map)} unique title variants")

# Near-duplicate employer name normalization using SequenceMatcher + TextBlob.
# Only maps to an existing employer name that already appears in the dataset.
if 'EMPLOYER_NAME' in output_data.columns:
    before_employer_name_changes = output_data['EMPLOYER_NAME'].copy()
    employer_name_map = build_employer_name_normalization_map(output_data['EMPLOYER_NAME'], min_similarity=0.97)
    if employer_name_map:
        output_data['EMPLOYER_NAME'] = output_data['EMPLOYER_NAME'].apply(
            lambda name: employer_name_map.get(name, name)
        )
        print(f"  Near-duplicate employer name normalization updated {len(employer_name_map)} unique name variants")
    else:
        before_employer_name_changes = None

# Log job title changes (SOC removal + noise cleanup + near-duplicate normalization; not fill-missing, only-numbers, or clean_field)
if 'JOB_TITLE' in output_data.columns and 'CASE_NUMBER' in output_data.columns:
    job_title_changes_log = []
    for idx in output_data.index:
        orig = before_job_title_changes.loc[idx] if idx in before_job_title_changes.index else ''
        final_val = output_data.at[idx, 'JOB_TITLE']
        if str(orig or '').strip() != str(final_val or '').strip():
            case_num = case_numbers.loc[idx] if idx in case_numbers.index else ''
            job_title_changes_log.append({
                'case_number': str(case_num),
                'before': str(orig) if pd.notna(orig) else '',
                'after': str(final_val) if pd.notna(final_val) else '',
            })
    with open(JOB_TITLE_CHANGES_LOG, 'w', encoding='utf-8') as f:
        json.dump({'job_title_changes': job_title_changes_log, 'total_changes': len(job_title_changes_log)}, f, indent=2)
    print(f"  Job title changes (SOC removal + noise cleanup + normalization) logged to {os.path.basename(JOB_TITLE_CHANGES_LOG)} ({len(job_title_changes_log)} rows)")

# Near-duplicate employer name normalization (SequenceMatcher + TextBlob, pick most correct)
if 'EMPLOYER_NAME' in output_data.columns and 'CASE_NUMBER' in output_data.columns:
    case_numbers = output_data['CASE_NUMBER'].copy()  # for change logging
    before_employer_changes = output_data['EMPLOYER_NAME'].copy()
    employer_map = build_employer_name_normalization_map(output_data['EMPLOYER_NAME'], min_similarity=0.97)
    if employer_map:
        output_data['EMPLOYER_NAME'] = output_data['EMPLOYER_NAME'].apply(lambda n: employer_map.get(n, n))
        print(f"  Near-duplicate employer name normalization updated {len(employer_map)} unique name variants")
    employer_changes_log = []
    for idx in output_data.index:
        orig = before_employer_changes.loc[idx] if idx in before_employer_changes.index else ''
        final_val = output_data.at[idx, 'EMPLOYER_NAME']
        if str(orig or '').strip() != str(final_val or '').strip():
            case_num = case_numbers.loc[idx] if idx in case_numbers.index else ''
            employer_changes_log.append({
                'case_number': str(case_num),
                'before': str(orig) if pd.notna(orig) else '',
                'after': str(final_val) if pd.notna(final_val) else '',
            })
    with open(EMPLOYER_NAME_CHANGES_LOG, 'w', encoding='utf-8') as f:
        json.dump({'employer_name_changes': employer_changes_log, 'total_changes': len(employer_changes_log)}, f, indent=2)
    print(f"  Employer name changes (near-duplicate normalization) logged to {os.path.basename(EMPLOYER_NAME_CHANGES_LOG)} ({len(employer_changes_log)} rows)")

# Output only the columns from columns_info.txt (in that order)
output_cols = [c for c in OUTPUT_COLUMNS if c in output_data.columns]
output_data = output_data[output_cols]

# Save to CSV
output_data.to_csv(target_file, index=False)
print(f"Saved {len(output_data)} rows to {target_file}")
elapsed_seconds = time.perf_counter() - start_time
print(f"Total processing time: {elapsed_seconds:.2f} seconds")
