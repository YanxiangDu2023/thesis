import os
import uuid
import pandas as pd
import unicodedata
from datetime import datetime
from fastapi import UploadFile, HTTPException
from app.database import get_connection

BASE_UPLOAD_DIR = "uploads"
OTH_EXPECTED_HEADERS = {
    "year",
    "source",
    "brand",
    "machine line",
    "country",
    "size class",
    "quantity",
}
OTH_COLUMN_NAMES = [
    "year",
    "source",
    "brand_name",
    "machine_line",
    "country",
    "size_class",
    "quantity",
]
SIZE_CLASS_EXPECTED_HEADERS = {
    "calendar",
    "source",
    "source code",
    "machine line",
    "machine code",
    "brand name",
    "brand code",
    "size class",
}
SIZE_CLASS_COLUMN_NAMES = [
    "calendar",
    "source",
    "source_code",
    "machine_line",
    "machine_code",
    "brand_name",
    "brand_code",
    "size_class",
]
MACHINE_LINE_MAPPING_EXPECTED_HEADERS = {
    "machine line name",
    "machine line code",
    "size class",
    "artificial machine line",
    "position",
}
MACHINE_LINE_MAPPING_COLUMN_NAMES = [
    "machine_line_name",
    "machine_line_code",
    "size_class",
    "artificial_machine_line",
    "position",
]
GROUP_COUNTRY_FIXED_COLUMNS = [
    "year",
    "country_grouping",
    "group_code",
    "country_code",
    "country_name",
    "market_area",
    "market_area_code",
    "region",
]
VOLVO_SALE_COLUMN_NAMES = [
    "calendar",
    "region",
    "market",
    "country",
    "machine",
    "machine_line",
    "size_class",
    "brand_owner_code",
    "brand_owner",
    "brand",
    "brand_nationality",
    "source",
    "fid",
]
TMA_DATA_COLUMN_NAMES = [
    "year",
    "geographical_region",
    "geographical_market_area",
    "end_country",
    "end_country_code",
    "machine_family",
    "machine_line",
    "machine_line_code",
    "size_class",
    "size_class_mapping",
    "total_market_fid_sales",
]

SOURCE_MATRIX_COUNTRY_GROUPING_ALIASES = [
    "Country Grouping",
    "Country Group",
    "country_grouping",
]
SOURCE_MATRIX_COUNTRY_NAME_ALIASES = [
    "Country Name",
    "Country",
    "Market",
    "Unnamed: 1",
    "country_name",
]
SOURCE_MATRIX_MACHINE_LINE_CODE_ALIASES = [
    "Machine Line",
    "Machine Line Code",
    "Machine Code",
    "machine_line_code",
]
SOURCE_MATRIX_MACHINE_LINE_NAME_ALIASES = [
    "Machine Line Name",
    "Machine Line Desc",
    "Machine Line Description",
    "Unnamed: 3",
    "machine_line_name",
]
SOURCE_MATRIX_PRIMARY_SOURCE_ALIASES = [
    "Primary Source",
    "primary_source",
]
SOURCE_MATRIX_SECONDARY_SOURCE_ALIASES = [
    "Secondary source NOT IN USE",
    "Secondary Source NOT IN USE",
    "Secondary Source",
    "secondary_source",
]
SOURCE_MATRIX_CRP_SOURCE_ALIASES = [
    "CRP source",
    "CRP Source",
    "crp_source",
]
SOURCE_MATRIX_CHANGE_INDICATOR_ALIASES = [
    "Change Indicator",
    "change_indicator",
    "Deletion Indicator",
]
BRAND_MAPPING_BRAND_NAME_ALIASES = [
    "Brand Name",
    "Brand",
    "brand_name",
]
BRAND_MAPPING_BRAND_CODE_ALIASES = [
    "Brand Code",
    "Code",
    "brand_code",
]
BRAND_MAPPING_DELETION_INDICATOR_ALIASES = [
    "Deletion Indicator",
    "Delete Indicator",
    "Change Indicator",
    "deletion_indicator",
]


def _clean_cell(value) -> str:
    if pd.isna(value):
        return ""
    text = unicodedata.normalize("NFKC", str(value)).strip()
    return "" if text.lower() == "nan" else text


def _normalize_header(value) -> str:
    # Normalize separators so header detection works for both
    # "geographical region" and "geographical_region" styles.
    return " ".join(
        str(value)
        .replace("\ufeff", "")
        .replace("\n", " ")
        .replace("_", " ")
        .replace("-", " ")
        .strip()
        .lower()
        .split()
    )

def _build_column_lookup(columns) -> dict[str, str]:
    lookup: dict[str, str] = {}
    for column in columns:
        normalized = _normalize_header(column)
        if normalized and normalized not in lookup:
            lookup[normalized] = column
    return lookup

def _get_cell_by_header_aliases(row, column_lookup: dict[str, str], aliases: list[str]) -> str:
    for alias in aliases:
        normalized_alias = _normalize_header(alias)
        source_column = column_lookup.get(normalized_alias)
        if source_column is not None:
            return _clean_cell(row.get(source_column, ""))
    return ""


def _get_cell_by_header_aliases_or_index(
    row,
    column_lookup: dict[str, str],
    aliases: list[str],
    fallback_index: int,
    source_columns: list,
) -> str:
    for alias in aliases:
        normalized_alias = _normalize_header(alias)
        source_column = column_lookup.get(normalized_alias)
        if source_column is not None:
            return _clean_cell(row.get(source_column, ""))

    if 0 <= fallback_index < len(source_columns):
        source_column = source_columns[fallback_index]
        return _clean_cell(row.get(source_column, ""))

    return ""


def _looks_like_oth_header(first_row) -> bool:
    normalized_headers = {_normalize_header(value) for value in first_row if str(value).strip()}
    header_matches = normalized_headers.intersection(OTH_EXPECTED_HEADERS)
    return len(header_matches) >= 4


def _load_oth_dataframe(stored_path: str) -> pd.DataFrame:
    oth_df = pd.read_csv(stored_path, header=None, dtype=str, keep_default_na=False)

    if oth_df.empty:
        return pd.DataFrame(columns=OTH_COLUMN_NAMES)

    if _looks_like_oth_header(oth_df.iloc[0].tolist()):
        oth_df = oth_df.iloc[1:].reset_index(drop=True)

    if oth_df.shape[1] < len(OTH_COLUMN_NAMES):
        raise HTTPException(
            status_code=400,
            detail=(
                "OTH Data CSV must include columns in this order: "
                "Year, Source, Brand, Machine Line, Country, Size Class, Quantity."
            ),
        )

    # Support both layouts:
    # 1) New 7-column layout: Year, Source, Brand, Machine Line, Country, Size Class, Quantity
    # 2) Legacy 9-column layout with blank placeholders between machine_line/country and country/size_class
    use_legacy_layout = False
    if oth_df.shape[1] >= 9:
        sample_size = min(len(oth_df), 50)
        if sample_size > 0:
            sample = oth_df.iloc[:sample_size]
            col_5_empty_ratio = (
                sample.iloc[:, 4].astype(str).str.strip().eq("").sum() / sample_size
            )
            col_7_empty_ratio = (
                sample.iloc[:, 6].astype(str).str.strip().eq("").sum() / sample_size
            )
            use_legacy_layout = col_5_empty_ratio >= 0.8 and col_7_empty_ratio >= 0.8

    if use_legacy_layout:
        normalized = pd.DataFrame({
            "year": oth_df.iloc[:, 0],
            "source": oth_df.iloc[:, 1],
            "brand_name": oth_df.iloc[:, 2],
            "machine_line": oth_df.iloc[:, 3],
            "country": oth_df.iloc[:, 5],
            "size_class": oth_df.iloc[:, 7],
            "quantity": oth_df.iloc[:, 8],
        })
    else:
        normalized = oth_df.iloc[:, :len(OTH_COLUMN_NAMES)].copy()
        normalized.columns = OTH_COLUMN_NAMES

    return normalized


def _looks_like_size_class_header(first_row) -> bool:
    normalized_headers = {_normalize_header(value) for value in first_row if str(value).strip()}
    header_matches = normalized_headers.intersection(SIZE_CLASS_EXPECTED_HEADERS)
    return len(header_matches) >= 5


def _load_size_class_dataframe(stored_path: str) -> pd.DataFrame:
    size_class_df = pd.read_csv(stored_path, header=None, dtype=str, keep_default_na=False)

    if size_class_df.empty:
        return pd.DataFrame(columns=SIZE_CLASS_COLUMN_NAMES)

    if _looks_like_size_class_header(size_class_df.iloc[0].tolist()):
        size_class_df = size_class_df.iloc[1:].reset_index(drop=True)

    if size_class_df.shape[1] < len(SIZE_CLASS_COLUMN_NAMES):
        for missing_col in range(size_class_df.shape[1], len(SIZE_CLASS_COLUMN_NAMES)):
            size_class_df[missing_col] = ""

    size_class_df = size_class_df.iloc[:, :len(SIZE_CLASS_COLUMN_NAMES)].copy()
    size_class_df.columns = SIZE_CLASS_COLUMN_NAMES

    # Safety net: drop any header-like rows that may still exist
    # (for example due to repeated header rows inside the file).
    header_like_mask = size_class_df.apply(
        lambda row: _looks_like_size_class_header(row.tolist()),
        axis=1,
    )
    if header_like_mask.any():
        size_class_df = size_class_df.loc[~header_like_mask].reset_index(drop=True)

    return size_class_df


def _looks_like_machine_line_mapping_header(first_row) -> bool:
    normalized_headers = {_normalize_header(value) for value in first_row if str(value).strip()}
    header_matches = normalized_headers.intersection(MACHINE_LINE_MAPPING_EXPECTED_HEADERS)
    return len(header_matches) >= 1


def _load_machine_line_mapping_dataframe(stored_path: str) -> pd.DataFrame:
    machine_line_mapping_df = pd.read_csv(stored_path, header=None, dtype=str, keep_default_na=False)

    if machine_line_mapping_df.empty:
        return pd.DataFrame(columns=MACHINE_LINE_MAPPING_COLUMN_NAMES)

    # Drop leading empty lines before header detection.
    while not machine_line_mapping_df.empty:
        first_values = [_clean_cell(value) for value in machine_line_mapping_df.iloc[0].tolist()]
        if any(value != "" for value in first_values):
            break
        machine_line_mapping_df = machine_line_mapping_df.iloc[1:].reset_index(drop=True)

    if machine_line_mapping_df.empty:
        return pd.DataFrame(columns=MACHINE_LINE_MAPPING_COLUMN_NAMES)

    if _looks_like_machine_line_mapping_header(machine_line_mapping_df.iloc[0].tolist()):
        machine_line_mapping_df = machine_line_mapping_df.iloc[1:].reset_index(drop=True)

    if machine_line_mapping_df.shape[1] < len(MACHINE_LINE_MAPPING_COLUMN_NAMES):
        for missing_col in range(machine_line_mapping_df.shape[1], len(MACHINE_LINE_MAPPING_COLUMN_NAMES)):
            machine_line_mapping_df[missing_col] = ""

    machine_line_mapping_df = machine_line_mapping_df.iloc[:, :len(MACHINE_LINE_MAPPING_COLUMN_NAMES)].copy()
    machine_line_mapping_df.columns = MACHINE_LINE_MAPPING_COLUMN_NAMES

    # Safety net: remove repeated header-like rows that may appear in data.
    header_like_mask = machine_line_mapping_df.apply(
        lambda row: _looks_like_machine_line_mapping_header(row.tolist()),
        axis=1,
    )
    if header_like_mask.any():
        machine_line_mapping_df = machine_line_mapping_df.loc[~header_like_mask].reset_index(drop=True)

    return machine_line_mapping_df


def _load_group_country_dataframe(stored_path: str) -> pd.DataFrame:
    group_country_df = pd.read_csv(stored_path, header=None, dtype=str, keep_default_na=False)

    if group_country_df.empty:
        return pd.DataFrame(columns=GROUP_COUNTRY_FIXED_COLUMNS)

    # Drop leading empty lines before header detection.
    while not group_country_df.empty:
        first_values = [_clean_cell(value) for value in group_country_df.iloc[0].tolist()]
        if any(value != "" for value in first_values):
            break
        group_country_df = group_country_df.iloc[1:].reset_index(drop=True)

    if group_country_df.empty:
        return pd.DataFrame(columns=GROUP_COUNTRY_FIXED_COLUMNS)

    first_row = [
        _normalize_header(value)
        for value in group_country_df.iloc[0].tolist()
    ]
    header_flags = [
        len(first_row) > 0 and ("calendar" in first_row[0] or "year" in first_row[0]),
        len(first_row) > 1 and "country grouping" in first_row[1],
        len(first_row) > 2 and "group code" in first_row[2],
        len(first_row) > 3 and "country code" in first_row[3],
        len(first_row) > 4 and "country" in first_row[4],
        len(first_row) > 5 and "market area" in first_row[5],
        len(first_row) > 6 and "market area code" in first_row[6],
        len(first_row) > 7 and "region" in first_row[7],
    ]
    has_header_row = sum(1 for flag in header_flags if flag) >= 6

    if has_header_row:
        group_country_df = group_country_df.iloc[1:].reset_index(drop=True)

    if group_country_df.shape[1] < len(GROUP_COUNTRY_FIXED_COLUMNS):
        raise HTTPException(
            status_code=400,
            detail=(
                "Group Country CSV must follow fixed column order: "
                "Calendar Year, Country Grouping, group code, Country Code, "
                "Country, Market Area, market area code, Region."
            ),
        )

    normalized_data = group_country_df.iloc[:, :len(GROUP_COUNTRY_FIXED_COLUMNS)].copy()
    normalized_data.columns = GROUP_COUNTRY_FIXED_COLUMNS

    # Safety net: remove any repeated header-like rows that may still appear in data.
    def _looks_like_group_country_header_row(row_values) -> bool:
        normalized = [_normalize_header(value) for value in row_values]
        checks = [
            len(normalized) > 0 and normalized[0] in {"calendar", "calendar year", "year"},
            len(normalized) > 1 and normalized[1] == "country grouping",
            len(normalized) > 2 and normalized[2] == "group code",
            len(normalized) > 3 and normalized[3] == "country code",
            len(normalized) > 4 and normalized[4] in {"country", "country name"},
            len(normalized) > 5 and normalized[5] == "market area",
            len(normalized) > 6 and normalized[6] == "market area code",
            len(normalized) > 7 and normalized[7] == "region",
        ]
        return sum(1 for flag in checks if flag) >= 6

    header_like_mask = normalized_data.apply(
        lambda row: _looks_like_group_country_header_row(row.tolist()),
        axis=1,
    )
    if header_like_mask.any():
        normalized_data = normalized_data.loc[~header_like_mask].reset_index(drop=True)

    return normalized_data


def _looks_like_volvo_sale_header(first_row) -> bool:
    normalized_row = [_normalize_header(value) for value in first_row]
    header_flags = [
        len(normalized_row) > 0 and "calendar" in normalized_row[0],
        len(normalized_row) > 1 and "region" in normalized_row[1],
        len(normalized_row) > 2 and "market" in normalized_row[2],
        len(normalized_row) > 3 and "country" in normalized_row[3],
        len(normalized_row) > 4 and "machine" in normalized_row[4],
        len(normalized_row) > 5 and "machine line" in normalized_row[5],
        len(normalized_row) > 6 and "size class" in normalized_row[6],
        len(normalized_row) > 7 and "brand owner" in normalized_row[7],
        len(normalized_row) > 8 and "brand owner" in normalized_row[8],
        len(normalized_row) > 9 and "brand" in normalized_row[9],
        len(normalized_row) > 10 and "brand" in normalized_row[10],
        len(normalized_row) > 11 and "source" in normalized_row[11],
        len(normalized_row) > 12 and "fid" in normalized_row[12],
    ]
    return sum(1 for flag in header_flags if flag) >= 9


def _load_volvo_sale_dataframe(stored_path: str) -> pd.DataFrame:
    volvo_sale_df = pd.read_csv(stored_path, header=None, dtype=str, keep_default_na=False)

    if volvo_sale_df.empty:
        return pd.DataFrame(columns=VOLVO_SALE_COLUMN_NAMES)

    if _looks_like_volvo_sale_header(volvo_sale_df.iloc[0].tolist()):
        volvo_sale_df = volvo_sale_df.iloc[1:].reset_index(drop=True)

    if volvo_sale_df.shape[1] < len(VOLVO_SALE_COLUMN_NAMES):
        raise HTTPException(
            status_code=400,
            detail=(
                "Volvo Sale Data CSV must include columns in this order: "
                "Calendar, Region, Market, Country, Machine, Machine Line, "
                "Size Class, Brand Owner code, Brand Owner, Brand, Brand Nationality, Source, FID."
            ),
        )

    volvo_sale_df = volvo_sale_df.iloc[:, :len(VOLVO_SALE_COLUMN_NAMES)].copy()
    volvo_sale_df.columns = VOLVO_SALE_COLUMN_NAMES

    return volvo_sale_df


def _looks_like_tma_data_header(first_row) -> bool:
    normalized_row = [_normalize_header(value) for value in first_row]
    header_flags = [
        len(normalized_row) > 0 and "year" in normalized_row[0],
        len(normalized_row) > 1 and "geographical region" in normalized_row[1],
        len(normalized_row) > 2 and "geographical market area" in normalized_row[2],
        len(normalized_row) > 3 and "end country" in normalized_row[3],
        len(normalized_row) > 4 and "end country code" in normalized_row[4],
        len(normalized_row) > 5 and "machine family" in normalized_row[5],
        len(normalized_row) > 6 and "machine line" in normalized_row[6],
        len(normalized_row) > 7 and "machine line code" in normalized_row[7],
        len(normalized_row) > 8 and "size class" in normalized_row[8],
        len(normalized_row) > 9 and "size class mapping" in normalized_row[9],
        len(normalized_row) > 10 and "total market fid sales" in normalized_row[10],
    ]
    return sum(1 for flag in header_flags if flag) >= 8


def _load_tma_data_dataframe(stored_path: str) -> pd.DataFrame:
    tma_df = pd.read_csv(stored_path, header=None, dtype=str, keep_default_na=False)

    if tma_df.empty:
        return pd.DataFrame(columns=TMA_DATA_COLUMN_NAMES)

    if _looks_like_tma_data_header(tma_df.iloc[0].tolist()):
        tma_df = tma_df.iloc[1:].reset_index(drop=True)

    if tma_df.shape[1] < len(TMA_DATA_COLUMN_NAMES):
        raise HTTPException(
            status_code=400,
            detail=(
                "TMA Data CSV must include columns in this order: "
                "Year, Geographical Region, Geographical Market Area, End Country, "
                "End Country Code, Machine Family, Machine Line, Machine Line Code, Size Class, "
                "Size Class Mapping, Total Market FID Sales."
            ),
        )

    tma_df = tma_df.iloc[:, :len(TMA_DATA_COLUMN_NAMES)].copy()
    tma_df.columns = TMA_DATA_COLUMN_NAMES

    return tma_df

async def handle_csv_upload(matrix_type: str, file: UploadFile):
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are allowed.")

    target_dir = os.path.join(BASE_UPLOAD_DIR, matrix_type)
    os.makedirs(target_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    unique_name = f"{timestamp}_{uuid.uuid4().hex}_{file.filename}"
    stored_path = os.path.join(target_dir, unique_name)

    content = await file.read()
    with open(stored_path, "wb") as f:
        f.write(content)

    conn = get_connection()
    cursor = conn.cursor()
    upload_run_id = None

    try:
        cursor.execute("""
            INSERT INTO upload_runs (
                matrix_type,
                original_file_name,
                stored_file_name,
                stored_path,
                status
            ) VALUES (?, ?, ?, ?, ?)
        """, (
            matrix_type,
            file.filename,
            unique_name,
            stored_path,
            "processing"
        ))
        upload_run_id = cursor.lastrowid
        conn.commit()

        df = pd.read_csv(stored_path, dtype=str, keep_default_na=False)

        row_count = 0

        if matrix_type == "reporter_list":
            row_count = len(df)
            reporter_column_lookup = _build_column_lookup(df.columns)
            for idx, row in df.iterrows():
                cursor.execute("""
                    INSERT INTO reporter_list_rows (
                        upload_run_id,
                        row_index,
                        calendar,
                        source,
                        source_code,
                        machine_line,
                        machine_code,
                        brand_name,
                        brand_code
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    upload_run_id,
                    idx + 1,
                    _get_cell_by_header_aliases(
                        row,
                        reporter_column_lookup,
                        ["Calendar Year", "Calendar", "Year"]
                    ),
                    _get_cell_by_header_aliases(
                        row,
                        reporter_column_lookup,
                        ["Source"]
                    ),
                    _get_cell_by_header_aliases(
                        row,
                        reporter_column_lookup,
                        ["source_code", "Source Code", "Source_Code", "Unnamed: 2"]
                    ),
                    _get_cell_by_header_aliases(
                        row,
                        reporter_column_lookup,
                        ["Machine Line"]
                    ),
                    _get_cell_by_header_aliases(
                        row,
                        reporter_column_lookup,
                        ["Machine Line Code", "Machine Code", "machine_code", "Unnamed: 4"]
                    ),
                    _get_cell_by_header_aliases(
                        row,
                        reporter_column_lookup,
                        ["Brand", "Brand Name", "Brand Code", "Unnamed: 5"]
                    ),
                    _get_cell_by_header_aliases(
                        row,
                        reporter_column_lookup,
                        ["Unnamed: 6", "Brand Code", "brand_code", "Brand_Code"]
                    )
                ))

        elif matrix_type == "source_matrix":
            row_count = len(df)
            source_matrix_column_lookup = _build_column_lookup(df.columns)
            source_matrix_columns = list(df.columns)
            for idx, row in df.iterrows():
                cursor.execute("""
                    INSERT INTO source_matrix_rows (
                        upload_run_id,
                        row_index,
                        country_grouping,
                        country_name,
                        machine_line_code,
                        machine_line_name,
                        primary_source,
                        secondary_source,
                        crp_source,
                        change_indicator
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    upload_run_id,
                    idx + 1,
                    _get_cell_by_header_aliases_or_index(
                        row,
                        source_matrix_column_lookup,
                        SOURCE_MATRIX_COUNTRY_GROUPING_ALIASES,
                        0,
                        source_matrix_columns,
                    ),
                    _get_cell_by_header_aliases_or_index(
                        row,
                        source_matrix_column_lookup,
                        SOURCE_MATRIX_COUNTRY_NAME_ALIASES,
                        1,
                        source_matrix_columns,
                    ),
                    _get_cell_by_header_aliases_or_index(
                        row,
                        source_matrix_column_lookup,
                        SOURCE_MATRIX_MACHINE_LINE_CODE_ALIASES,
                        2,
                        source_matrix_columns,
                    ),
                    _get_cell_by_header_aliases_or_index(
                        row,
                        source_matrix_column_lookup,
                        SOURCE_MATRIX_MACHINE_LINE_NAME_ALIASES,
                        3,
                        source_matrix_columns,
                    ),
                    _get_cell_by_header_aliases_or_index(
                        row,
                        source_matrix_column_lookup,
                        SOURCE_MATRIX_PRIMARY_SOURCE_ALIASES,
                        4,
                        source_matrix_columns,
                    ),
                    _get_cell_by_header_aliases_or_index(
                        row,
                        source_matrix_column_lookup,
                        SOURCE_MATRIX_SECONDARY_SOURCE_ALIASES,
                        5,
                        source_matrix_columns,
                    ),
                    _get_cell_by_header_aliases_or_index(
                        row,
                        source_matrix_column_lookup,
                        SOURCE_MATRIX_CRP_SOURCE_ALIASES,
                        6,
                        source_matrix_columns,
                    ),
                    _get_cell_by_header_aliases_or_index(
                        row,
                        source_matrix_column_lookup,
                        SOURCE_MATRIX_CHANGE_INDICATOR_ALIASES,
                        7,
                        source_matrix_columns,
                    ),
                ))

        elif matrix_type == "size_class":
            size_class_df = _load_size_class_dataframe(stored_path)
            row_count = len(size_class_df)

            for idx, row in size_class_df.iterrows():
                cursor.execute("""
                    INSERT INTO size_class_rows (
                        upload_run_id,
                        row_index,
                        calendar,
                        source,
                        source_code,
                        machine_line,
                        machine_code,
                        brand_name,
                        brand_code,
                        size_class
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    upload_run_id,
                    idx + 1,
                    _clean_cell(row.get("calendar", "")),
                    _clean_cell(row.get("source", "")),
                    _clean_cell(row.get("source_code", "")),
                    _clean_cell(row.get("machine_line", "")),
                    _clean_cell(row.get("machine_code", "")),
                    _clean_cell(row.get("brand_name", "")),
                    _clean_cell(row.get("brand_code", "")),
                    _clean_cell(row.get("size_class", ""))
                ))

        elif matrix_type == "brand_mapping":
            row_count = len(df)
            brand_mapping_column_lookup = _build_column_lookup(df.columns)
            brand_mapping_columns = list(df.columns)
            for idx, row in df.iterrows():
                cursor.execute("""
                    INSERT INTO brand_mapping_rows (
                        upload_run_id,
                        row_index,
                        brand_name,
                        brand_code,
                        deletion_indicator
                    ) VALUES (?, ?, ?, ?, ?)
                """, (
                    upload_run_id,
                    idx + 1,
                    _get_cell_by_header_aliases_or_index(
                        row,
                        brand_mapping_column_lookup,
                        BRAND_MAPPING_BRAND_NAME_ALIASES,
                        0,
                        brand_mapping_columns,
                    ),
                    _get_cell_by_header_aliases_or_index(
                        row,
                        brand_mapping_column_lookup,
                        BRAND_MAPPING_BRAND_CODE_ALIASES,
                        1,
                        brand_mapping_columns,
                    ),
                    _get_cell_by_header_aliases_or_index(
                        row,
                        brand_mapping_column_lookup,
                        BRAND_MAPPING_DELETION_INDICATOR_ALIASES,
                        2,
                        brand_mapping_columns,
                    ),
                ))

        elif matrix_type == "group_country":
            group_country_df = _load_group_country_dataframe(stored_path)
            row_count = len(group_country_df)

            for idx, row in group_country_df.iterrows():

                cursor.execute("""
                    INSERT INTO group_country_rows (
                        upload_run_id,
                        row_index,
                        year,
                        group_code,
                        country_code,
                        country_name,
                        country_grouping,
                        region,
                        market_area,
                        market_area_code
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    upload_run_id,
                    idx + 1,
                    _clean_cell(row.get("year", "")),
                    _clean_cell(row.get("group_code", "")),
                    _clean_cell(row.get("country_code", "")),
                    _clean_cell(row.get("country_name", "")),
                    _clean_cell(row.get("country_grouping", "")),
                    _clean_cell(row.get("region", "")),
                    _clean_cell(row.get("market_area", "")),
                    _clean_cell(row.get("market_area_code", ""))
                ))

        elif matrix_type == "machine_line_mapping":
            machine_line_mapping_df = _load_machine_line_mapping_dataframe(stored_path)
            row_count = len(machine_line_mapping_df)

            for idx, row in machine_line_mapping_df.iterrows():
                cursor.execute("""
                    INSERT INTO machine_line_mapping_rows (
                        upload_run_id,
                        row_index,
                        machine_line_name,
                        machine_line_code,
                        size_class,
                        artificial_machine_line,
                        position
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    upload_run_id,
                    idx + 1,
                    _clean_cell(row.get("machine_line_name", "")),
                    _clean_cell(row.get("machine_line_code", "")),
                    _clean_cell(row.get("size_class", "")),
                    _clean_cell(row.get("artificial_machine_line", "")),
                    _clean_cell(row.get("position", ""))
                ))

        elif matrix_type == "oth_data":
            oth_df = _load_oth_dataframe(stored_path)
            row_count = len(oth_df)

            for idx, row in oth_df.iterrows():
                cursor.execute("""
                    INSERT INTO oth_data_rows (
                        upload_run_id,
                        row_index,
                        year,
                        source,
                        brand_name,
                        machine_line,
                        country,
                        size_class,
                        quantity
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    upload_run_id,
                    idx + 1,
                    _clean_cell(row.get("year", "")),
                    _clean_cell(row.get("source", "")),
                    _clean_cell(row.get("brand_name", "")),
                    _clean_cell(row.get("machine_line", "")),
                    _clean_cell(row.get("country", "")),
                    _clean_cell(row.get("size_class", "")),
                    _clean_cell(row.get("quantity", ""))
                ))

        elif matrix_type == "volvo_sale_data":
            volvo_sale_df = _load_volvo_sale_dataframe(stored_path)
            row_count = len(volvo_sale_df)

            for idx, row in volvo_sale_df.iterrows():
                cursor.execute("""
                    INSERT INTO volvo_sale_data_rows (
                        upload_run_id,
                        row_index,
                        calendar,
                        region,
                        market,
                        country,
                        machine,
                        machine_line,
                        size_class,
                        brand_owner_code,
                        brand_owner,
                        brand,
                        brand_nationality,
                        source,
                        fid
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    upload_run_id,
                    idx + 1,
                    _clean_cell(row.get("calendar", "")),
                    _clean_cell(row.get("region", "")),
                    _clean_cell(row.get("market", "")),
                    _clean_cell(row.get("country", "")),
                    _clean_cell(row.get("machine", "")),
                    _clean_cell(row.get("machine_line", "")),
                    _clean_cell(row.get("size_class", "")),
                    _clean_cell(row.get("brand_owner_code", "")),
                    _clean_cell(row.get("brand_owner", "")),
                    _clean_cell(row.get("brand", "")),
                    _clean_cell(row.get("brand_nationality", "")),
                    _clean_cell(row.get("source", "")),
                    _clean_cell(row.get("fid", ""))
                ))

        elif matrix_type == "tma_data":
            tma_df = _load_tma_data_dataframe(stored_path)
            row_count = len(tma_df)

            for idx, row in tma_df.iterrows():
                cursor.execute("""
                    INSERT INTO tma_data_rows (
                        upload_run_id,
                        row_index,
                        year,
                        geographical_region,
                        geographical_market_area,
                        end_country,
                        end_country_code,
                        machine_family,
                        machine_line,
                        machine_line_code,
                        size_class,
                        size_class_mapping,
                        total_market_fid_sales
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    upload_run_id,
                    idx + 1,
                    _clean_cell(row.get("year", "")),
                    _clean_cell(row.get("geographical_region", "")),
                    _clean_cell(row.get("geographical_market_area", "")),
                    _clean_cell(row.get("end_country", "")),
                    _clean_cell(row.get("end_country_code", "")),
                    _clean_cell(row.get("machine_family", "")),
                    _clean_cell(row.get("machine_line", "")),
                    _clean_cell(row.get("machine_line_code", "")),
                    _clean_cell(row.get("size_class", "")),
                    _clean_cell(row.get("size_class_mapping", "")),
                    _clean_cell(row.get("total_market_fid_sales", ""))
                ))

        else:
            raise HTTPException(status_code=400, detail="Unsupported matrix type.")

        cursor.execute("""
            UPDATE upload_runs
            SET row_count = ?, status = ?, message = ?
            WHERE id = ?
        """, (
            row_count,
            "success",
            "Upload completed successfully",
            upload_run_id
        ))
        conn.commit()

        return {
            "message": "Upload successful",
            "upload_run_id": upload_run_id,
            "row_count": row_count,
            "matrix_type": matrix_type,
            "original_file_name": file.filename,
            "status": "success"
        }

    except HTTPException as e:
        if upload_run_id is not None:
            cursor.execute("""
                UPDATE upload_runs
                SET status = ?, message = ?
                WHERE id = ?
            """, (
                "failed",
                str(e.detail),
                upload_run_id
            ))
            conn.commit()
        raise e

    except Exception as e:
        if upload_run_id is not None:
            cursor.execute("""
                UPDATE upload_runs
                SET status = ?, message = ?
                WHERE id = ?
            """, (
                "failed",
                str(e),
                upload_run_id
            ))
            conn.commit()
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        conn.close()
