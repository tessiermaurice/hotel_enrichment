#!/usr/bin/env python3
import argparse
import csv
import logging
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import pandas as pd
import tldextract
import yaml
from unidecode import unidecode

REQUIRED_COLUMNS = [
    "DATE DE CLASSEMENT",
    "TYPE D'HÉBERGEMENT",
    "STAR",
    "NOM COMMERCIAL",
    "ADRESSE",
    "CODE POSTAL",
    "COMMUNE",
    "WEBSITE",
    "CAPACITÉ D'ACCUEIL (PERSONNES)",
    "NOMBRE DE CHAMBRES",
    "Email_Primary",
    "Email_Additional",
    "Country",
    "Phone_Primary",
    "Phone_Additional",
    "Website_Status",
    "Scraping_Result",
]

NEW_COLUMNS = [
    "department",
    "region",
    "capacity_range",
    "size_segment",
    "restaurant_flag",
    "spa_flag",
    "hotel_domain",
    "independent_or_group",
    "group_name",
    "large_property_flag",
    "boutique_flag",
    "hotel_context",
]

DEFAULT_CONFIG = {
    "threshold_small_max": 30,
    "threshold_medium_max": 80,
    "threshold_boutique_max": 25,
    "threshold_large_rooms_min": 80,
    "threshold_large_capacity_min": 160,
    "restaurant_keywords": [
        "restaurant",
        "brasserie",
        "bistro",
        "bistrot",
        "table",
        "auberge",
        "gourmand",
        "cuisine",
        "ristorante",
        "trattoria",
    ],
    "spa_keywords": [
        "spa",
        "thalasso",
        "thalassotherapie",
        "balneo",
        "balneotherapie",
        "bien etre",
        "wellness",
        "thermes",
        "thermal",
    ],
}

CITY_KEYWORDS_URBAIN = ["aeroport", "gare", "centre ville", "city"]
NAME_KEYWORDS_LOISIR = ["plage", "mer", "montagne", "ski", "lac", "golf", "domaine", "resort"]
TYPE_KEYWORDS_LOISIR = ["camping", "residence", "village"]


def setup_logging(output_dir: Path) -> logging.Logger:
    logger = logging.getLogger("enrich_hotels")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    fh = logging.FileHandler(output_dir / "enrich_hotels.log", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    sh = logging.StreamHandler()
    sh.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(sh)
    return logger


def load_config(path: Path) -> Dict:
    if not path.exists():
        return DEFAULT_CONFIG.copy()
    with path.open("r", encoding="utf-8") as f:
        loaded = yaml.safe_load(f) or {}
    cfg = DEFAULT_CONFIG.copy()
    cfg.update(loaded)
    return cfg


def read_input(input_path: Path, logger: logging.Logger) -> pd.DataFrame:
    ext = input_path.suffix.lower()
    if ext in {".xlsx", ".xls"}:
        return pd.read_excel(input_path)

    if ext == ".csv":
        encodings = ["utf-8", "utf-8-sig", "cp1252", "latin-1"]
        last_error = None
        for enc in encodings:
            try:
                with input_path.open("r", encoding=enc, newline="") as f:
                    sample = f.read(4096)
                    dialect = csv.Sniffer().sniff(sample, delimiters=",;")
                    delimiter = dialect.delimiter
                logger.info(f"CSV detected: encoding={enc}, delimiter='{delimiter}'")
                return pd.read_csv(input_path, encoding=enc, delimiter=delimiter)
            except Exception as e:
                last_error = e
                logger.warning(f"CSV read attempt failed with encoding {enc}: {e}")
        raise RuntimeError(f"Unable to read CSV with supported encodings. Last error: {last_error}")

    raise ValueError("Input must be .xlsx/.xls or .csv")


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).strip() for c in df.columns]
    return df


def validate_columns(df: pd.DataFrame):
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError("Missing required columns: " + ", ".join(missing))


def to_str_clean(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip()


def parse_int(value) -> Optional[int]:
    if pd.isna(value):
        return None
    text = str(value).strip().replace(" ", "")
    if text == "":
        return None
    text = text.replace(",", ".")
    try:
        return int(float(text))
    except Exception:
        return None


def normalize_text(text: str) -> str:
    return unidecode((text or "").lower()).strip()


def infer_department(code_postal: str) -> str:
    cp = re.sub(r"\s+", "", code_postal or "")
    if not re.fullmatch(r"\d{5}", cp):
        return ""
    if cp.startswith(("97", "98")):
        return cp[:3]
    if cp.startswith("20"):
        val = int(cp)
        if 20000 <= val <= 20199:
            return "2A"
        if 20200 <= val <= 20699:
            return "2B"
    return cp[:2]


def extract_domain(url: str, extractor) -> str:
    raw = (url or "").strip()
    if not raw:
        return ""
    if not re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*://", raw):
        raw = "http://" + raw
    try:
        parsed = urlparse(raw)
        host = parsed.netloc
        if not host:
            return ""
        ext = extractor(host)
        if not ext.domain or not ext.suffix:
            return ""
        return f"{ext.domain}.{ext.suffix}".lower()
    except Exception:
        return ""


def contains_any_keywords(text: str, keywords: List[str]) -> bool:
    n = normalize_text(text)
    return any(normalize_text(k) in n for k in keywords)


def infer_context(type_hebergement: str, nom: str, commune: str, major_cities: set) -> str:
    type_norm = normalize_text(type_hebergement)
    name_norm = normalize_text(nom)
    commune_norm = normalize_text(commune)

    if any(k in type_norm for k in TYPE_KEYWORDS_LOISIR):
        return "loisir"
    if any(k in name_norm for k in CITY_KEYWORDS_URBAIN):
        return "urbain"
    if any(k in name_norm for k in NAME_KEYWORDS_LOISIR):
        return "loisir"
    if commune_norm in major_cities:
        return "urbain"
    return "inconnu"


def load_department_to_region(path: Path) -> Dict[str, str]:
    df = pd.read_csv(path, dtype=str).fillna("")
    return {str(r["department"]).strip().upper(): str(r["region"]).strip() for _, r in df.iterrows()}


def load_group_domains(path: Path) -> Dict[str, str]:
    df = pd.read_csv(path, dtype=str).fillna("")
    return {str(r["domain"]).strip().lower(): str(r["group_name"]).strip() for _, r in df.iterrows()}


def load_major_cities(path: Path) -> set:
    if not path.exists():
        return set()
    with path.open("r", encoding="utf-8") as f:
        return {normalize_text(line.strip()) for line in f if line.strip()}


def enrich(df: pd.DataFrame, cfg: Dict, dep_to_region: Dict[str, str], groups: Dict[str, str], major_cities: set):
    # Cleaning
    df["CODE POSTAL"] = to_str_clean(df["CODE POSTAL"])
    df["NOM COMMERCIAL"] = to_str_clean(df["NOM COMMERCIAL"])
    df["WEBSITE"] = to_str_clean(df["WEBSITE"])

    star_num = pd.to_numeric(df["STAR"], errors="coerce")
    rooms = df["NOMBRE DE CHAMBRES"].apply(parse_int)
    capacity = df["CAPACITÉ D'ACCUEIL (PERSONNES)"].apply(parse_int)

    extractor = tldextract.TLDExtract(suffix_list_urls=None)

    departments = df["CODE POSTAL"].apply(infer_department)
    regions = departments.apply(lambda d: dep_to_region.get(d.upper(), "") if d else "")

    def cap_range(r: Optional[int]) -> str:
        if r is None:
            return ""
        if 1 <= r <= int(cfg["threshold_small_max"]):
            return "petite"
        if r <= int(cfg["threshold_medium_max"]):
            return "intermediaire"
        return "grande"

    def size_seg(r: Optional[int]) -> str:
        if r is None:
            return ""
        if 1 <= r <= int(cfg["threshold_small_max"]):
            return "small"
        if r <= int(cfg["threshold_medium_max"]):
            return "medium"
        return "large"

    restaurant_flag = df["NOM COMMERCIAL"].apply(lambda x: contains_any_keywords(x, cfg["restaurant_keywords"]))
    spa_flag = df["NOM COMMERCIAL"].apply(lambda x: contains_any_keywords(x, cfg["spa_keywords"]))

    hotel_domain = df["WEBSITE"].apply(lambda u: extract_domain(u, extractor))
    group_name = hotel_domain.apply(lambda d: groups.get(d, "") if d else "")

    independent_or_group = hotel_domain.apply(lambda d: "group" if d in groups else ("unknown" if not d else "independent"))

    large_property_flag = [
        ((r is not None and r > int(cfg["threshold_large_rooms_min"])) or (c is not None and c > int(cfg["threshold_large_capacity_min"])))
        for r, c in zip(rooms, capacity)
    ]

    boutique_flag = []
    for kind, r, s in zip(independent_or_group, rooms, star_num):
        ok = kind == "independent" and r is not None and r <= int(cfg["threshold_boutique_max"])
        if pd.notna(s):
            ok = ok and s >= 3
        boutique_flag.append(bool(ok))

    contexts = [
        infer_context(t, n, c, major_cities)
        for t, n, c in zip(df["TYPE D'HÉBERGEMENT"].fillna(""), df["NOM COMMERCIAL"], df["COMMUNE"].fillna(""))
    ]

    enriched = df.copy()
    enriched["department"] = departments
    enriched["region"] = regions
    enriched["capacity_range"] = rooms.apply(cap_range)
    enriched["size_segment"] = rooms.apply(size_seg)
    enriched["restaurant_flag"] = restaurant_flag
    enriched["spa_flag"] = spa_flag
    enriched["hotel_domain"] = hotel_domain
    enriched["independent_or_group"] = independent_or_group
    enriched["group_name"] = group_name
    enriched["large_property_flag"] = large_property_flag
    enriched["boutique_flag"] = boutique_flag
    enriched["hotel_context"] = contexts
    return enriched


def main():
    parser = argparse.ArgumentParser(description="Enrich French hotels file with local business-ready columns.")
    parser.add_argument("input_file", help="Input file (.xlsx/.xls/.csv)")
    parser.add_argument("--output_dir", default=".", help="Output directory")
    parser.add_argument("--config", default="config.yaml", help="Path to config.yaml")
    args = parser.parse_args()

    input_path = Path(args.input_file)
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    logger = setup_logging(out_dir)

    config = load_config(Path(args.config))

    dep_path = Path("department_to_region_fr.csv")
    groups_path = Path("hotel_groups_domains.csv")
    cities_path = Path("major_cities_fr.txt")

    dep_to_region = load_department_to_region(dep_path)
    groups = load_group_domains(groups_path)
    major_cities = load_major_cities(cities_path)

    df = normalize_columns(read_input(input_path, logger))
    validate_columns(df)

    row_count_in = len(df)
    enriched = enrich(df, config, dep_to_region, groups, major_cities)

    # Keep exact original order + new columns at the end.
    ordered = enriched[list(df.columns) + NEW_COLUMNS]

    if len(ordered) != row_count_in:
        raise RuntimeError("Output row count mismatch with input row count")

    csv_out = out_dir / "enriched_hotels.csv"
    xlsx_out = out_dir / "enriched_hotels.xlsx"

    ordered.to_csv(csv_out, index=False, encoding="utf-8-sig", sep=",", quoting=csv.QUOTE_MINIMAL)
    try:
        ordered.to_excel(xlsx_out, index=False)
        xlsx_status = str(xlsx_out)
    except Exception as e:
        logger.warning(f"Could not write XLSX output: {e}")
        xlsx_status = "not written"

    summary = {
        "Row count read": row_count_in,
        "Valid postal code count": int((ordered["department"] != "").sum()),
        "Group count": int((ordered["independent_or_group"] == "group").sum()),
        "Independent count": int((ordered["independent_or_group"] == "independent").sum()),
        "Unknown count": int((ordered["independent_or_group"] == "unknown").sum()),
        "restaurant_flag true count": int(ordered["restaurant_flag"].sum()),
        "spa_flag true count": int(ordered["spa_flag"].sum()),
        "boutique_flag true count": int(ordered["boutique_flag"].sum()),
        "large_property_flag true count": int(ordered["large_property_flag"].sum()),
        "context urbain count": int((ordered["hotel_context"] == "urbain").sum()),
        "context loisir count": int((ordered["hotel_context"] == "loisir").sum()),
        "context inconnu count": int((ordered["hotel_context"] == "inconnu").sum()),
        "Output CSV": str(csv_out),
        "Output XLSX": xlsx_status,
    }

    logger.info("\n=== Enrichment Summary ===")
    for k, v in summary.items():
        logger.info(f"{k}: {v}")


if __name__ == "__main__":
    main()
