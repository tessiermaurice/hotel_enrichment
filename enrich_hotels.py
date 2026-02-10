#!/usr/bin/env python3
"""
Hotel Enrichment Script V2 - French Personalization Edition
Adds personalization columns to French hotel database for sales outreach
"""

import pandas as pd
import yaml
import sys
import os
import logging
import re
from pathlib import Path
from unidecode import unidecode
import tldextract
import argparse


def setup_logging(output_dir):
    """Setup logging to file and console"""
    log_path = os.path.join(output_dir, 'enrich_hotels.log')
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_path, mode='w', encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    # Force UTF-8 for console output on Windows
    if sys.stdout.encoding != 'utf-8':
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    return log_path


def load_config(config_path):
    """Load configuration from YAML file"""
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        logging.info(f"[OK] Config loaded from {config_path}")
        return config
    except Exception as e:
        logging.error(f"[ERROR] Failed to load config: {e}")
        sys.exit(1)


def load_input_file(input_path):
    """Load CSV or XLSX file with intelligent encoding detection"""
    if not os.path.exists(input_path):
        logging.error(f"[ERROR] Input file not found: {input_path}")
        sys.exit(1)
    
    file_ext = os.path.splitext(input_path)[1].lower()
    
    try:
        if file_ext == '.xlsx':
            df = pd.read_excel(input_path, dtype=str)
            logging.info(f"[OK] Loaded XLSX file with {len(df)} rows")
        elif file_ext == '.csv':
            # Try different encodings and delimiters
            encodings = ['utf-8', 'utf-8-sig', 'cp1252', 'latin-1']
            delimiters = [',', ';']
            
            df = None
            for encoding in encodings:
                for delimiter in delimiters:
                    try:
                        df = pd.read_csv(input_path, encoding=encoding, sep=delimiter, dtype=str)
                        # Check if we got valid columns (more than 1 column means delimiter worked)
                        if len(df.columns) > 1:
                            logging.info(f"[OK] Loaded CSV with encoding={encoding}, delimiter='{delimiter}', {len(df)} rows")
                            break
                    except:
                        continue
                if df is not None and len(df.columns) > 1:
                    break
            
            if df is None or len(df.columns) == 1:
                logging.error("[ERROR] Failed to load CSV with any encoding/delimiter combination")
                sys.exit(1)
        else:
            logging.error(f"[ERROR] Unsupported file type: {file_ext}. Use .xlsx or .csv")
            sys.exit(1)
        
        # Trim column names
        df.columns = df.columns.str.strip()
        return df
    
    except Exception as e:
        logging.error(f"[ERROR] Failed to load input file: {e}")
        sys.exit(1)


def validate_columns(df):
    """Validate that all required columns exist"""
    required_columns = [
        'DATE DE CLASSEMENT', 'TYPE D\'HÉBERGEMENT', 'STAR', 'NOM COMMERCIAL',
        'ADRESSE', 'CODE POSTAL', 'COMMUNE', 'WEBSITE',
        'CAPACITÉ D\'ACCUEIL (PERSONNES)', 'NOMBRE DE CHAMBRES',
        'Email_Primary', 'Email_Additional', 'Country',
        'Phone_Primary', 'Phone_Additional', 'Website_Status', 'Scraping_Result'
    ]
    
    missing = [col for col in required_columns if col not in df.columns]
    
    if missing:
        logging.error(f"[ERROR] Missing required columns: {missing}")
        logging.error(f"Available columns: {list(df.columns)}")
        sys.exit(1)
    
    logging.info("[OK] All required columns present")


def load_lookup_file(file_path, key_col, value_col):
    """Load a lookup CSV file into a dictionary"""
    try:
        if not os.path.exists(file_path):
            logging.warning(f"[WARN] Lookup file not found: {file_path}")
            return {}
        
        df = pd.read_csv(file_path, dtype=str)
        df.columns = df.columns.str.strip()
        
        if key_col not in df.columns or value_col not in df.columns:
            logging.warning(f"[WARN] Invalid columns in {file_path}")
            return {}
        
        lookup = dict(zip(df[key_col].str.strip(), df[value_col].str.strip()))
        logging.info(f"[OK] Loaded {len(lookup)} entries from {file_path}")
        return lookup
    
    except Exception as e:
        logging.warning(f"[WARN] Failed to load {file_path}: {e}")
        return {}


def load_department_lookup(file_path):
    """Load department lookup with both code->name and code->region mappings"""
    try:
        if not os.path.exists(file_path):
            logging.warning(f"[WARN] Department lookup file not found: {file_path}")
            return {}, {}
        
        df = pd.read_csv(file_path, dtype=str)
        df.columns = df.columns.str.strip()
        
        # Create two mappings
        code_to_name = {}
        code_to_region = {}
        
        if 'department' in df.columns and 'department_name' in df.columns and 'region' in df.columns:
            for _, row in df.iterrows():
                code = str(row['department']).strip()
                code_to_name[code] = str(row['department_name']).strip()
                code_to_region[code] = str(row['region']).strip()
        
        logging.info(f"[OK] Loaded {len(code_to_name)} department mappings")
        return code_to_name, code_to_region
    
    except Exception as e:
        logging.warning(f"[WARN] Failed to load department lookup: {e}")
        return {}, {}


def clean_postal_code(postal_code):
    """Clean postal code - remove decimals and ensure it's a proper 5-digit string"""
    if pd.isna(postal_code):
        return ''
    
    # Convert to string and remove any decimal points
    postal_str = str(postal_code).strip()
    
    # Remove .0 suffix if present
    if '.' in postal_str:
        postal_str = postal_str.split('.')[0]
    
    # Pad with leading zeros if needed (in case it's stored as integer and lost leading zeros)
    if postal_str.isdigit() and len(postal_str) < 5:
        postal_str = postal_str.zfill(5)
    
    return postal_str


def extract_department_code(postal_code):
    """Extract department code from French postal code"""
    postal_code = clean_postal_code(postal_code)
    
    if not postal_code or len(postal_code) != 5 or not postal_code.isdigit():
        return ''
    
    # Overseas departments (97x, 98x) - use first 3 digits
    if postal_code.startswith('97') or postal_code.startswith('98'):
        return postal_code[:3]
    
    # Corsica special cases
    if postal_code.startswith('20'):
        code_int = int(postal_code)
        if 20000 <= code_int <= 20199:
            return '2A'
        elif 20200 <= code_int <= 20699:
            return '2B'
        else:
            return postal_code[:2]
    
    # Standard case: first 2 digits
    return postal_code[:2]


def to_proper_case(text):
    """Convert text to proper case, handling French articles and prepositions"""
    if pd.isna(text) or not text:
        return ''
    
    text = str(text).strip()
    
    # List of words that should stay lowercase (French articles and prepositions)
    lowercase_words = {
        'le', 'la', 'les', 'l', 'de', 'des', 'du', 'd', 'et', 'à', 'au', 'aux',
        'en', 'un', 'une', 'sur', 'sous', 'pour', 'par', 'avec', 'sans'
    }
    
    words = text.split()
    result = []
    
    for i, word in enumerate(words):
        # First word is always capitalized
        if i == 0:
            result.append(word.capitalize())
        # Check if word (without punctuation) is in lowercase list
        elif word.lower().strip('.,;:!?()[]{}"\'-') in lowercase_words:
            result.append(word.lower())
        else:
            result.append(word.capitalize())
    
    return ' '.join(result)


def clean_hotel_name(name):
    """
    Clean hotel name by removing prefixes, legal suffixes, and chain names.
    This is the TRICKY part - we want ONLY the actual hotel name.
    
    Examples:
    - "Palace Hôtel Spa Le Beaumarchais SAS" -> "Le Beaumarchais"
    - "Camping aux 3 flots SARL" -> "Aux 3 Flots"
    - "Résidence Pierre et Vacances Les Terrasses" -> "Les Terrasses"
    """
    if pd.isna(name) or not name:
        return ''
    
    name = str(name).strip()
    
    # STEP 1: Remove legal suffixes (SAS, SARL, etc.)
    legal_suffixes = [
        r'\bSAS\b', r'\bSARL\b', r'\bSA\b', r'\bSNC\b', r'\bEURL\b', 
        r'\bSCI\b', r'\bSCM\b', r'\bSCP\b', r'\bSELARL\b', r'\bSEL\b',
        r'\bLtd\b', r'\bLLC\b', r'\bInc\b', r'\bCorp\b', r'\bGmbH\b'
    ]
    
    for suffix in legal_suffixes:
        name = re.sub(suffix, '', name, flags=re.IGNORECASE)
    
    # STEP 2: Remove common hotel/accommodation type prefixes
    type_prefixes = [
        r'\bHôtel\b', r'\bHotel\b', r'\bHotêl\b',  # Common misspellings
        r'\bPalace\b',
        r'\bCamping\b', r'\bCamp\b',
        r'\bRésidence\b', r'\bResidence\b',
        r'\bVillage\b',
        r'\bAppart\'?hôtel\b', r'\bApparthotel\b', r'\bAppart\'?hotel\b',
        r'\bAuberge\b',
        r'\bRelais\b',
        r'\bManoir\b',
        r'\bChâteau\b', r'\bChateau\b',
        r'\bMaison\b',
        r'\bDomaine\b',
        r'\bGîte\b', r'\bGite\b',
        r'\bLodge\b',
        r'\bHostel\b', r'\bHostellerie\b'
    ]
    
    for prefix in type_prefixes:
        name = re.sub(prefix, '', name, flags=re.IGNORECASE)
    
    # STEP 3: Remove amenity-related words
    amenity_words = [
        r'\bRestaurant\b', r'\bBrasserie\b', r'\bBistro\b', r'\bBistrot\b',
        r'\bSpa\b', r'\bThalasso\b', r'\bWellness\b', r'\bThermes\b',
        r'\bGolf\b', r'\bResort\b',
        r'\bBar\b', r'\bCafé\b', r'\bCafe\b',
        r'\bClub\b'
    ]
    
    for word in amenity_words:
        name = re.sub(word, '', name, flags=re.IGNORECASE)
    
    # STEP 4: Remove common chain/brand names
    # This is important to get ONLY the hotel's actual name
    chain_names = [
        r'\bPierre\s+et\s+Vacances\b', r'\bPierre\s+&\s+Vacances\b',
        r'\bBelambra\b', r'\bVVF\b', r'\bVacancéole\b', r'\bVacanceole\b',
        r'\bOdalys\b', r'\bLagrange\b', r'\bNemea\b', r'\bGoélia\b', r'\bGoelia\b',
        r'\bMaeva\b', r'\bLes\s+Balcons\b', r'\bLa\s+Plagne\b',
        r'\bCenter\s+Parcs\b', r'\bSunêlia\b', r'\bSunelia\b',
        r'\bYelloh\s+Village\b', r'\bCastels\b', r'\bSandaya\b',
        r'\bHomair\b', r'\bEurocamp\b', r'\bCanvas\b',
        # Big hotel chains (just in case they appear in name)
        r'\bAccor\b', r'\bIbis\b', r'\bNovotel\b', r'\bMercure\b', r'\bSofitel\b',
        r'\bCampanile\b', r'\bKyriad\b', r'\bPremiere\s+Classe\b', r'\bB&B\s+Hotels\b',
        r'\bBest\s+Western\b', r'\bHoliday\s+Inn\b', r'\bMarriott\b', r'\bHilton\b'
    ]
    
    for chain in chain_names:
        name = re.sub(chain, '', name, flags=re.IGNORECASE)
    
    # STEP 5: Remove star ratings if present
    name = re.sub(r'\b\d+\s*étoiles?\b', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\b\d+\s*stars?\b', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\*+', '', name)
    
    # STEP 6: Clean up extra whitespace and punctuation
    name = re.sub(r'\s+', ' ', name)  # Multiple spaces to single space
    name = re.sub(r'^[\s\-,;:]+', '', name)  # Leading punctuation
    name = re.sub(r'[\s\-,;:]+$', '', name)  # Trailing punctuation
    name = name.strip()
    
    # STEP 7: If name is empty after cleaning, return original (better than nothing)
    if not name:
        return ''
    
    # STEP 8: Convert to proper case
    name = to_proper_case(name)
    
    return name


def normalize_text(text):
    """Normalize text for case-insensitive, accent-insensitive matching"""
    if pd.isna(text):
        return ''
    return unidecode(str(text)).lower()


def contains_keywords(text, keywords):
    """Check if text contains any of the keywords (case/accent insensitive)"""
    normalized = normalize_text(text)
    return any(keyword in normalized for keyword in keywords)


def extract_domain(url):
    """Extract registrable domain from URL"""
    if pd.isna(url) or not str(url).strip():
        return ''
    
    try:
        extracted = tldextract.extract(str(url).strip())
        if extracted.domain and extracted.suffix:
            return f"{extracted.domain}.{extracted.suffix}"
        return ''
    except:
        return ''


def enrich_hotels(df, config, dept_to_name, dept_to_region, group_domains):
    """Main enrichment function - adds all new columns"""
    
    initial_count = len(df)
    logging.info(f"Starting enrichment of {initial_count} rows...")
    
    # ===== DATA CLEANING =====
    
    logging.info("Cleaning data...")
    
    # Clean CODE POSTAL - CRITICAL FIX for the .0 issue
    df['CODE POSTAL_cleaned'] = df['CODE POSTAL'].apply(clean_postal_code)
    
    # Clean STAR
    df['STAR_numeric'] = pd.to_numeric(df['STAR'], errors='coerce')
    
    # Clean NOMBRE DE CHAMBRES
    df['NOMBRE DE CHAMBRES_int'] = pd.to_numeric(df['NOMBRE DE CHAMBRES'], errors='coerce').fillna(0).astype(int)
    df.loc[df['NOMBRE DE CHAMBRES_int'] == 0, 'NOMBRE DE CHAMBRES_int'] = None
    
    # Clean NOM COMMERCIAL
    df['NOM COMMERCIAL'] = df['NOM COMMERCIAL'].fillna('').astype(str)
    
    # Clean WEBSITE
    df['WEBSITE'] = df['WEBSITE'].astype(str).str.strip()
    df.loc[df['WEBSITE'] == 'nan', 'WEBSITE'] = ''
    
    # Clean TYPE D'HÉBERGEMENT
    df['TYPE D\'HÉBERGEMENT'] = df['TYPE D\'HÉBERGEMENT'].fillna('').astype(str)
    
    # ===== 1. HOTEL NAME (CLEANED) =====
    
    logging.info("Cleaning hotel names...")
    df['nom_hotel'] = df['NOM COMMERCIAL'].apply(clean_hotel_name)
    
    # ===== 2. LOCATION (CODE DEPARTEMENT, DEPARTEMENT NAME, REGION) =====
    
    logging.info("Adding location columns...")
    
    # Extract department code
    df['code_departement'] = df['CODE POSTAL_cleaned'].apply(extract_department_code)
    
    # Map to department name
    df['departement'] = df['code_departement'].map(dept_to_name).fillna('')
    
    # Map to region
    df['region'] = df['code_departement'].map(dept_to_region).fillna('')
    
    valid_postal = (df['code_departement'] != '').sum()
    logging.info(f"  Valid postal codes: {valid_postal}/{initial_count}")
    
    # ===== 3. TYPE (from TYPE D'HÉBERGEMENT) =====
    
    logging.info("Adding type column...")
    df['type'] = df['TYPE D\'HÉBERGEMENT'].fillna('')
    
    # ===== 4. TAILLE (SIZE in French) =====
    
    logging.info("Adding size column...")
    
    def get_taille(rooms):
        if pd.isna(rooms) or rooms == 0:
            return '0'
        if rooms <= config['threshold_small_max']:
            return 'petite'
        elif rooms <= config['threshold_medium_max']:
            return 'intermédiaire'
        else:
            return 'grande'
    
    df['taille'] = df['NOMBRE DE CHAMBRES_int'].apply(get_taille)
    
    # ===== 5. STATUT (independent/group in French) =====
    
    logging.info("Adding group/independent classification...")
    
    df['hotel_domain'] = df['WEBSITE'].apply(extract_domain)
    
    def classify_statut(domain):
        if not domain or domain == '':
            return '0'
        if domain in group_domains:
            return 'groupe'
        return 'indépendant'
    
    df['statut'] = df['hotel_domain'].apply(classify_statut)
    
    # ===== 6. GROUPE (group name or 0) =====
    
    df['groupe'] = df['hotel_domain'].map(group_domains).fillna('0')
    
    group_count = (df['statut'] == 'groupe').sum()
    independent_count = (df['statut'] == 'indépendant').sum()
    unknown_count = (df['statut'] == '0').sum()
    
    logging.info(f"  Groups: {group_count}")
    logging.info(f"  Independent: {independent_count}")
    logging.info(f"  Unknown: {unknown_count}")
    
    # ===== 7. RESTAURANT (restaurant or 0) =====
    
    logging.info("Adding amenity columns...")
    
    # Normalize keywords
    restaurant_keywords = [normalize_text(kw) for kw in config['restaurant_keywords']]
    spa_keywords = [normalize_text(kw) for kw in config['spa_keywords']]
    
    df['restaurant_flag_temp'] = df['NOM COMMERCIAL'].apply(lambda x: contains_keywords(x, restaurant_keywords))
    df['restaurant'] = df['restaurant_flag_temp'].apply(lambda x: 'restaurant' if x else '0')
    
    # ===== 8. SPA (spa or 0) =====
    
    df['spa_flag_temp'] = df['NOM COMMERCIAL'].apply(lambda x: contains_keywords(x, spa_keywords))
    df['spa'] = df['spa_flag_temp'].apply(lambda x: 'spa' if x else '0')
    
    restaurant_count = (df['restaurant'] == 'restaurant').sum()
    spa_count = (df['spa'] == 'spa').sum()
    
    logging.info(f"  Restaurant mentions: {restaurant_count}")
    logging.info(f"  Spa mentions: {spa_count}")
    
    # ===== CLEANUP TEMPORARY COLUMNS =====
    
    columns_to_drop = [
        'CODE POSTAL_cleaned', 'STAR_numeric', 'NOMBRE DE CHAMBRES_int',
        'hotel_domain', 'restaurant_flag_temp', 'spa_flag_temp'
    ]
    
    df = df.drop(columns=columns_to_drop)
    
    # ===== VERIFY ROW COUNT =====
    
    final_count = len(df)
    if final_count != initial_count:
        logging.error(f"[ERROR] ROW COUNT MISMATCH! Started with {initial_count}, ended with {final_count}")
        sys.exit(1)
    
    logging.info(f"[OK] Enrichment complete. Row count verified: {final_count}")
    
    return df


def save_outputs(df, output_dir, base_name='enriched_hotels'):
    """Save enriched data to CSV and XLSX"""
    
    os.makedirs(output_dir, exist_ok=True)
    
    # Save CSV with UTF-8 BOM for Excel compatibility
    csv_path = os.path.join(output_dir, f'{base_name}.csv')
    df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    logging.info(f"[OK] Saved CSV: {csv_path}")
    
    # Clean data for Excel (remove illegal characters)
    df_excel = df.copy()
    for col in df_excel.columns:
        if df_excel[col].dtype == 'object':
            # Remove illegal XML characters that Excel can't handle
            df_excel[col] = df_excel[col].astype(str).apply(
                lambda x: ''.join(char for char in x if ord(char) >= 32 or char in '\n\r\t') if x != 'nan' else ''
            )
    
    # Save XLSX
    xlsx_path = os.path.join(output_dir, f'{base_name}.xlsx')
    df_excel.to_excel(xlsx_path, index=False, engine='openpyxl')
    logging.info(f"[OK] Saved XLSX: {xlsx_path}")
    
    return csv_path, xlsx_path


def print_summary(df, log_path, csv_path, xlsx_path):
    """Print summary statistics"""
    
    summary = f"""
{'='*60}
ENRICHMENT SUMMARY
{'='*60}
Total rows: {len(df)}
Valid postal codes: {(df['code_departement'] != '').sum()}

LOCATION:
  Departments identified: {df['departement'].ne('').sum()}
  Regions identified: {df['region'].ne('').sum()}

HOTEL NAMES:
  Names cleaned: {df['nom_hotel'].ne('').sum()}

GROUP CLASSIFICATION:
  Groups: {(df['statut'] == 'groupe').sum()}
  Independent: {(df['statut'] == 'indépendant').sum()}
  Unknown: {(df['statut'] == '0').sum()}

SIZE:
  Petite: {(df['taille'] == 'petite').sum()}
  Intermédiaire: {(df['taille'] == 'intermédiaire').sum()}
  Grande: {(df['taille'] == 'grande').sum()}

AMENITIES:
  With restaurant: {(df['restaurant'] == 'restaurant').sum()}
  With spa: {(df['spa'] == 'spa').sum()}

OUTPUT FILES:
  CSV: {csv_path}
  XLSX: {xlsx_path}
  Log: {log_path}
{'='*60}
"""
    
    print(summary)
    logging.info(summary)


def main():
    parser = argparse.ArgumentParser(
        description='Enrich French hotel database with personalization columns (French edition)'
    )
    parser.add_argument('input_file', help='Input CSV or XLSX file')
    parser.add_argument('--output_dir', default='.', help='Output directory (default: current dir)')
    parser.add_argument('--config', default='config.yaml', help='Config file path')
    
    args = parser.parse_args()
    
    # Setup logging
    log_path = setup_logging(args.output_dir)
    
    logging.info("="*60)
    logging.info("HOTEL ENRICHMENT SCRIPT V2 - FRENCH EDITION")
    logging.info("="*60)
    
    # Load config
    config = load_config(args.config)
    
    # Load input
    df = load_input_file(args.input_file)
    validate_columns(df)
    
    # Load lookups
    dept_to_name, dept_to_region = load_department_lookup('department_to_region_fr.csv')
    group_domains = load_lookup_file('hotel_groups_domains.csv', 'domain', 'group_name')
    
    # Enrich
    df_enriched = enrich_hotels(df, config, dept_to_name, dept_to_region, group_domains)
    
    # Save
    csv_path, xlsx_path = save_outputs(df_enriched, args.output_dir)
    
    # Summary
    print_summary(df_enriched, log_path, csv_path, xlsx_path)
    
    logging.info("[OK] All done!")


if __name__ == '__main__':
    main()
