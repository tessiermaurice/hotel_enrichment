#!/usr/bin/env python3
"""
Hotel Enrichment Script
Adds personalization columns to French hotel database for sales outreach
"""

import pandas as pd
import yaml
import sys
import os
import logging
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
            logging.FileHandler(log_path, mode='w'),
            logging.StreamHandler()
        ]
    )
    return log_path


def load_config(config_path):
    """Load configuration from YAML file"""
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        logging.info(f"✓ Config loaded from {config_path}")
        return config
    except Exception as e:
        logging.error(f"✗ Failed to load config: {e}")
        sys.exit(1)


def load_input_file(input_path):
    """Load CSV or XLSX file with intelligent encoding detection"""
    if not os.path.exists(input_path):
        logging.error(f"✗ Input file not found: {input_path}")
        sys.exit(1)
    
    file_ext = os.path.splitext(input_path)[1].lower()
    
    try:
        if file_ext == '.xlsx':
            df = pd.read_excel(input_path, dtype=str)
            logging.info(f"✓ Loaded XLSX file with {len(df)} rows")
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
                            logging.info(f"✓ Loaded CSV with encoding={encoding}, delimiter='{delimiter}', {len(df)} rows")
                            break
                    except:
                        continue
                if df is not None and len(df.columns) > 1:
                    break
            
            if df is None or len(df.columns) == 1:
                logging.error("✗ Failed to load CSV with any encoding/delimiter combination")
                sys.exit(1)
        else:
            logging.error(f"✗ Unsupported file type: {file_ext}. Use .xlsx or .csv")
            sys.exit(1)
        
        # Trim column names
        df.columns = df.columns.str.strip()
        return df
    
    except Exception as e:
        logging.error(f"✗ Failed to load input file: {e}")
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
        logging.error(f"✗ Missing required columns: {missing}")
        logging.error(f"Available columns: {list(df.columns)}")
        sys.exit(1)
    
    logging.info("✓ All required columns present")


def load_lookup_file(file_path, key_col, value_col):
    """Load a lookup CSV file into a dictionary"""
    try:
        if not os.path.exists(file_path):
            logging.warning(f"⚠ Lookup file not found: {file_path}")
            return {}
        
        df = pd.read_csv(file_path, dtype=str)
        df.columns = df.columns.str.strip()
        
        if key_col not in df.columns or value_col not in df.columns:
            logging.warning(f"⚠ Invalid columns in {file_path}")
            return {}
        
        lookup = dict(zip(df[key_col].str.strip(), df[value_col].str.strip()))
        logging.info(f"✓ Loaded {len(lookup)} entries from {file_path}")
        return lookup
    
    except Exception as e:
        logging.warning(f"⚠ Failed to load {file_path}: {e}")
        return {}


def load_text_list(file_path):
    """Load a text file as a list (one item per line)"""
    try:
        if not os.path.exists(file_path):
            logging.warning(f"⚠ List file not found: {file_path}")
            return []
        
        with open(file_path, 'r', encoding='utf-8') as f:
            items = [line.strip().lower() for line in f if line.strip()]
        
        logging.info(f"✓ Loaded {len(items)} items from {file_path}")
        return items
    
    except Exception as e:
        logging.warning(f"⚠ Failed to load {file_path}: {e}")
        return []


def extract_department(postal_code):
    """Extract department code from French postal code"""
    if pd.isna(postal_code):
        return ''
    
    postal_code = str(postal_code).strip()
    
    # Must be 5 digits
    if not postal_code.isdigit() or len(postal_code) != 5:
        return ''
    
    # Overseas departments (97x, 98x)
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


def enrich_hotels(df, config, dept_to_region, group_domains, major_cities):
    """Main enrichment function - adds all new columns"""
    
    initial_count = len(df)
    logging.info(f"Starting enrichment of {initial_count} rows...")
    
    # ===== DATA CLEANING =====
    
    # Clean CODE POSTAL (keep as string to preserve leading zeros)
    df['CODE POSTAL'] = df['CODE POSTAL'].astype(str).str.strip()
    df.loc[df['CODE POSTAL'] == 'nan', 'CODE POSTAL'] = ''
    
    # Clean STAR
    df['STAR_numeric'] = pd.to_numeric(df['STAR'], errors='coerce')
    
    # Clean NOMBRE DE CHAMBRES
    df['NOMBRE DE CHAMBRES_int'] = pd.to_numeric(df['NOMBRE DE CHAMBRES'], errors='coerce').fillna(0).astype(int)
    df.loc[df['NOMBRE DE CHAMBRES_int'] == 0, 'NOMBRE DE CHAMBRES_int'] = None
    
    # Clean CAPACITÉ D'ACCUEIL
    df['CAPACITÉ D\'ACCUEIL (PERSONNES)_int'] = pd.to_numeric(df['CAPACITÉ D\'ACCUEIL (PERSONNES)'], errors='coerce').fillna(0).astype(int)
    df.loc[df['CAPACITÉ D\'ACCUEIL (PERSONNES)_int'] == 0, 'CAPACITÉ D\'ACCUEIL (PERSONNES)_int'] = None
    
    # Clean NOM COMMERCIAL
    df['NOM COMMERCIAL'] = df['NOM COMMERCIAL'].fillna('').astype(str)
    
    # Clean WEBSITE
    df['WEBSITE'] = df['WEBSITE'].astype(str).str.strip()
    df.loc[df['WEBSITE'] == 'nan', 'WEBSITE'] = ''
    
    # Clean COMMUNE
    df['COMMUNE'] = df['COMMUNE'].fillna('').astype(str).str.strip().str.lower()
    
    # Clean TYPE D'HÉBERGEMENT
    df['TYPE D\'HÉBERGEMENT'] = df['TYPE D\'HÉBERGEMENT'].fillna('').astype(str)
    
    # ===== A. LOCATION =====
    
    logging.info("Adding location columns...")
    df['department'] = df['CODE POSTAL'].apply(extract_department)
    df['region'] = df['department'].map(dept_to_region).fillna('')
    
    valid_postal = (df['department'] != '').sum()
    logging.info(f"  Valid postal codes: {valid_postal}/{initial_count}")
    
    # ===== B. SIZE AND CAPACITY =====
    
    logging.info("Adding size columns...")
    
    def get_capacity_range(rooms):
        if pd.isna(rooms) or rooms == 0:
            return ''
        if rooms <= config['threshold_small_max']:
            return 'petite'
        elif rooms <= config['threshold_medium_max']:
            return 'intermediaire'
        else:
            return 'grande'
    
    def get_size_segment(rooms):
        if pd.isna(rooms) or rooms == 0:
            return ''
        if rooms <= config['threshold_small_max']:
            return 'small'
        elif rooms <= config['threshold_medium_max']:
            return 'medium'
        else:
            return 'large'
    
    df['capacity_range'] = df['NOMBRE DE CHAMBRES_int'].apply(get_capacity_range)
    df['size_segment'] = df['NOMBRE DE CHAMBRES_int'].apply(get_size_segment)
    
    # ===== C. AMENITIES =====
    
    logging.info("Adding amenity flags...")
    
    # Normalize keywords
    restaurant_keywords = [normalize_text(kw) for kw in config['restaurant_keywords']]
    spa_keywords = [normalize_text(kw) for kw in config['spa_keywords']]
    
    df['restaurant_flag'] = df['NOM COMMERCIAL'].apply(lambda x: contains_keywords(x, restaurant_keywords))
    df['spa_flag'] = df['NOM COMMERCIAL'].apply(lambda x: contains_keywords(x, spa_keywords))
    
    restaurant_count = df['restaurant_flag'].sum()
    spa_count = df['spa_flag'].sum()
    logging.info(f"  Restaurant mentions: {restaurant_count}")
    logging.info(f"  Spa mentions: {spa_count}")
    
    # ===== D. GROUP VS INDEPENDENT =====
    
    logging.info("Adding group/independent classification...")
    
    df['hotel_domain'] = df['WEBSITE'].apply(extract_domain)
    
    def classify_hotel(domain):
        if not domain or domain == '':
            return 'unknown'
        if domain in group_domains:
            return 'group'
        return 'independent'
    
    df['independent_or_group'] = df['hotel_domain'].apply(classify_hotel)
    df['group_name'] = df['hotel_domain'].map(group_domains).fillna('')
    
    group_count = (df['independent_or_group'] == 'group').sum()
    independent_count = (df['independent_or_group'] == 'independent').sum()
    unknown_count = (df['independent_or_group'] == 'unknown').sum()
    
    logging.info(f"  Groups: {group_count}")
    logging.info(f"  Independent: {independent_count}")
    logging.info(f"  Unknown: {unknown_count}")
    
    # ===== E. POSITIONING FLAGS =====
    
    logging.info("Adding positioning flags...")
    
    def is_large_property(row):
        rooms = row['NOMBRE DE CHAMBRES_int']
        capacity = row['CAPACITÉ D\'ACCUEIL (PERSONNES)_int']
        
        if pd.isna(rooms) and pd.isna(capacity):
            return False
        
        if not pd.isna(rooms) and rooms > config['threshold_large_rooms_min']:
            return True
        if not pd.isna(capacity) and capacity > config['threshold_large_capacity_min']:
            return True
        
        return False
    
    def is_boutique(row):
        if row['independent_or_group'] != 'independent':
            return False
        
        rooms = row['NOMBRE DE CHAMBRES_int']
        if pd.isna(rooms):
            return False
        
        if rooms > config['threshold_boutique_max']:
            return False
        
        star = row['STAR_numeric']
        if pd.isna(star):
            # If STAR missing, ignore STAR rule
            return True
        
        return star >= 3
    
    df['large_property_flag'] = df.apply(is_large_property, axis=1)
    df['boutique_flag'] = df.apply(is_boutique, axis=1)
    
    large_count = df['large_property_flag'].sum()
    boutique_count = df['boutique_flag'].sum()
    
    logging.info(f"  Large properties: {large_count}")
    logging.info(f"  Boutique hotels: {boutique_count}")
    
    # ===== F. CONTEXT =====
    
    logging.info("Adding hotel context...")
    
    # Normalize keywords
    leisure_type_keywords = ['camping', 'residence', 'village']
    urban_name_keywords = ['aeroport', 'gare', 'centre ville', 'city']
    leisure_name_keywords = ['plage', 'mer', 'montagne', 'ski', 'lac', 'golf', 'domaine', 'resort']
    
    def get_context(row):
        type_hebergement = normalize_text(row['TYPE D\'HÉBERGEMENT'])
        nom = normalize_text(row['NOM COMMERCIAL'])
        commune = row['COMMUNE']
        
        # Check type
        if any(kw in type_hebergement for kw in leisure_type_keywords):
            return 'loisir'
        
        # Check name for urban
        if any(kw in nom for kw in urban_name_keywords):
            return 'urbain'
        
        # Check name for leisure
        if any(kw in nom for kw in leisure_name_keywords):
            return 'loisir'
        
        # Check if commune is major city
        if commune in major_cities:
            return 'urbain'
        
        return 'inconnu'
    
    df['hotel_context'] = df.apply(get_context, axis=1)
    
    urbain_count = (df['hotel_context'] == 'urbain').sum()
    loisir_count = (df['hotel_context'] == 'loisir').sum()
    inconnu_count = (df['hotel_context'] == 'inconnu').sum()
    
    logging.info(f"  Urban: {urbain_count}")
    logging.info(f"  Leisure: {loisir_count}")
    logging.info(f"  Unknown: {inconnu_count}")
    
    # ===== CLEANUP =====
    
    # Remove temporary columns
    df = df.drop(columns=['STAR_numeric', 'NOMBRE DE CHAMBRES_int', 
                          'CAPACITÉ D\'ACCUEIL (PERSONNES)_int', 'COMMUNE'])
    
    final_count = len(df)
    if final_count != initial_count:
        logging.error(f"✗ ROW COUNT MISMATCH! Started with {initial_count}, ended with {final_count}")
        sys.exit(1)
    
    logging.info(f"✓ Enrichment complete. Row count verified: {final_count}")
    
    return df


def save_outputs(df, output_dir, base_name='enriched_hotels'):
    """Save enriched data to CSV and XLSX"""
    
    os.makedirs(output_dir, exist_ok=True)
    
    # Save CSV with UTF-8 BOM for Excel compatibility
    csv_path = os.path.join(output_dir, f'{base_name}.csv')
    df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    logging.info(f"✓ Saved CSV: {csv_path}")
    
    # Save XLSX
    xlsx_path = os.path.join(output_dir, f'{base_name}.xlsx')
    df.to_excel(xlsx_path, index=False, engine='openpyxl')
    logging.info(f"✓ Saved XLSX: {xlsx_path}")
    
    return csv_path, xlsx_path


def print_summary(df, log_path, csv_path, xlsx_path):
    """Print summary statistics"""
    
    summary = f"""
{'='*60}
ENRICHMENT SUMMARY
{'='*60}
Total rows: {len(df)}
Valid postal codes: {(df['department'] != '').sum()}

GROUP CLASSIFICATION:
  Groups: {(df['independent_or_group'] == 'group').sum()}
  Independent: {(df['independent_or_group'] == 'independent').sum()}
  Unknown: {(df['independent_or_group'] == 'unknown').sum()}

AMENITIES:
  Restaurant mentions: {df['restaurant_flag'].sum()}
  Spa mentions: {df['spa_flag'].sum()}

POSITIONING:
  Boutique hotels: {df['boutique_flag'].sum()}
  Large properties: {df['large_property_flag'].sum()}

CONTEXT:
  Urban: {(df['hotel_context'] == 'urbain').sum()}
  Leisure: {(df['hotel_context'] == 'loisir').sum()}
  Unknown: {(df['hotel_context'] == 'inconnu').sum()}

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
        description='Enrich French hotel database with personalization columns'
    )
    parser.add_argument('input_file', help='Input CSV or XLSX file')
    parser.add_argument('--output_dir', default='.', help='Output directory (default: current dir)')
    parser.add_argument('--config', default='config.yaml', help='Config file path')
    
    args = parser.parse_args()
    
    # Setup logging
    log_path = setup_logging(args.output_dir)
    
    logging.info("="*60)
    logging.info("HOTEL ENRICHMENT SCRIPT")
    logging.info("="*60)
    
    # Load config
    config = load_config(args.config)
    
    # Load input
    df = load_input_file(args.input_file)
    validate_columns(df)
    
    # Load lookups
    dept_to_region = load_lookup_file('department_to_region_fr.csv', 'department', 'region')
    group_domains = load_lookup_file('hotel_groups_domains.csv', 'domain', 'group_name')
    major_cities = load_text_list('major_cities_fr.txt')
    
    # Enrich
    df_enriched = enrich_hotels(df, config, dept_to_region, group_domains, major_cities)
    
    # Save
    csv_path, xlsx_path = save_outputs(df_enriched, args.output_dir)
    
    # Summary
    print_summary(df_enriched, log_path, csv_path, xlsx_path)
    
    logging.info("✓ All done!")


if __name__ == '__main__':
    main()
