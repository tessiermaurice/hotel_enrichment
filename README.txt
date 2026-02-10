Hotel enrichment (offline, local only)
====================================

Purpose
-------
This script enriches a French hotels file with extra personalization columns for outreach.
It uses only local logic and local files (no scraping, no external APIs).

Input supported
---------------
- Excel: .xlsx or .xls
- CSV: auto-detect delimiter between comma and semicolon

For CSV input, encodings are tried in this order:
1) utf-8
2) utf-8-sig
3) cp1252
4) latin-1

Required input columns (exact names after trimming spaces)
----------------------------------------------------------
DATE DE CLASSEMENT
TYPE D'HÉBERGEMENT
STAR
NOM COMMERCIAL
ADRESSE
CODE POSTAL
COMMUNE
WEBSITE
CAPACITÉ D'ACCUEIL (PERSONNES)
NOMBRE DE CHAMBRES
Email_Primary
Email_Additional
Country
Phone_Primary
Phone_Additional
Website_Status
Scraping_Result

Install
-------
pip install pandas openpyxl tldextract unidecode pyyaml

Run
---
python enrich_hotels.py input.xlsx
or
python enrich_hotels.py input.csv

Optional arguments
------------------
--output_dir PATH     default: current folder
--config PATH         default: config.yaml

Outputs
-------
- enriched_hotels.csv (always written, UTF-8 with BOM, comma delimiter)
- enriched_hotels.xlsx (written when possible)
- enrich_hotels.log (summary + parsing warnings)

New columns appended at the end
-------------------------------
- department, region
- capacity_range, size_segment
- restaurant_flag, spa_flag
- hotel_domain, independent_or_group, group_name
- large_property_flag, boutique_flag
- hotel_context

Key logic notes
---------------
- CODE POSTAL is treated as text to preserve leading zeros.
- Department extraction includes special Corsica rule (2A/2B) and 97/98 overseas logic.
- Region is mapped using local file: department_to_region_fr.csv
- Group matching uses local file: hotel_groups_domains.csv
- Major city context fallback uses local file: major_cities_fr.txt
- Accent-insensitive matching is applied to keywords with unidecode.
- No row is dropped. Output row count must equal input row count.
- Existing columns keep original order. New columns are appended only.

Files to edit safely
--------------------
- config.yaml: thresholds and keyword lists
- department_to_region_fr.csv: department-region mapping
- hotel_groups_domains.csv: known hotel group domains
- major_cities_fr.txt: one city name per line
