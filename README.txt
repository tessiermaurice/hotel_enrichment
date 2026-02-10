HOTEL ENRICHMENT SCRIPT
=======================

For Sales Guys with Little Coding Experience

WHAT THIS DOES
--------------
Takes your French hotel Excel/CSV file and adds smart columns to help you personalize sales emails:
- Location (department, region)
- Size (small/medium/large)
- Features (restaurant? spa?)
- Independent vs Chain (Accor, Louvre Hotels, etc.)
- Context (urban vs resort/leisure)
- Positioning flags (boutique, large property)


INSTALLATION (ONE TIME ONLY)
-----------------------------

1. Open Command Prompt (Windows) or Terminal (Mac)

2. Copy and paste this EXACT command:

   pip install pandas openpyxl tldextract unidecode pyyaml

3. Hit Enter and wait for it to finish


HOW TO USE
----------

1. Put your hotel CSV or Excel file in the same folder as enrich_hotels.py

2. Open Command Prompt / Terminal

3. Navigate to the project folder:
   
   cd C:\Users\tessi\OneDrive\Documents\PRO\PROJECTS\hotel_enrichment

4. Run the script:

   python enrich_hotels.py your_hotel_file.csv

   (Replace "your_hotel_file.csv" with your actual filename)

5. Get your enriched files:
   - enriched_hotels.csv
   - enriched_hotels.xlsx
   - enrich_hotels.log (shows what happened)


EXAMPLE
-------

python enrich_hotels.py contacts_FINAL_20260205_222438.csv


WHAT THE NEW COLUMNS MEAN
--------------------------

LOCATION:
- department: French department code (75 for Paris, 06 for Alpes-Maritimes, etc.)
- region: French region (Île-de-France, Provence-Alpes-Côte d'Azur, etc.)

SIZE:
- capacity_range: petite (1-30 rooms), intermediaire (31-80), grande (81+)
- size_segment: small, medium, large (same logic, English version)

AMENITIES:
- restaurant_flag: TRUE if hotel name mentions restaurant/bistro/brasserie/etc.
- spa_flag: TRUE if hotel name mentions spa/wellness/thalasso/etc.

GROUP VS INDEPENDENT:
- hotel_domain: The website domain (example.com)
- independent_or_group: Is it a chain (group) or independent?
- group_name: If it's a chain, which one? (Accor, Marriott, etc.)

POSITIONING:
- large_property_flag: TRUE if 80+ rooms or 160+ capacity
- boutique_flag: TRUE if independent, ≤25 rooms, and 3+ stars

CONTEXT:
- hotel_context: urbain (city), loisir (resort/leisure), or inconnu (unknown)


CUSTOMIZATION
-------------

Want to change thresholds or keywords? Edit config.yaml

For example:
- Change threshold_boutique_max: 25 to 30 if you want boutique to mean ≤30 rooms
- Add more restaurant keywords
- Add more spa keywords

Want to add more hotel chains? Edit hotel_groups_domains.csv

Want to add more major cities? Edit major_cities_fr.txt


HOW THE LOGIC WORKS (SIMPLIFIED)
---------------------------------

DEPARTMENT EXTRACTION:
- Takes first 2 digits of postal code (except overseas/Corsica which are special)
- 75001 → 75 (Paris)
- 06000 → 06 (Alpes-Maritimes)
- 97110 → 971 (Guadeloupe)

REGION LOOKUP:
- Uses department_to_region_fr.csv to map department → region
- 75 → Île-de-France
- 06 → Provence-Alpes-Côte d'Azur

SIZE CLASSIFICATION:
- Based on NOMBRE DE CHAMBRES (room count)
- 1-30 rooms = small/petite
- 31-80 rooms = medium/intermediaire
- 81+ rooms = large/grande

AMENITIES:
- Searches hotel name (NOM COMMERCIAL) for keywords
- Case and accent insensitive (Brasserie = brasserie = BRASSERIE)
- Restaurant: restaurant, brasserie, bistro, table, auberge, etc.
- Spa: spa, thalasso, wellness, thermes, etc.

GROUP DETECTION:
- Extracts domain from website (www.accor.com → accor.com)
- Looks up in hotel_groups_domains.csv
- If found = group, otherwise = independent
- If no website = unknown

BOUTIQUE HOTEL:
- Must be independent (not a chain)
- Must have ≤25 rooms
- Must have 3+ stars (or stars unknown)

LARGE PROPERTY:
- Either 80+ rooms OR 160+ capacity

CONTEXT:
- Checks TYPE D'HÉBERGEMENT for camping/residence/village → loisir
- Checks NOM COMMERCIAL for airport/train station/city center → urbain
- Checks NOM COMMERCIAL for beach/mountain/ski/lake/golf → loisir
- Checks if COMMUNE is in major_cities_fr.txt → urbain
- Otherwise → inconnu


TROUBLESHOOTING
---------------

ERROR: "pip: command not found"
→ Python not installed. Download from python.org

ERROR: "No module named pandas"
→ Run: pip install pandas openpyxl tldextract unidecode pyyaml

ERROR: "Missing required columns"
→ Your CSV doesn't have the expected column names. Check the column list in the error.

ERROR: "Failed to load CSV"
→ Try: python enrich_hotels.py your_file.csv
→ Script will try different encodings/delimiters automatically

The script outputs enriched_hotels.csv and enriched_hotels.xlsx
→ Check the current folder!


SUPPORT
-------

If you get stuck:
1. Check enrich_hotels.log for details
2. Make sure all files are in the same folder
3. Make sure your input file has all required columns
4. Try running with a small test file first


FILES IN THIS PROJECT
---------------------

enrich_hotels.py              - Main script
config.yaml                   - Thresholds and keywords (editable!)
department_to_region_fr.csv   - Department → Region lookup
hotel_groups_domains.csv      - Hotel chain domains
major_cities_fr.txt           - List of major French cities
README.txt                    - This file


REQUIRED INPUT COLUMNS
----------------------

Your CSV/Excel MUST have these columns (exact names):
- DATE DE CLASSEMENT
- TYPE D'HÉBERGEMENT
- STAR
- NOM COMMERCIAL
- ADRESSE
- CODE POSTAL
- COMMUNE
- WEBSITE
- CAPACITÉ D'ACCUEIL (PERSONNES)
- NOMBRE DE CHAMBRES
- Email_Primary
- Email_Additional
- Country
- Phone_Primary
- Phone_Additional
- Website_Status
- Scraping_Result

The script will NOT delete any of these. It only ADDS new columns at the end.


OUTPUT GUARANTEE
----------------

✓ All original columns preserved
✓ All rows preserved (no deletions)
✓ New columns added at the end
✓ Output row count = Input row count
✓ Both CSV and XLSX formats


THAT'S IT!
----------

Questions? Check the log file or re-run with a smaller test file to debug.

Happy selling! 🚀
