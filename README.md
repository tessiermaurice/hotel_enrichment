# Hotel Enrichment Script

🏨 Enrich French hotel database with personalization columns for sales outreach

## Quick Start

```bash
# Install dependencies
pip install pandas openpyxl tldextract unidecode pyyaml

# Run the script
python enrich_hotels.py your_hotels.csv
```

## What It Does

Adds smart personalization columns to your hotel database:

- **Location**: department, region
- **Size**: capacity classification (small/medium/large)
- **Amenities**: restaurant and spa flags based on name
- **Group vs Independent**: detects hotel chains (Accor, Marriott, etc.)
- **Context**: urban vs leisure/resort classification
- **Positioning**: boutique and large property flags

## Input Requirements

Your CSV/XLSX must have these columns:
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
- Email_Primary, Email_Additional
- Country
- Phone_Primary, Phone_Additional
- Website_Status
- Scraping_Result

## Output

- `enriched_hotels.csv` - CSV with UTF-8 BOM for Excel
- `enriched_hotels.xlsx` - Excel format
- `enrich_hotels.log` - Processing log with statistics

## Customization

Edit `config.yaml` to change:
- Room count thresholds
- Restaurant keywords
- Spa keywords

Edit lookup files:
- `hotel_groups_domains.csv` - Add more hotel chains
- `major_cities_fr.txt` - Add more cities for urban detection

## How It Works

**Department Extraction**: Derives department code from postal code (handles Corsica and overseas territories)

**Region Lookup**: Maps department to French region using local CSV

**Size Classification**: Categorizes by room count (configurable thresholds)

**Amenity Detection**: Case/accent-insensitive keyword matching in hotel names

**Group Detection**: Extracts domain from website and matches against known chains

**Context Classification**: Analyzes hotel type, name, and location to determine urban vs leisure

## Features

✓ No external APIs - fully local processing  
✓ Preserves all original columns  
✓ Never drops rows  
✓ Handles CSV and XLSX input  
✓ Auto-detects CSV encoding and delimiter  
✓ Comprehensive logging  

## Example

```bash
python enrich_hotels.py contacts_FINAL_20260205_222438.csv
```

Output summary:
```
Total rows: 15000
Valid postal codes: 14823
Groups: 3421
Independent: 9234
Unknown: 2345
Restaurant mentions: 1876
Spa mentions: 543
Boutique hotels: 892
Large properties: 234
Urban: 6543
Leisure: 5234
Unknown: 3223
```

## License

Free to use for sales and business purposes
