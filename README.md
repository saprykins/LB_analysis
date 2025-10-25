# F5 Data Processing and Enrichment Tool

Python script for processing F5 load balancer data, enriching it with CMDB and Global Exit information, and generating comprehensive analysis reports.

## Overview

This tool consolidates multiple F5 Excel files, filters and transforms the data, enriches it with hostname and migration information, and produces a multi-sheet Excel report with full traceability.

## Requirements

- Python 3.7+
- pandas
- openpyxl

Install dependencies:
```bash
pip install pandas openpyxl
```

## Input Files

The script expects the following files in the same directory:

1. **F5 Files**: `f5*.xlsx` (e.g., f5_prod.xlsx, f5_dev.xlsx, f5_test.xlsx)
   - Must contain columns: VIP, VIP Destination, member_addrs, member_names, VIP availability, Environment
   
2. **CMDB File**: `cmdb_ci_server_full.csv`
   - Must contain columns: hostname, ip_address, install_status
   
3. **Global Exit File**: `__Global_exit_unit.xlsx`
   - Sheet name: "Worksheet"
   - Must contain columns: Hostname, Entity, Source, HostStatus, Type, Target, TargetDate, MigreDate, DecomDate, DecomedDate, MigratedBy

## Configuration

All file names and column mappings are defined in the `CONFIG` dictionary at the top of `process_f5_data.py`. Modify this section if your files have different names or column headers.

```python
CONFIG = {
    'f5_pattern': 'f5*.xlsx',
    'cmdb_file': 'cmdb_ci_server_full.csv',
    'global_exit_file': '__Global_exit_unit.xlsx',
    'output_file': 'output.xlsx',
    # ... column mappings ...
}
```

## Usage

Run the script from the command line:

```bash
python process_f5_data.py
```

The script will process all matching files and create `output.xlsx` with multiple sheets.

## Processing Steps

### 1. Data Append
- Loads all f5*.xlsx files
- Removes empty rows
- Adds `source_file` column to track origin
- Assigns unique `row_id` to each row

### 2. Filtering
Applies three filters:
- **VIP availability** = "available"
- **Environment** = "CoreIT"
- **VIP Destination** not in IP range 0.0.0.0 to 0.0.0.53

### 3. Member Address Expansion
- Splits multi-line `member_addrs` cells into separate rows
- Each row contains only one IP address
- Duplicates all other columns for each IP
- Matches corresponding `member_names` when available

### 4. CMDB Enrichment
- Extracts IP addresses from `member_addrs` (removes port numbers like %1101)
- Matches IPs with CMDB data
- Adds columns: `hostname`, `install_status`

### 5. Global Exit Enrichment
- Matches hostnames with Global Exit data
- When multiple matches exist, applies priority rules:
  1. Type != "APP"
  2. MigratedBy = [specific value]
- Adds columns: `entity`, `source`, `host_status`, `type`, `target`, `target_date`, `migre_date`, `decom_date`, `decomed_date`, `migrated_by`

### 6. Summary Generation
Groups VIPs into categories based on hostname analysis:
- **Group 1**: No hostnames identified in Global Exit
- **Group 2**: All hostnames have HostStatus = DECOM
- **Group 3**: Any hostname has HostStatus != DECOM and Type != APP
- **Group 4**: Any hostname has HostStatus != DECOM and DecomDate in future
- **Group 5**: Any hostname has HostStatus != DECOM and DecomDate is empty
- **Group 6**: Any hostname has HostStatus != DECOM and DecomDate in past
- **Multiple Groups**: VIPs with hostnames falling into different categories

## Output

The script generates `output.xlsx` with 6 sheets:

### Sheet 1: 1_After_Append
Raw data after appending all F5 files with source tracking.

### Sheet 2: 2_After_Filters
Data after applying VIP availability, Environment, and IP range filters.

### Sheet 3: 3_After_Expansion
Data after expanding multi-line member_addrs into separate rows.

### Sheet 4: 4_Final_Enriched
Complete dataset with all CMDB and Global Exit enrichments.

### Sheet 5: 5_Hostnames_Not_Found
List of hostnames that couldn't be matched in Global Exit file.

### Sheet 6: 6_Summary
VIP-level summary showing group classifications and hostname counts.

## Data Flow Example

```
F5 File 1 (10 rows) ─┐
F5 File 2 (15 rows) ─┼─> Append (25 rows)
F5 File 3 (8 rows)  ─┘
                      │
                      ├─> Filter (20 rows remain)
                      │
                      ├─> Expand member_addrs (35 rows - some had multiple IPs)
                      │
                      ├─> Enrich with CMDB (add hostname, install_status)
                      │
                      ├─> Enrich with Global Exit (add migration data)
                      │
                      └─> Generate Summary (15 unique VIPs grouped)
```

## Troubleshooting

**No files found matching pattern**
- Ensure F5 files start with "f5" and have .xlsx extension
- Check files are in the same directory as the script

**Column not found errors**
- Verify column names in your input files match the CONFIG section
- Update CONFIG dictionary if your files use different column names

**Date parsing issues**
- Dates should be in format DD/MM/YYYY (e.g., 20/09/2024)
- Script also supports YYYY-MM-DD and DD-MM-YYYY formats

**IP matching issues**
- IPs in member_addrs can include port numbers (e.g., [ip_address]%1101)
- Script automatically strips port numbers before matching with CMDB

## Notes

- Empty rows in source files are automatically removed
- Multi-line cells in member_addrs are split by newline characters
- When a VIP has multiple hostnames, each is evaluated independently for grouping
- Date comparisons use the current system date
- The script preserves all original columns from F5 files in the output

## Support

For issues or questions, review the console output which provides detailed progress information for each processing step.
