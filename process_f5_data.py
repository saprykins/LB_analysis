"""
F5 Data Processing and Enrichment Script

This script processes multiple F5 Excel files, enriches them with CMDB and Global Exit data,
and generates a comprehensive analysis with multiple sheets for traceability.
"""

import pandas as pd
import numpy as np
from datetime import datetime
import glob
import re
from ipaddress import ip_address, ip_network
import warnings
warnings.filterwarnings('ignore')


# ============================================================================
# CONFIGURATION SECTION - All file names and column mappings in one place
# ============================================================================

CONFIG = {
    # Input files
    'f5_pattern': 'f5*.xlsx',  # Pattern to match F5 source files
    'cmdb_file': 'cmdb_ci_server_full.csv',
    'global_exit_file': '__Global_exit_units.xlsx',
    'global_exit_sheet': 'Worksheet',
    
    # Output file
    'output_file': 'output.xlsx',
    
    # Column names in F5 files
    'f5_columns': {
        'vip': 'VIP',
        'vip_destination': 'VIP Destination',
        'member_addrs': 'member_addrs',
        'member_names': 'member_names',
        'vip_availability': 'VIP availability',
        'environment': 'Environment'
    },
    
    # Column names in CMDB file
    'cmdb_columns': {
        'hostname': 'hostname',
        'ip_address': 'ip_address',
        'install_status': 'install_status'
    },
    
    # Column names in Global Exit file
    'global_exit_columns': {
        'hostname': 'Hostname',
        'entity': 'Entity',
        'source': 'Source',
        'host_status': 'hostStatus',
        'type': 'Type',
        'target': 'Target',
        'target_date': 'TargetDate',
        'migre_date': 'MigreDate',
        'decom_date': 'DecomDate',
        'decomed_date': 'DecomedDate',
        'migrated_by': 'MigrateBy'
    },
    
    # Filters
    'filters': {
        'vip_availability': 'available',
        'environment': 'CoreIT',
        'ip_range_start': '0.0.0.0',
        'ip_range_end': '0.0.0.53'
    },
    
    # Priority rules for selecting from Global Exit when multiple matches
    'priority_rules': {
        'type_not_equal': 'APP',
        'migrated_by_equal': 'AXA GO'
    },
    
    # Today's date for comparisons
    'today': datetime.now()
}


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def extract_ip_from_member_addr(addr_str):
    """Extract IP address from member_addrs format (e.g., '[ip_address]%1101')"""
    if pd.isna(addr_str) or addr_str == '':
        return None
    # Remove port number (after % or :)
    ip_str = str(addr_str).split('%')[0].split(':')[0].strip()
    try:
        # Validate it's a proper IP
        ip_address(ip_str)
        return ip_str
    except:
        return None


def split_multiline_cell(cell_value):
    """Split cell value by newlines and return list"""
    if pd.isna(cell_value) or cell_value == '':
        return []
    return [line.strip() for line in str(cell_value).split('\n') if line.strip()]


def is_ip_in_range(ip_str, start_ip, end_ip):
    """Check if IP is in the range between start_ip and end_ip"""
    if not ip_str:
        return False
    try:
        ip = ip_address(ip_str)
        start = ip_address(start_ip)
        end = ip_address(end_ip)
        return start <= ip <= end
    except:
        return False


def parse_date(date_str):
    """Parse date string in format DD/MM/YYYY"""
    if pd.isna(date_str) or date_str == '':
        return None
    try:
        # Try multiple date formats
        for fmt in ['%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y']:
            try:
                return datetime.strptime(str(date_str), fmt)
            except:
                continue
        return None
    except:
        return None


def select_best_global_exit_match(matches_df, config):
    """Select the best match from multiple Global Exit rows based on priority rules"""
    if len(matches_df) == 0:
        return None
    if len(matches_df) == 1:
        return matches_df.iloc[0]
    
    # Priority 1: Type != APP
    type_col = config['global_exit_columns']['type']
    type_not_app = matches_df[matches_df[type_col] != config['priority_rules']['type_not_equal']]
    if len(type_not_app) > 0:
        matches_df = type_not_app
    
    # Priority 2: MigratedBy = AXA GO
    migrated_by_col = config['global_exit_columns']['migrated_by']
    migrated_by_axa = matches_df[matches_df[migrated_by_col] == config['priority_rules']['migrated_by_equal']]
    if len(migrated_by_axa) > 0:
        return migrated_by_axa.iloc[0]
    
    # Return first match if no priority matches
    return matches_df.iloc[0]


# ============================================================================
# MAIN PROCESSING FUNCTIONS
# ============================================================================

def load_f5_files(config):
    """Load and append all F5 files"""
    print("Step 1: Loading F5 files...")
    f5_files = glob.glob(config['f5_pattern'])
    
    if not f5_files:
        raise FileNotFoundError(f"No files found matching pattern: {config['f5_pattern']}")
    
    print(f"Found {len(f5_files)} F5 files: {f5_files}")
    
    all_data = []
    for file in f5_files:
        print(f"  Reading {file}...")
        df = pd.read_excel(file)
        # Remove completely empty rows
        df = df.dropna(how='all')
        # Add source file identifier
        df['source_file'] = file
        all_data.append(df)
    
    combined_df = pd.concat(all_data, ignore_index=True)
    combined_df['row_id'] = range(1, len(combined_df) + 1)
    
    print(f"  Total rows after append: {len(combined_df)}")
    return combined_df


def apply_filters(df, config):
    """Apply filters to the data"""
    print("\nStep 2: Applying filters...")
    initial_count = len(df)
    
    cols = config['f5_columns']
    filters = config['filters']
    
    # Filter 1: VIP availability = "available"
    df = df[df[cols['vip_availability']] == filters['vip_availability']]
    print(f"  After VIP availability filter: {len(df)} rows (removed {initial_count - len(df)})")
    
    # Filter 2: Environment = "CoreIT"
    count_before = len(df)
    df = df[df[cols['environment']] == filters['environment']]
    print(f"  After Environment filter: {len(df)} rows (removed {count_before - len(df)})")
    
    # Filter 3: VIP Destination not in range 0.0.0.0 to 0.0.0.53
    count_before = len(df)
    df['_temp_vip_dest_ip'] = df[cols['vip_destination']].apply(extract_ip_from_member_addr)
    df = df[~df['_temp_vip_dest_ip'].apply(
        lambda x: is_ip_in_range(x, filters['ip_range_start'], filters['ip_range_end'])
    )]
    df = df.drop(columns=['_temp_vip_dest_ip'])
    print(f"  After IP range filter: {len(df)} rows (removed {count_before - len(df)})")
    
    return df


def expand_member_addrs(df, config):
    """Expand rows where member_addrs has multiple lines"""
    print("\nStep 3: Expanding member_addrs with multiple lines...")
    initial_count = len(df)
    
    cols = config['f5_columns']
    expanded_rows = []
    
    for idx, row in df.iterrows():
        member_addrs_list = split_multiline_cell(row[cols['member_addrs']])
        member_names_list = split_multiline_cell(row[cols['member_names']])
        
        if len(member_addrs_list) == 0:
            # Keep row as is if no member_addrs
            expanded_rows.append(row)
        else:
            # Create one row per member_addr
            for i, addr in enumerate(member_addrs_list):
                new_row = row.copy()
                new_row[cols['member_addrs']] = addr
                # Match member_names if available
                if i < len(member_names_list):
                    new_row[cols['member_names']] = member_names_list[i]
                else:
                    new_row[cols['member_names']] = ''
                expanded_rows.append(new_row)
    
    expanded_df = pd.DataFrame(expanded_rows).reset_index(drop=True)
    print(f"  Rows after expansion: {len(expanded_df)} (from {initial_count})")
    
    return expanded_df


def enrich_with_cmdb(df, config):
    """Enrich data with CMDB information (hostname, install_status)"""
    print("\nStep 4: Enriching with CMDB data...")
    
    # Load CMDB
    cmdb_df = pd.read_csv(config['cmdb_file'])
    print(f"  Loaded CMDB with {len(cmdb_df)} rows")
    
    # Extract IPs from member_addrs
    cols = config['f5_columns']
    df['extracted_ip'] = df[cols['member_addrs']].apply(extract_ip_from_member_addr)
    
    # Create lookup dictionary from CMDB
    # Handle multiple IPs in the same cell (separated by ", ")
    cmdb_cols = config['cmdb_columns']
    cmdb_lookup = {}
    for _, row in cmdb_df.iterrows():
        ip_field = str(row[cmdb_cols['ip_address']]).strip()
        if ip_field and ip_field != 'nan':
            # Split by comma and process each IP
            ips = [ip.strip() for ip in ip_field.split(',')]
            for ip in ips:
                if ip:
                    cmdb_lookup[ip] = {
                        'hostname': row[cmdb_cols['hostname']],
                        'install_status': row[cmdb_cols['install_status']]
                    }
    
    print(f"  Created CMDB lookup with {len(cmdb_lookup)} unique IPs")
    
    # Enrich
    df['hostname'] = df['extracted_ip'].apply(lambda x: cmdb_lookup.get(x, {}).get('hostname', ''))
    df['install_status'] = df['extracted_ip'].apply(lambda x: cmdb_lookup.get(x, {}).get('install_status', ''))
    
    matched = df['hostname'].notna() & (df['hostname'] != '')
    print(f"  Matched {matched.sum()} rows with CMDB ({matched.sum()/len(df)*100:.1f}%)")
    
    return df


def enrich_with_global_exit(df, config):
    """Enrich data with Global Exit information"""
    print("\nStep 5: Enriching with Global Exit data...")
    
    # Load Global Exit
    global_exit_df = pd.read_excel(
        config['global_exit_file'],
        sheet_name=config['global_exit_sheet']
    )
    print(f"  Loaded Global Exit with {len(global_exit_df)} rows")
    
    # Create lookup dictionary (hostname -> list of matching rows)
    ge_cols = config['global_exit_columns']
    global_exit_lookup = {}
    for idx, row in global_exit_df.iterrows():
        hostname = str(row[ge_cols['hostname']]).strip()
        if hostname and hostname != 'nan':
            if hostname not in global_exit_lookup:
                global_exit_lookup[hostname] = []
            global_exit_lookup[hostname].append(row)
    
    # Columns to add from Global Exit
    ge_columns_to_add = [
        'entity', 'source', 'host_status', 'type', 'target',
        'target_date', 'migre_date', 'decom_date', 'decomed_date', 'migrated_by'
    ]
    
    # Initialize new columns
    for col in ge_columns_to_add:
        df[col] = ''
    
    # Track hostnames not found
    hostnames_not_found = set()
    
    # Enrich each row
    for idx, row in df.iterrows():
        hostname = row['hostname']
        if not hostname or hostname == '':
            continue
        
        if hostname in global_exit_lookup:
            matches = pd.DataFrame(global_exit_lookup[hostname])
            best_match = select_best_global_exit_match(matches, config)
            
            if best_match is not None:
                for col in ge_columns_to_add:
                    df.at[idx, col] = best_match[ge_cols[col]]
        else:
            hostnames_not_found.add(hostname)
    
    matched = df['entity'].notna() & (df['entity'] != '')
    print(f"  Matched {matched.sum()} rows with Global Exit ({matched.sum()/len(df)*100:.1f}%)")
    print(f"  Hostnames not found: {len(hostnames_not_found)}")
    
    return df, hostnames_not_found


def create_summary_sheet(df, config):
    """Create summary sheet with VIP groupings"""
    print("\nStep 6: Creating summary sheet...")
    
    cols = config['f5_columns']
    vip_col = cols['vip']
    
    # Group by VIP
    vip_groups = df.groupby(vip_col)
    
    summary_data = []
    
    for vip, group in vip_groups:
        hostnames = group['hostname'].unique()
        hostnames = [h for h in hostnames if h and h != '']
        
        if len(hostnames) == 0:
            # Group 1: VIPs with no hostnames identified
            summary_data.append({
                'VIP': vip,
                'Group': 'Group 1: No hostnames identified in Global Exit',
                'Hostname_Count': 0,
                'Hostnames': ''
            })
            continue
        
        # Get all host statuses and decom dates for this VIP
        vip_data = group[group['hostname'].isin(hostnames)]
        host_statuses = vip_data['host_status'].unique()
        decom_dates = vip_data['decom_date'].dropna()
        types = vip_data['type'].unique()
        
        # Determine group(s)
        groups_assigned = []
        
        # Group 2: ALL hostnames have hostStatus = DECOM
        if all(vip_data[vip_data['hostname'] == h]['host_status'].iloc[0] == 'DECOM' 
               for h in hostnames if len(vip_data[vip_data['hostname'] == h]) > 0):
            groups_assigned.append('Group 2: All hostnames DECOM')
        
        # Group 3: ANY hostname has hostStatus != DECOM and Type != APP
        if any((vip_data['host_status'] != 'DECOM') & (vip_data['type'] != 'APP')):
            groups_assigned.append('Group 3: hostStatus != DECOM, Type != APP')
        
        # Group 4: ANY hostname has hostStatus != DECOM and DecomDate in future
        future_decom = False
        for _, row in vip_data.iterrows():
            if row['host_status'] != 'DECOM':
                decom_date = parse_date(row['decom_date'])
                if decom_date and decom_date > config['today']:
                    future_decom = True
                    break
        if future_decom:
            groups_assigned.append('Group 4: hostStatus != DECOM, DecomDate in future')
        
        # Group 5: ANY hostname has hostStatus != DECOM and DecomDate is empty
        if any((vip_data['host_status'] != 'DECOM') & 
               ((vip_data['decom_date'].isna()) | (vip_data['decom_date'] == ''))):
            groups_assigned.append('Group 5: hostStatus != DECOM, DecomDate empty')
        
        # Group 6: ANY hostname has hostStatus != DECOM and DecomDate in past
        past_decom = False
        for _, row in vip_data.iterrows():
            if row['host_status'] != 'DECOM':
                decom_date = parse_date(row['decom_date'])
                if decom_date and decom_date < config['today']:
                    past_decom = True
                    break
        if past_decom:
            groups_assigned.append('Group 6: hostStatus != DECOM, DecomDate in past')
        
        # Determine final group
        if len(groups_assigned) == 0:
            group_label = 'Unclassified'
        elif len(groups_assigned) > 1:
            group_label = 'Multiple Groups: ' + '; '.join(groups_assigned)
        else:
            group_label = groups_assigned[0]
        
        summary_data.append({
            'VIP': vip,
            'Group': group_label,
            'Hostname_Count': len(hostnames),
            'Hostnames': ', '.join(hostnames)
        })
    
    summary_df = pd.DataFrame(summary_data)
    print(f"  Created summary for {len(summary_df)} VIPs")
    
    return summary_df


def main():
    """Main execution function"""
    print("=" * 80)
    print("F5 Data Processing and Enrichment Script")
    print("=" * 80)
    
    config = CONFIG
    
    # Step 1: Load and append F5 files
    df_step1 = load_f5_files(config)
    
    # Step 2: Apply filters
    df_step2 = apply_filters(df_step1.copy(), config)
    
    # Step 3: Expand member_addrs
    df_step3 = expand_member_addrs(df_step2.copy(), config)
    
    # Step 4: Enrich with CMDB
    df_step4 = enrich_with_cmdb(df_step3.copy(), config)
    
    # Step 5: Enrich with Global Exit
    df_step5, hostnames_not_found = enrich_with_global_exit(df_step4.copy(), config)
    
    # Step 6: Create summary
    summary_df = create_summary_sheet(df_step5, config)
    
    # Create hostnames not found sheet
    hostnames_not_found_df = pd.DataFrame({
        'Hostname': sorted(list(hostnames_not_found))
    })
    
    # Write to Excel with multiple sheets
    print("\nStep 7: Writing output to Excel...")
    with pd.ExcelWriter(config['output_file'], engine='openpyxl') as writer:
        df_step1.to_excel(writer, sheet_name='1_After_Append', index=False)
        df_step2.to_excel(writer, sheet_name='2_After_Filters', index=False)
        df_step3.to_excel(writer, sheet_name='3_After_Expansion', index=False)
        df_step5.to_excel(writer, sheet_name='4_Final_Enriched', index=False)
        hostnames_not_found_df.to_excel(writer, sheet_name='5_Hostnames_Not_Found', index=False)
        summary_df.to_excel(writer, sheet_name='6_Summary', index=False)
    
    print(f"\n✓ Output written to: {config['output_file']}")
    print("\nSheets created:")
    print("  1. 1_After_Append - Data after appending F5 files")
    print("  2. 2_After_Filters - Data after applying filters")
    print("  3. 3_After_Expansion - Data after expanding member_addrs")
    print("  4. 4_Final_Enriched - Final enriched data")
    print("  5. 5_Hostnames_Not_Found - Hostnames not found in Global Exit")
    print("  6. 6_Summary - VIP summary by groups")
    print("\n" + "=" * 80)
    print("Processing complete!")
    print("=" * 80)


if __name__ == "__main__":
    main()
