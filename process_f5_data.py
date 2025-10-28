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

# Visualization libraries
try:
    import matplotlib.pyplot as plt
    import seaborn as sns
    from upsetplot import UpSet, from_memberships
    VISUALIZATION_AVAILABLE = True
except ImportError:
    VISUALIZATION_AVAILABLE = False
    print("Warning: Visualization libraries not available. Install with: pip install matplotlib seaborn upsetplot")


# ============================================================================
# CONFIGURATION SECTION - All file names and column mappings in one place
# ============================================================================

CONFIG = {
    # Input files
    'f5_pattern': 'f5*.xlsx',  # Pattern to match F5 source files
    'cmdb_file': 'cmdb_ci_server_full.csv',
    'global_exit_file': '__Global_exit_units_.xlsx',
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


def clean_hostname(hostname):
    """Remove timestamp from hostname (e.g., 'lu1app88_2018-02-08 17:52:14' -> 'lu1app88')"""
    if pd.isna(hostname) or hostname == '':
        return ''
    hostname_str = str(hostname).strip()
    # Remove timestamp pattern: _YYYY-MM-DD HH:MM:SS or similar
    # Split by underscore and check if last part looks like a date
    if '_' in hostname_str:
        parts = hostname_str.split('_')
        # Check if last part starts with a year pattern (19xx or 20xx)
        if len(parts) > 1 and re.match(r'^(19|20)\d{2}-', parts[-1]):
            # Remove the timestamp part
            return '_'.join(parts[:-1])
    return hostname_str


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
    
    # Debug: Show unique values before filtering
    print(f"\n  DEBUG - Unique values in '{cols['vip_availability']}':")
    print(f"    {df[cols['vip_availability']].unique()[:10]}")
    print(f"  DEBUG - Unique values in '{cols['environment']}':")
    print(f"    {df[cols['environment']].unique()[:10]}")
    
    # Filter 1: VIP availability = "available"
    count_before = len(df)
    df = df[df[cols['vip_availability']] == filters['vip_availability']]
    print(f"\n  After VIP availability filter ('{filters['vip_availability']}'): {len(df)} rows (removed {count_before - len(df)})")
    
    # Filter 2: Environment = "CoreIT"
    count_before = len(df)
    df = df[df[cols['environment']] == filters['environment']]
    print(f"  After Environment filter ('{filters['environment']}'): {len(df)} rows (removed {count_before - len(df)})")
    
    # Filter 3: VIP Destination not in range 0.0.0.0 to 0.0.0.53
    count_before = len(df)
    df['_temp_vip_dest_ip'] = df[cols['vip_destination']].apply(extract_ip_from_member_addr)
    
    # Debug: Show sample IPs and which are in range
    print(f"\n  DEBUG - Sample VIP Destination IPs:")
    sample_ips = df['_temp_vip_dest_ip'].dropna().head(10).tolist()
    for ip in sample_ips:
        in_range = is_ip_in_range(ip, filters['ip_range_start'], filters['ip_range_end'])
        print(f"    {ip} - In range: {in_range}")
    
    # Count how many are in the excluded range
    in_range_count = df['_temp_vip_dest_ip'].apply(
        lambda x: is_ip_in_range(x, filters['ip_range_start'], filters['ip_range_end'])
    ).sum()
    print(f"  DEBUG - IPs in excluded range {filters['ip_range_start']} to {filters['ip_range_end']}: {in_range_count}")
    
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
    
    # Create lookup dictionary from CMDB (case-insensitive for IPs)
    # Handle multiple IPs in the same cell (separated by ", ")
    cmdb_cols = config['cmdb_columns']
    cmdb_lookup = {}
    for _, row in cmdb_df.iterrows():
        ip_field = str(row[cmdb_cols['ip_address']]).strip()
        if ip_field and ip_field != 'nan':
            # Split by comma and process each IP
            ips = [ip.strip().lower() for ip in ip_field.split(',')]
            for ip in ips:
                if ip:
                    cmdb_lookup[ip] = {
                        'hostname': row[cmdb_cols['hostname']],
                        'install_status': row[cmdb_cols['install_status']]
                    }
    
    print(f"  Created CMDB lookup with {len(cmdb_lookup)} unique IPs")
    
    # Track IPs not found in CMDB
    ips_not_found = set()
    
    # Enrich (convert extracted IP to lowercase for matching)
    def get_hostname(ip):
        if not ip:
            return ''
        ip_lower = ip.lower()
        if ip_lower in cmdb_lookup:
            hostname = cmdb_lookup[ip_lower].get('hostname', '')
            return clean_hostname(hostname)
        else:
            ips_not_found.add(ip)
            return ''
    
    def get_install_status(ip):
        if not ip:
            return ''
        ip_lower = ip.lower()
        return cmdb_lookup.get(ip_lower, {}).get('install_status', '')
    
    df['hostname'] = df['extracted_ip'].apply(get_hostname)
    df['install_status'] = df['extracted_ip'].apply(get_install_status)
    
    matched = df['hostname'].notna() & (df['hostname'] != '')
    print(f"  Matched {matched.sum()} rows with CMDB ({matched.sum()/len(df)*100:.1f}%)")
    print(f"  IPs not found: {len(ips_not_found)}")
    
    return df, ips_not_found


def enrich_with_global_exit(df, config):
    """Enrich data with Global Exit information"""
    print("\nStep 5: Enriching with Global Exit data...")
    
    # Load Global Exit
    global_exit_df = pd.read_excel(
        config['global_exit_file'],
        sheet_name=config['global_exit_sheet']
    )
    print(f"  Loaded Global Exit with {len(global_exit_df)} rows")
    
    # Create lookup dictionary (hostname -> list of matching rows) - case-insensitive
    ge_cols = config['global_exit_columns']
    global_exit_lookup = {}
    for idx, row in global_exit_df.iterrows():
        hostname = str(row[ge_cols['hostname']]).strip().lower()
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
    
    # Enrich each row (case-insensitive hostname matching)
    for idx, row in df.iterrows():
        hostname = row['hostname']
        if not hostname or hostname == '':
            continue
        
        hostname_lower = str(hostname).strip().lower()
        
        if hostname_lower in global_exit_lookup:
            matches = pd.DataFrame(global_exit_lookup[hostname_lower])
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


def create_group_detail_sheets(df, summary_df, config):
    """Create detailed sheets for each group showing hostnames and VIPs"""
    print("\nStep 8: Creating group detail sheets...")
    
    group_details = {}
    cols = config['f5_columns']
    vip_col = cols['vip']
    
    # For each group, collect VIPs and their hostnames
    group_cols = ['No_CMDB', 'No_GlobalExit', 'All_DECOM', 'Tech_Servers', 
                  'Planned', 'No_DecomDate', 'Overdue']
    
    for group_col in group_cols:
        # Get VIPs in this group
        vips_in_group = summary_df[summary_df[group_col] == 'X']['VIP'].tolist()
        
        if vips_in_group:
            # Get all rows for these VIPs
            group_data = df[df[vip_col].isin(vips_in_group)].copy()
            
            # Apply group-specific filters to show only relevant hostnames
            if group_col == 'No_CMDB':
                # Show rows with no hostname (IP not found in CMDB)
                group_data = group_data[(group_data['hostname'].isna()) | (group_data['hostname'] == '')]
            
            elif group_col == 'No_GlobalExit':
                # Show rows with hostname but no entity/type (not found in Global Exit)
                group_data = group_data[
                    (group_data['hostname'].notna()) & 
                    (group_data['hostname'] != '') &
                    ((group_data['entity'].isna()) | (group_data['entity'] == '')) &
                    ((group_data['type'].isna()) | (group_data['type'] == ''))
                ]
            
            elif group_col == 'All_DECOM':
                # Show rows where host_status = DECOM
                group_data = group_data[group_data['host_status'].str.upper() == 'DECOM']
            
            elif group_col == 'Tech_Servers':
                # Show rows where host_status != DECOM and Type != APP
                group_data = group_data[
                    (group_data['host_status'].str.upper() != 'DECOM') &
                    (group_data['type'].str.upper() != 'APP')
                ]
            
            elif group_col == 'Planned':
                # Show rows where Type = APP, host_status != DECOM, and DecomDate in future
                group_data = group_data[
                    (group_data['type'].str.upper() == 'APP') &
                    (group_data['host_status'].str.upper() != 'DECOM') &
                    (group_data['decom_date'].notna()) &
                    (group_data['decom_date'] != '')
                ]
                # Filter by future dates
                future_rows = []
                for idx, row in group_data.iterrows():
                    decom_date = parse_date(row['decom_date'])
                    if decom_date and decom_date > config['today']:
                        future_rows.append(idx)
                group_data = group_data.loc[future_rows] if future_rows else pd.DataFrame(columns=group_data.columns)
            
            elif group_col == 'No_DecomDate':
                # Show rows where Type = APP, host_status != DECOM, and DecomDate is empty
                group_data = group_data[
                    (group_data['type'].str.upper() == 'APP') &
                    (group_data['host_status'].str.upper() != 'DECOM') &
                    ((group_data['decom_date'].isna()) | (group_data['decom_date'] == ''))
                ]
            
            elif group_col == 'Overdue':
                # Show rows where Type = APP, host_status != DECOM, and DecomDate in past
                group_data = group_data[
                    (group_data['type'].str.upper() == 'APP') &
                    (group_data['host_status'].str.upper() != 'DECOM') &
                    (group_data['decom_date'].notna()) &
                    (group_data['decom_date'] != '')
                ]
                # Filter by past dates
                past_rows = []
                for idx, row in group_data.iterrows():
                    decom_date = parse_date(row['decom_date'])
                    if decom_date and decom_date < config['today']:
                        past_rows.append(idx)
                group_data = group_data.loc[past_rows] if past_rows else pd.DataFrame(columns=group_data.columns)
            
            # Select relevant columns
            detail_cols = [vip_col, 'hostname', 'extracted_ip', 'host_status', 'type', 
                          'decom_date', 'entity', 'source', 'install_status']
            
            # Only include columns that exist
            available_cols = [col for col in detail_cols if col in group_data.columns]
            group_detail_df = group_data[available_cols].copy()
            
            # Remove duplicates and sort
            group_detail_df = group_detail_df.drop_duplicates()
            group_detail_df = group_detail_df.sort_values(by=[vip_col, 'hostname'])
            
            group_details[group_col] = group_detail_df
            print(f"  {group_col}: {len(vips_in_group)} VIPs, {len(group_detail_df)} rows")
        else:
            # Create empty dataframe with headers
            group_details[group_col] = pd.DataFrame(columns=[vip_col, 'hostname'])
            print(f"  {group_col}: 0 VIPs")
    
    return group_details


def create_visualizations(summary_df, config):
    """Create visualization charts for VIP group analysis"""
    if not VISUALIZATION_AVAILABLE:
        print("\nStep 7: Skipping visualizations (libraries not installed)")
        return
    
    print("\nStep 7: Creating visualizations...")
    
    # Prepare data - Excel column names and chart labels
    group_mapping = {
        'No_CMDB': 'IP not found in CMDB',
        'No_GlobalExit': 'Hostname not in Global Exit',
        'All_DECOM': 'All hostnames decommissioned',
        'Tech_Servers': 'Non-APP servers',
        'Planned': 'APP with future decom date',
        'No_DecomDate': 'APP without decom date',
        'Overdue': 'APP with past decom date'
    }
    
    group_cols = list(group_mapping.keys())
    
    # 1. Bar Chart - Count of VIPs per group
    plt.figure(figsize=(14, 7))
    group_counts = {}
    chart_labels = []
    for col in group_cols:
        count = (summary_df[col] == 'X').sum()
        group_counts[col] = count
        chart_labels.append(group_mapping[col])
    
    colors = ['#e74c3c', '#e67e22', '#2ecc71', '#3498db', '#9b59b6', '#f39c12', '#1abc9c']
    bars = plt.bar(range(len(chart_labels)), list(group_counts.values()), 
                   color=colors, edgecolor='black', linewidth=1.5)
    
    # Add value labels on bars
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(height)}',
                ha='center', va='bottom', fontsize=12, fontweight='bold')
    
    plt.xticks(range(len(chart_labels)), chart_labels, rotation=15, ha='right', fontsize=10)
    plt.ylabel('Number of VIPs', fontsize=12, fontweight='bold')
    plt.title('VIP Distribution Across Groups', fontsize=14, fontweight='bold', pad=20)
    plt.yticks(fontsize=11)
    plt.grid(axis='y', alpha=0.3, linestyle='--')
    plt.tight_layout()
    plt.savefig('vip_group_distribution.png', dpi=300, bbox_inches='tight')
    plt.close()
    print("  Created: vip_group_distribution.png")
    
    # 2. UpSet Plot - Group intersections
    try:
        # Prepare data for UpSet plot - use chart labels
        memberships = []
        for _, row in summary_df.iterrows():
            groups = [group_mapping[col] for col in group_cols if row[col] == 'X']
            if groups:
                memberships.append(groups)
        
        if memberships:
            upset_data = from_memberships(memberships)
            
            plt.figure(figsize=(16, 9))
            upset = UpSet(upset_data, 
                         subset_size='count',
                         show_counts=True,
                         element_size=40,
                         intersection_plot_elements=10)
            upset.plot()
            plt.suptitle('VIP Group Intersections', 
                        fontsize=14, fontweight='bold', y=0.98)
            plt.tight_layout()
            plt.savefig('vip_group_upset.png', dpi=300, bbox_inches='tight')
            plt.close()
            print("  Created: vip_group_upset.png")
    except Exception as e:
        print(f"  Warning: Could not create UpSet plot: {e}")
    
    print("\n  All visualizations saved successfully!")


def create_summary_sheet(df, config):
    """Create summary sheet with VIP groupings"""
    print("\nStep 6: Creating summary sheet...")
    
    cols = config['f5_columns']
    vip_col = cols['vip']
    
    # Group by VIP
    vip_groups = df.groupby(vip_col)
    
    summary_data = []
    
    for vip, group in vip_groups:
        # Get all rows for this VIP
        vip_data = group.copy()
        
        # Extract hostnames
        all_hostnames = vip_data['hostname'].unique()
        all_hostnames = [h for h in all_hostnames if h and h != '']
        
        # Initialize group flags
        group0 = False  # No hostnames found in CMDB
        group1 = False  # Hostnames found in CMDB but not in Global Exit
        group2 = False  # All hostnames DECOM
        group3 = False  # Running hostnames with Type != APP (Tech servers)
        group4 = False  # Type = APP, DecomDate in future
        group5 = False  # Type = APP, DecomDate empty
        group6 = False  # Type = APP, DecomDate in past
        
        if len(all_hostnames) == 0:
            # Group 0: No hostnames found in CMDB
            group0 = True
        else:
            # Check which hostnames were found in Global Exit (have entity/type data)
            hostnames_in_ge = []
            hostnames_not_in_ge = []
            
            for hostname in all_hostnames:
                hostname_rows = vip_data[vip_data['hostname'] == hostname]
                # Check if found in Global Exit (entity or type should be populated)
                if hostname_rows.iloc[0]['entity'] or hostname_rows.iloc[0]['type']:
                    hostnames_in_ge.append(hostname)
                else:
                    hostnames_not_in_ge.append(hostname)
            
            # Group 1: Hostnames found in CMDB but not in Global Exit
            if len(hostnames_not_in_ge) > 0:
                group1 = True
            
            # For hostnames found in Global Exit, check their status
            if len(hostnames_in_ge) > 0:
                ge_data = vip_data[vip_data['hostname'].isin(hostnames_in_ge)]
                
                # Group 2: ALL hostnames found in GE are DECOM (case-insensitive)
                all_decom = True
                for hostname in hostnames_in_ge:
                    hostname_row = ge_data[ge_data['hostname'] == hostname].iloc[0]
                    host_status = str(hostname_row['host_status']).strip().upper()
                    if host_status != 'DECOM':
                        all_decom = False
                        break
                
                if all_decom:
                    group2 = True
                
                # Groups 3-6: Check running hostnames (hostStatus != DECOM)
                # Process each unique hostname only once
                for hostname in hostnames_in_ge:
                    hostname_row = ge_data[ge_data['hostname'] == hostname].iloc[0]
                    host_status = str(hostname_row['host_status']).strip().upper()
                    
                    if host_status != 'DECOM':
                        hostname_type = str(hostname_row['type']).strip().upper()
                        
                        # Group 3: Running hostname with Type != APP (Tech servers)
                        if hostname_type != 'APP':
                            group3 = True
                        else:
                            # Groups 4-6: In scope (Type = APP)
                            decom_date_raw = hostname_row['decom_date']
                            
                            # Check if decom_date is empty/null
                            if pd.isna(decom_date_raw) or decom_date_raw == '' or decom_date_raw is None:
                                # Group 5: Type = APP, DecomDate empty
                                group5 = True
                            else:
                                # Parse the date
                                decom_date = parse_date(decom_date_raw)
                                
                                if decom_date:
                                    # Group 4: DecomDate in future
                                    if decom_date > config['today']:
                                        group4 = True
                                    # Group 6: DecomDate in past
                                    elif decom_date < config['today']:
                                        group6 = True
                                else:
                                    # Could not parse date, treat as empty
                                    group5 = True
        
        summary_data.append({
            'VIP': vip,
            'Hostname_Count': len(all_hostnames),
            'Hostnames': ', '.join(all_hostnames),
            'No_CMDB': 'X' if group0 else '',
            'No_GlobalExit': 'X' if group1 else '',
            'All_DECOM': 'X' if group2 else '',
            'Tech_Servers': 'X' if group3 else '',
            'Planned': 'X' if group4 else '',
            'No_DecomDate': 'X' if group5 else '',
            'Overdue': 'X' if group6 else ''
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
    df_step4, ips_not_found = enrich_with_cmdb(df_step3.copy(), config)
    
    # Step 5: Enrich with Global Exit
    df_step5, hostnames_not_found = enrich_with_global_exit(df_step4.copy(), config)
    
    # Step 6: Create summary
    summary_df = create_summary_sheet(df_step5, config)
    
    # Step 7: Create visualizations
    create_visualizations(summary_df, config)
    
    # Create IPs not found sheet
    ips_not_found_df = pd.DataFrame({
        'IP_Address': sorted(list(ips_not_found))
    })
    
    # Create hostnames not found sheet
    hostnames_not_found_df = pd.DataFrame({
        'Hostname': sorted(list(hostnames_not_found))
    })
    
    # Create detailed group breakdown sheets
    group_details = create_group_detail_sheets(df_step5, summary_df, config)
    
    # Write to Excel with multiple sheets
    print("\nStep 9: Writing output to Excel...")
    with pd.ExcelWriter(config['output_file'], engine='openpyxl') as writer:
        df_step1.to_excel(writer, sheet_name='1_After_Append', index=False)
        df_step2.to_excel(writer, sheet_name='2_After_Filters', index=False)
        df_step3.to_excel(writer, sheet_name='3_After_Expansion', index=False)
        df_step5.to_excel(writer, sheet_name='4_Final_Enriched', index=False)
        ips_not_found_df.to_excel(writer, sheet_name='5_IPs_Not_Found_CMDB', index=False)
        hostnames_not_found_df.to_excel(writer, sheet_name='6_Hostnames_Not_Found_GE', index=False)
        summary_df.to_excel(writer, sheet_name='7_Summary', index=False)
        
        # Write group detail sheets
        group_details['No_CMDB'].to_excel(writer, sheet_name='8_Detail_No_CMDB', index=False)
        group_details['No_GlobalExit'].to_excel(writer, sheet_name='9_Detail_No_GlobalExit', index=False)
        group_details['All_DECOM'].to_excel(writer, sheet_name='10_Detail_All_DECOM', index=False)
        group_details['Tech_Servers'].to_excel(writer, sheet_name='11_Detail_Tech_Servers', index=False)
        group_details['Planned'].to_excel(writer, sheet_name='12_Detail_Planned', index=False)
        group_details['No_DecomDate'].to_excel(writer, sheet_name='13_Detail_No_DecomDate', index=False)
        group_details['Overdue'].to_excel(writer, sheet_name='14_Detail_Overdue', index=False)
    
    print(f"\n✓ Output written to: {config['output_file']}")
    print("\nSheets created:")
    print("  1. 1_After_Append - Data after appending F5 files")
    print("  2. 2_After_Filters - Data after applying filters")
    print("  3. 3_After_Expansion - Data after expanding member_addrs")
    print("  4. 4_Final_Enriched - Final enriched data")
    print("  5. 5_IPs_Not_Found_CMDB - IPs not found in CMDB")
    print("  6. 6_Hostnames_Not_Found_GE - Hostnames not found in Global Exit")
    print("  7. 7_Summary - VIP summary by groups")
    print("  8-14. Detail sheets for each group (VIPs and hostnames)")
    print("\n" + "=" * 80)
    print("Processing complete!")
    print("=" * 80)


if __name__ == "__main__":
    main()
