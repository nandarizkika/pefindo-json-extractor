"""
Pefindo JSON Data Extraction Script
Extracts nested Pefindo JSON data into 9 structured DataFrames
"""

import pandas as pd
import json


def load_json_data(json_path):
    """Load JSON file and normalize to DataFrame"""
    with open(json_path, 'r') as file:
        data = json.load(file)
    return pd.json_normalize(data)


def clean_column_names(df):
    """Remove 'report.debitur.' and 'report.header.' prefixes from column names"""
    df.columns = df.columns.str.replace('report.debitur.', '', regex=False)
    df.columns = df.columns.str.replace('report.header.', '', regex=False)
    return df


def extract_list_to_dataframe(json_df, column_name, cols_to_add):
    """
    Extract list column to separate DataFrame with additional columns
    
    Args:
        json_df: Source DataFrame
        column_name: Column containing list of dictionaries
        cols_to_add: List of columns to add from source DataFrame
    
    Returns:
        DataFrame with expanded list data
    """
    all_rows = []
    
    for i in range(len(json_df)):
        item_list = json_df[column_name][i]
        
        if item_list:
            temp_df = pd.DataFrame(item_list)
            
            for col in cols_to_add:
                temp_df[col] = json_df[col][i]
            
            all_rows.append(temp_df)
    
    if all_rows:
        result_df = pd.concat(all_rows, ignore_index=True)
        return clean_column_names(result_df)
    else:
        return pd.DataFrame()


def extract_facilities_history(df_facilities):
    """
    Extract facilities history with special handling for empty lists
    
    Args:
        df_facilities: Facilities DataFrame
    
    Returns:
        DataFrame with facilities history
    """
    all_rows = []
    
    cols_to_add = [ 'username', 'nomor_identitas',
                   'id_report', 'tgl_permintaan', 'npwp', 'email', 'telepon',
                   'nomor_rekening_fasilitas', 'id_jenis_fasilitas',
                   'id_pelapor', 'id_jenis_pelapor', 'id_jenis_kredit', 'id_sifat_kredit']
    
    
    sample_keys = None
    for riwayat in df_facilities['riwayat_fasilitas']:
        if riwayat:
            sample_keys = list(riwayat[0].keys())
            break
    
    matching_cols = [col for col in df_facilities.columns if col in sample_keys]
    
    for i in range(len(df_facilities)):
        riwayat_list = df_facilities['riwayat_fasilitas'][i]
        
        if riwayat_list:
            
            temp_df = pd.DataFrame(riwayat_list)
            
            for col in cols_to_add:
                temp_df[col] = df_facilities[col][i]
            
            all_rows.append(temp_df)
        else:
            
            temp_df = df_facilities.iloc[[i]][matching_cols].copy()
            
            
            temp_df['snapshot_order'] = 1
            temp_df['status_tunggakan'] = 1 if df_facilities['tunggakan_pokok'][i] > 0 else 0
            
            for col in cols_to_add:
                temp_df[col] = df_facilities[col][i]
            
            all_rows.append(temp_df)
    
    result_df = pd.concat(all_rows, ignore_index=True)
    return clean_column_names(result_df)


def extract_pefindo_data(json_path):
    """
    Main function to extract all Pefindo data tables
    
    Args:
        json_path: Path to JSON file
    
    Returns:
        Dictionary containing all DataFrames
    """
    
    print("Loading JSON data...")
    json_df = load_json_data(json_path)
    print(f"Loaded {len(json_df)} records")
    
    
    cols_basic = [ 'report.header.username', 'report.debitur.nomor_identitas',
                  'report.header.id_report', 'report.header.tgl_permintaan', 
                  'report.debitur.npwp', 'report.debitur.email', 'report.debitur.telepon']
    
    cols_facilities = [ 'username', 'nomor_identitas',
                       'id_report', 'tgl_permintaan', 'npwp', 'email', 'telepon',
                       'nomor_rekening_fasilitas', 'id_jenis_fasilitas',
                       'id_pelapor', 'id_jenis_pelapor', 'id_jenis_kredit', 'id_sifat_kredit']
    
    
    dfs = {}
    
    
    print("Extracting pefindo_scoring...")
    dfs['pefindo_scoring'] = extract_list_to_dataframe(json_df, 'scoring', cols_basic)
    print(f"  Shape: {dfs['pefindo_scoring'].shape}")
    
    
    print("Extracting pefindo_information_summary...")
    debitur_cols = [col for col in json_df.columns if 'report.debitur' in col]
    debitur_cols.extend([
                        'report.header.username', 'report.header.id_report', 
                        'report.header.tgl_permintaan'])
    dfs['pefindo_information_summary'] = clean_column_names(json_df[debitur_cols])
    print(f"  Shape: {dfs['pefindo_information_summary'].shape}")
    
    
    print("Extracting pefindo_facilities...")
    dfs['pefindo_facilities'] = extract_list_to_dataframe(json_df, 'report.fasilitas', cols_basic)
    print(f"  Shape: {dfs['pefindo_facilities'].shape}")
    
    
    print("Extracting pefindo_facilities_history...")
    dfs['pefindo_facilities_history'] = extract_facilities_history(dfs['pefindo_facilities'])
    print(f"  Shape: {dfs['pefindo_facilities_history'].shape}")
    
    
    print("Extracting pefindo_facilities_collateral...")
    dfs['pefindo_facilities_collateral'] = extract_list_to_dataframe(
        dfs['pefindo_facilities'], 'agunan', cols_facilities
    )
    print(f"  Shape: {dfs['pefindo_facilities_collateral'].shape}")
    
    
    print("Extracting pefindo_facilities_guarantor...")
    dfs['pefindo_facilities_guarantor'] = extract_list_to_dataframe(
        dfs['pefindo_facilities'], 'penjamin', cols_facilities
    )
    print(f"  Shape: {dfs['pefindo_facilities_guarantor'].shape}")
    
    
    print("Extracting pefindo_inquiry...")
    dfs['pefindo_inquiry'] = extract_list_to_dataframe(
        json_df, 'report.permintaan_data', cols_basic
    )
    print(f"  Shape: {dfs['pefindo_inquiry'].shape}")
    
    
    print("Extracting pefindo_inquiry_summary...")
    dfs['pefindo_inquiry_summary'] = extract_list_to_dataframe(
        json_df, 'report.summary_permintaan_data', cols_basic
    )
    print(f"  Shape: {dfs['pefindo_inquiry_summary'].shape}")
    
    
    print("Extracting pefindo_information_history...")
    dfs['pefindo_information_history'] = extract_list_to_dataframe(
        json_df, 'report.riwayat_identitas_debitur', cols_basic
    )
    print(f"  Shape: {dfs['pefindo_information_history'].shape}")
    
    
    print("Extracting pefindo_collectibility_history...")
    dfs['pefindo_collectibility_history'] = extract_list_to_dataframe(
        json_df, 'report.summary_riwayat_debitur', cols_basic
    )
    print(f"  Shape: {dfs['pefindo_information_history'].shape}")
    
    print("\n✅ Extraction complete!")
    return dfs


def main():
    """Main execution function"""
    
    json_path = '../json_file/pefindo-staging-data.json'
    
    
    dataframes = extract_pefindo_data(json_path)
    
    
    print("\n" + "="*60)
    print("EXTRACTION SUMMARY")
    print("="*60)
    total_columns = 0
    for name, df in dataframes.items():
        print(f"{name}: {df.shape[0]} rows × {df.shape[1]} columns")
        total_columns += df.shape[1]
    print(f"\nTotal columns across all tables: {total_columns}")
    print("="*60)
    
    return dataframes


if __name__ == "__main__":
    
    dfs = main()
    
    
    
    
    