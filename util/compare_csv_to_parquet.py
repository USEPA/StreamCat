import os
import zipfile
import pandas as pd

def match_file_pairs(csv_file_dir, parquet_files):
    # List all files in the directory
    all_files = os.listdir(csv_file_dir)
    
    # Filter out the ZIP files containing CSV files
    #csv_zip_files = [os.path.join(csv_file_dir, f) for f in all_files if f.endswith('.zip')]
    csv_zip_files = [f"{csv_file_dir}/{f}" for f in all_files if f.endswith('.zip')]
    
    # Create dictionaries to map base filenames to their full paths
    csv_dict = {}
    for zip_file in csv_zip_files:
        with zipfile.ZipFile(zip_file, 'r') as z:
            for name in z.namelist():
                if name.endswith('.csv'):
                    base_name = name.rsplit('.', 1)[0]
                    csv_dict[base_name] = (zip_file, name)
    
    parquet_dict = {filename.rsplit('.', 1)[0]: filename for filename in parquet_files}
    
    # Find the common base filenames
    common_bases = set(csv_dict.keys()) & set(parquet_dict.keys())
    
    # Create the list of tuple pairs
    matched_pairs = [(csv_dict[base], parquet_dict[base]) for base in common_bases]
    
    return matched_pairs

def compare_file_pairs(csv_file_dir, parquet_files, parq_file_dir):
    # Function to read and compare a single pair of files
    def compare_pair(csv_info, parquet_file):
        zip_file, csv_path = csv_info
        with zipfile.ZipFile(zip_file, 'r') as z:
            with z.open(csv_path) as f:
                df_csv = pd.read_csv(f)
        
        df_parquet = pd.read_parquet(f"{parq_file_dir}/{parquet_file}")
        
        # Ensure the DataFrames have the same columns and data
        if df_csv.equals(df_parquet):
            return True
        else:
            return False
    
    # Get the matched pairs
    matched_pairs = match_file_pairs(csv_file_dir, parquet_files)
    
    # Compare each pair
    results = []
    for (zip_file, csv_path), parquet_file in matched_pairs:
        result = compare_pair((zip_file, csv_path), parquet_file)
        results.append(((zip_file, csv_path), parquet_file, result))
    
    return results

if __name__ == '__main__':

    csv_file_dir = "O:/PRIV/CPHEA/PESD/COR/CORFILES/Geospatial_Library_Projects/StreamCat/FTP_Staging/HydroRegions/zips"

    parq_file_dir = "O:/PRIV/CPHEA/PESD/COR/CORFILES/Geospatial_Library_Projects/StreamCat/FTP_Staging/HydroRegions"


    parquet_files = [f for f in os.listdir(parq_file_dir) if f.count('.parquet') and not f.count('Archive') and not f.count('zips')]

    comparison_results = compare_file_pairs(csv_file_dir, parquet_files, parq_file_dir)
    for (zip_file, csv_path), parquet_file, result in comparison_results:
        # print(f"Comparing {zip_file}/{csv_path} and {parquet_file}: {'Equivalent' if result else 'Not Equivalent'}")
        if not result:
            print(f"{csv_path} and {parquet_file} are not equivalent: Check these results")