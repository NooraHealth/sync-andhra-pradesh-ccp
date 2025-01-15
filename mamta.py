import io
import json
import os
import oyaml as yaml
import polars as pl
import uuid
import warnings
from google.cloud import bigquery
from google.oauth2.service_account import Credentials
from pathlib import Path
import ftplib
import pandas as pd  # Ensure pandas is installed for Excel file handling
import re
from google.api_core.exceptions import GoogleAPIError
import xlsxwriter

def get_params(params_path='params.yaml'):
    """
    Load configuration parameters from a YAML file and environment variables.
    """
    github_ref_name = os.getenv('GITHUB_REF_NAME')

    with open(params_path, 'r') as file:
        params = yaml.safe_load(file)

    key = 'service_account_key'
    if github_ref_name is None:
        with open(Path('secrets', params[key])) as f:
            params[key] = json.load(f)
        with open(Path('secrets', 'ftp_credentials.json')) as f:
            params['ftp_credentials'] = json.load(f)
    else:
        params[key] = json.loads(os.getenv('SERVICE_ACCOUNT_KEY'))
        params['ftp_credentials'] = json.loads(os.getenv('FTP_CREDENTIALS'))

    params['credentials'] = Credentials.from_service_account_info(params[key])
    del params[key]
    return params

def connect_ftp(ftp_params):
    """
    Establish a secure connection to the FTP server.
    """
    try:
        ftp = ftplib.FTP_TLS()
        ftp.connect(ftp_params['host'], ftp_params.get('port', 21))
        ftp.login(ftp_params['user'], ftp_params['password'])
        ftp.prot_p()  # Secure the data connection
        print(f"Connected to FTP server: {ftp_params['host']}")
        return ftp
    except ftplib.all_errors as e:
        print(f"Failed to connect to FTP server: {e}")
        return None

def test_bigquery_connection(credentials, project_id):
    try:
        client = bigquery.Client(credentials=credentials, project=project_id)
        datasets = list(client.list_datasets())
        if datasets:
            print(f"Connected successfully. Datasets in project '{project_id}':")
            for dataset in datasets:
                print(f" - {dataset.dataset_id}")
        else:
            print(f"Connected successfully but no datasets found in project '{project_id}'.")
        return True
    except GoogleAPIError as e:
        print(f"Failed to connect to BigQuery: {e}")
        return False
    
def list_files(ftp, folder):
    """
    List all files in a given directory on the FTP server.
    """
    try:
        ftp.cwd(folder)
        files = ftp.nlst()
        return files
    except ftplib.error_perm as e:
        print(f"Failed to list files in directory '{folder}': {e}")
        return []

def download_file(ftp, file_name, local_folder):
    local_path = os.path.join(local_folder, file_name)
    try:
        ftp.cwd(os.path.dirname(file_name))  # Change to the file's directory
        with open(local_path, 'wb') as f:
            ftp.retrbinary(f"RETR {os.path.basename(file_name)}", f.write)
        print(f"Downloaded: {file_name}")
    except ftplib.error_perm as e:
        print(f"Failed to download file '{file_name}': {e}")


def download_excel_files(ftp, remote_folder, local_folder):
    """
    Download all Excel files and csv files from the FTP server's remote folder to the local folder.
    """
    if not os.path.exists(local_folder):
        os.makedirs(local_folder)
    files = list_files(ftp, remote_folder)
    excel_files = [file for file in files if file.lower().endswith(('.xls', '.xlsx','.csv'))]
    for file_name in excel_files:
        download_file(ftp, file_name, local_folder)

def combine_sheets(local_folder, patterns,expected_columns):
    combined_df = pd.DataFrame()
    pattern_regex = re.compile('|'.join(patterns), re.IGNORECASE)

    # Define the complete set of expected columns for CCP Session
    

    # Column mapping for standardization
    column_mapping = {
        'Mothers Trained/Pregnant Women/Lactating Mothers': 'Mothers Trained',
        'Mothers Trained': 'Mothers Trained',
        'Sr. No.': 'Serial No',
        'Sr. No': 'Serial No',
        'S_No.': 'Serial No',
        'Sr No': 'Serial No',
        'Sno': 'Serial No',
        'Name of the Trainer':'Name of the Trainer1',
        'State':'State',
        'Partner':'Partner'
    }

    for file_name in os.listdir(local_folder):
        if file_name.lower().endswith(('.xls', '.xlsx')):
            file_path = os.path.join(local_folder, file_name)
            try:
                excel_file = pd.read_excel(file_path, sheet_name=None, header=None)
                for sheet_name, df in excel_file.items():
                    if pattern_regex.search(sheet_name):
                        # Identify file type based on file name or sheet content
                        if 'HP CCP Session & TOT data till' in file_name or 'aug' in file_name and 'TOT' not in sheet_name:
                            # Use 2nd row as header
                            df = df.copy()
                            df.columns = df.iloc[1]
                            df = df.iloc[2:].reset_index(drop=True)
                        elif 'HP CCP Session data till' in file_name and 'TOT' not in sheet_name:
                            # Use 1st row as header
                            df = df.copy()
                            df.columns = df.iloc[0]
                            df = df.iloc[1:].reset_index(drop=True)
                        
                        elif 'TOT' in sheet_name:  # Specifically for TOT sheets within the files
                          # For TOT sessions, always use second row as header
                          df = df.copy()
                          df.columns = df.iloc[1]  # Set second row as column names
                          df = df.iloc[2:].reset_index(drop=True)  # Skip first two rows

                        # Standardize column names
                        df.rename(columns=lambda x: column_mapping.get(x, x), inplace=True)

                        # Drop 'Serial No' column if it exists (formerly 'Sno')
                        if 'Serial No' in df.columns:
                            df.drop(columns=['Serial No'], inplace=True)
                        
                        # Remove duplicate columns (if any) and reset index
                        if df.columns.duplicated().any():
                            df = df.loc[:, ~df.columns.duplicated()]  # Drop duplicate columns

                        # Ensure unique index
                        if df.index.duplicated().any():
                            df = df[~df.index.duplicated()]  # Drop duplicate index rows

                        # Ensure all expected columns are present
                        for col in expected_columns:
                            if col not in df.columns:
                                df[col] = pd.NA  # Add missing columns as NaN

                        # Add source tracking columns
                        df['source_file'] = file_name
                        df['sheet_name'] = sheet_name

                        # Append to the combined DataFrame
                        combined_df = pd.concat([combined_df, df], ignore_index=True)
            except Exception as e:
                print(f"Failed to read from '{file_name}': {e}")

    return combined_df



def save_dataframe_to_excel(df, file_path, sheet_name='Sheet1'):
    """
    Save a DataFrame to an Excel file.

    Args:
        df (pd.DataFrame): DataFrame to save.
        file_path (str): File path to save the Excel file.
        sheet_name (str): Sheet name in the Excel file.
    """
    try:
        df.to_excel(file_path, sheet_name=sheet_name, index=False)
        print(f"DataFrame saved to {file_path}")
    except Exception as e:
        print(f"Failed to save DataFrame: {e}")


def main():
    params = get_params()
    ftp_params = params.get('ftp_credentials')
    if not ftp_params:
        print("FTP credentials not found.")
        return
    
    #check connection with ftp server
    ftp = connect_ftp(ftp_params)
    if not ftp:
        print("Exiting due to failed ftp server connection")
        return
    
    #Check connection with Bigquery
    credentials = params['credentials']
    project_id = params['project']

    if not test_bigquery_connection(credentials, project_id):
        print("Exiting due to failed BigQuery connection.")
        return

    target_directory = params.get('ftp_directory', '/files')
    all_files = list_files(ftp, target_directory)
    print(f"All files in '{target_directory}': {all_files}")

    desired_extensions = ('.xlsx','.xls','.csv')
    filtered_files = [file for file in all_files if file.lower().endswith(desired_extensions)]
    
    print("filtered files are : ",filtered_files)
    
    excel_files_folder = params.get('excel_files_directory', 'excel_files')


    # Download Excel files from FTP server
    download_excel_files(ftp, target_directory, excel_files_folder)
    print("Excel files downloaded successfully.")
    
    # Define patterns for sheet names
    ccp_patterns = [r'ccp', r'CCP', r'CCP Session']
    ccp_columns = [
        'Serial No',
        'Name of Facility',
        'Username',
        'Date of the Session',
        'Time of the Session',
        'People Trained',
        'Mothers Trained',
        'Caregivers Trained',
        'Male Participants',
        'Female Participants',
        'Type of Session',
        'Number of Trainers',
        'Name of the Trainer1',
        'Name of the Trainer2',
        'Name of the Trainer3',
        'Name of the Trainer4',
        'Data Appended Date',
        'Condition',
        'Partner',
        'State',
        'District',
        'MCCS',
        'Class ID'
    ]
    # Combine data from 'CCP' sheets
    ccp_df = combine_sheets(excel_files_folder, ccp_patterns,ccp_columns)
    print("Combined 'CCP' DataFrame:")
    print(ccp_df)
    
    # combined data from TOT sheets
    tot_patterns = [r'tot', r'TOT']
    tot_columns = [
        'Serial No',
        'District',
        'Facility Name',
        'Username',
        'MIS ID',
        'Name of the Trainer',
        'Type of Session',
        'Date of the Session',
        'State',
        'Partner'
    ]
    #Combine data from 'TOT' sheets
    tot_df = combine_sheets(excel_files_folder, tot_patterns,tot_columns)
    print("Combined 'TOT' DataFrame:")
    print(tot_df)
    
    # Save the combined CCP DataFrame
    save_dataframe_to_excel(ccp_df, 'combined_ccp_data.xlsx', sheet_name='CCP_Session')

    #Save the combined TOT DataFrame
    save_dataframe_to_excel(tot_df, 'combined_tot_data.xlsx', sheet_name='TOT')

    # Write combined DataFrames to a single Excel file with two sheets
    # output_excel_path = params.get('output_excel_path', 'combined_data.xlsx')
    # write_dataframes_to_excel([ccp_df, tot_df], ['CCP_Session', 'TOT'], output_excel_path)
    # print(f"Combined data written to '{output_excel_path}'.")

    ftp.quit()
    
if __name__ == "__main__":
    main()
