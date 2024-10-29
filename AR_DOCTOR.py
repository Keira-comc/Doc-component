import os
import openpyxl
import csv
from pandas import DataFrame
import concurrent.futures
import pyodbc
import modin.config as cfg
cfg.Engine.put('ray')
import pandas as pd
import numpy as np
from tqdm import tqdm
from fpdf import FPDF
from PyPDF2 import PdfMerger
from reportlab.lib.pagesizes import A4, landscape
from reportlab.pdfgen import canvas
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.platypus import Table, TableStyle
from sqlalchemy import create_engine, text
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from math import ceil

print(pyodbc.drivers())
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.max_rows', None)  # Show all columns

# Connection parameters for SQL Server
server = r'192.168.0.213\SQL'  # Use raw string or double backslash
database = 'HIS'
username = 'ACTG'
password = 'HIS1a2b3c4d'

# Input Data
# User-input date   
startDate_input = input("Start Date (YYYY-MM-DD): ")
endDate_input = input("End Date:(YYYY-MM-DD): ")

# Define the start and end dates for filtering 
start_date_series = pd.Series([pd.Timestamp(startDate_input)])
end_date_series = pd.Series([pd.Timestamp(endDate_input)]) + pd.Timedelta(days=1)

start_date = start_date_series.dt.strftime('%Y-%m-%d')[0]
end_date = end_date_series.dt.strftime('%Y-%m-%d')[0]

start_date_use = start_date_series.dt.strftime('%Y-%m-%d %H:%M:%S')[0]
end_date_use = end_date_series.dt.strftime('%Y-%m-%d %H:%M:%S')[0]

# Define the current datetime as of the use of the program
current_datetime = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')

def create_engine_instance():
    """Create a SQLAlchemy engine with connection pooling."""
    connection_string = (
        f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC%20Driver%2018%20for%20SQL%20Server&TrustServerCertificate=yes"
    )
    engine = create_engine(connection_string, pool_size=10, max_overflow=10)  # Adjust the pool size for concurrency
    return engine


def parallel_query_execution(engine, base_query, chunk_ranges):
    """Execute SQL queries in parallel based on the chunk ranges provided."""
    results = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [
            executor.submit(execute_query, engine, base_query, {'start_id': start, 'end_id': end, 'start_date_use': start_date, 'end_date_use':end_date})
            for start, end in chunk_ranges
        ]
            
        # Use tqdm to track the progress of the queries
        with tqdm(total=len(futures)) as pbar:
            for future in as_completed(futures):
                results.append(future.result())
                pbar.update(1)
        
    # Combine results from all the parallel queries
    return pd.concat(results)


# Function to execute a SQL query
def execute_query(engine, query, params=None):
    with engine.connect() as connection:
        result = connection.execute(text(query), params)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        return df

# Function to divide query execution into chunks (parallelized)
def parallel_query_execution(engine, base_query, chunk_ranges, start_date, end_date):
    """Execute SQL queries in parallel based on the chunk ranges provided."""
    results = []
    with ThreadPoolExecutor(max_workers=10) as executor:  # Adjust the number of workers for parallelism
        futures = [
            executor.submit(execute_query, engine, base_query, {'start_id': start, 'end_id': end, 'start_date_use': start_date, 'end_date_use': end_date })
            for start, end in chunk_ranges
        ]
        
        # Use tqdm to track the progress of the queries
        with tqdm(total=len(futures), desc="Executing SQL queries", unit="query") as pbar:
            for future in as_completed(futures):
                results.append(future.result())
                pbar.update(1)  # Update the progress bar for each completed query
    
    # Combine results from all the parallel queries
    print(f"This is start id: {start_id}")
    print(f"This is start date: {end_id}")
    return pd.concat(results)

def generate_chunk_ranges(engine, table_name, id_column, num_chunks):
    """Generate chunk ranges based on the total number of records and number of chunks."""
    # Query to get the total number of records
    query = f"SELECT COUNT({id_column}) AS total FROM {table_name}"
    with engine.connect() as connection:
        result = connection.execute(text(query))
        total_records = result.fetchone()[0]
    
    # Calculate the size of each chunk
    chunk_size = ceil(total_records / num_chunks)
    
    # Generate chunk ranges
    chunk_ranges = [(start, min(start + chunk_size - 1, total_records)) for start in range(1, total_records + 1, chunk_size)]
    return chunk_ranges

# Main execution
if __name__ == "__main__":
    engine = create_engine_instance()

    # Define the number of chunks you want to divide the data into
    num_chunks = 10  # Adjust this number as needed

    # Automatically generate chunk ranges based on the total records
    chunk_ranges = generate_chunk_ranges(engine, '[HIS].[dbo].[HIS_PFRECORD]', 'HOSPITALRECORDID', num_chunks)

    # Base query with parameters to handle chunks
    base_query = """
    SELECT
        [HIS].[dbo].[HIS_PFRECORD].[ID] AS ID,
        [HIS].[dbo].[HIS_PFRECORD].[HOSPITALRECORDID] AS HOSPITALRECORDID,
        [HIS].[dbo].[HIS_PFRECORD].[DRNAME] AS DRNAME,
        DATEADD(SECOND, 1, [HIS].[dbo].[HIS_PFRECORD].[SERVICESTART]) AS SERVICESTART,
        DATEADD(SECOND, 1, [HIS].[dbo].[HIS_PFRECORD].[SERVICEEND]) AS SERVICEEND,
        DATEADD(SECOND, 1, [HIS].[dbo].[CHRNG_SALESHEADER].[TRANSDATETIME]) AS PAYTIME_PERSONAL,
        [HIS].[dbo].[HIS_PFJOBTITLE].[NAME] AS JOBTITLENAME,
        [HIS].[dbo].[HIS_PHYSICIANSLIST].[EWT]*0.01 AS EWT,
        ([HIS].[dbo].[HIS_PFRECORD].[PFAMOUNT] - ISNULL([HIS].[dbo].[HIS_PFRECORD].[PFDISCOUNT],0)) AS PFNETAMOUNT,
        ([HIS].[dbo].[HIS_PFRECORD].[PFAMOUNT] - ISNULL([HIS].[dbo].[HIS_PFRECORD].[PFDISCOUNT],0)) * [HIS].[dbo].[HIS_PHYSICIANSLIST].[EWT]*0.01 AS EWT_PAYABLE,
        [HIS].[dbo].[HIS_PFRECORD].[CANCELLED] AS CANCELLED,
        [HIS].[dbo].[HIS_PFRECORD].[SALESHEADERID] AS SALESHEADERID,
        [HIS].[dbo].[HIS_PFRECORD].[SALESDETAILID] AS SALESDETAILID,
        [HIS].[dbo].[CHRNG_PFDETAIL].[PFAMOUNT] AS PFPAID_PERSONAL,
        [HIS].[dbo].[AR_IPDOPDPYMNTDETAIL].[BPFAMOUNT] AS PFPAID_OTHERS,
        (([HIS].[dbo].[CHRNG_PFDETAIL].[PFAMOUNT] +  ISNULL([HIS].[dbo].[AR_IPDOPDPYMNTDETAIL].[BPFAMOUNT],0)) * [HIS].[dbo].[HIS_PHYSICIANSLIST].[EWT]*0.01 ) AS EWT_PAYMENT
       

    FROM [HIS].[dbo].[HIS_PFRECORD]
    JOIN [HIS].[dbo].[HIS_PFJOBTITLE]
        ON [HIS].[dbo].[HIS_PFRECORD].[JOBTITLEID] = [HIS].[dbo].[HIS_PFJOBTITLE].[ID]
    JOIN [HIS].[dbo].[HIS_PHYSICIANSLIST]
        ON [HIS].[dbo].[HIS_PFRECORD].[PHYSICIANSLISTID] = [HIS].[dbo].[HIS_PHYSICIANSLIST].[ID]
    JOIN [HIS].[dbo].[CHRNG_PFDETAIL]
        ON [HIS].[dbo].[HIS_PFRECORD].[ID] = [HIS].[dbo].[CHRNG_PFDETAIL].[PFRECORDID]
    JOIN [HIS].[dbo].[CHRNG_SALESHEADER]
        ON [HIS].[dbo].[CHRNG_PFDETAIL].[SALESHEADERID] = [HIS].[dbo].[CHRNG_SALESHEADER].[ID]
    LEFT JOIN [HIS].[dbo].[AR_IPDOPDPYMNTDETAIL]
        ON [HIS].[dbo].[HIS_PFRECORD].[ID] = [HIS].[dbo].[AR_IPDOPDPYMNTDETAIL].[PFRECID]


    WHERE
        [HIS].[dbo].[HIS_PFRECORD].[ID] BETWEEN :start_id AND :end_id
        AND [HIS].[dbo].[HIS_PFRECORD].[SERVICEEND] BETWEEN :start_date_use AND :end_date_use
        AND [HIS].[dbo].[CHRNG_SALESHEADER].[TRANSDATETIME] <= :end_date_use
        AND [HIS].[dbo].[HIS_PFRECORD].[CANCELLED] = 'N'
    """
    ## 1st query is about the payment to and service rendered by the docotor within the set range of provided

    base_query2 = """
    SELECT
        [HIS].[dbo].[HIS_PFRECORD].[ID] AS ID,
        [HIS].[dbo].[HIS_PFRECORD].[HOSPITALRECORDID] AS HOSPITALRECORDID,
        [HIS].[dbo].[HIS_PFRECORD].[DRNAME] AS DRNAME,
        DATEADD(SECOND, 1, [HIS].[dbo].[HIS_PFRECORD].[SERVICESTART]) AS SERVICESTART,
        DATEADD(SECOND, 1, [HIS].[dbo].[HIS_PFRECORD].[SERVICEEND]) AS SERVICEEND,
        DATEADD(SECOND, 1, [HIS].[dbo].[CHRNG_SALESHEADER].[TRANSDATETIME]) AS PAYTIME_PERSONAL,
        [HIS].[dbo].[HIS_PFJOBTITLE].[NAME] AS JOBTITLENAME,
        [HIS].[dbo].[HIS_PHYSICIANSLIST].[EWT]*0.01 AS EWT,
        ([HIS].[dbo].[HIS_PFRECORD].[PFAMOUNT] - ISNULL([HIS].[dbo].[HIS_PFRECORD].[PFDISCOUNT],0)) AS PFNETAMOUNT,
        ([HIS].[dbo].[HIS_PFRECORD].[PFAMOUNT] - ISNULL([HIS].[dbo].[HIS_PFRECORD].[PFDISCOUNT],0)) * [HIS].[dbo].[HIS_PHYSICIANSLIST].[EWT]*0.01 AS EWT_PAYABLE,
        [HIS].[dbo].[HIS_PFRECORD].[CANCELLED] AS CANCELLED,
        [HIS].[dbo].[HIS_PFRECORD].[SALESHEADERID] AS SALESHEADERID,
        [HIS].[dbo].[HIS_PFRECORD].[SALESDETAILID] AS SALESDETAILID,
        [HIS].[dbo].[CHRNG_PFDETAIL].[PFAMOUNT] AS PFPAID_PERSONAL,
        [HIS].[dbo].[AR_IPDOPDPYMNTDETAIL].[BPFAMOUNT] AS PFPAID_OTHERS,
        (([HIS].[dbo].[CHRNG_PFDETAIL].[PFAMOUNT] +  ISNULL([HIS].[dbo].[AR_IPDOPDPYMNTDETAIL].[BPFAMOUNT],0)) * [HIS].[dbo].[HIS_PHYSICIANSLIST].[EWT]*0.01 ) AS EWT_PAYMENT
       

    FROM [HIS].[dbo].[HIS_PFRECORD]
    JOIN [HIS].[dbo].[HIS_PFJOBTITLE]
        ON [HIS].[dbo].[HIS_PFRECORD].[JOBTITLEID] = [HIS].[dbo].[HIS_PFJOBTITLE].[ID]
    JOIN [HIS].[dbo].[HIS_PHYSICIANSLIST]
        ON [HIS].[dbo].[HIS_PFRECORD].[PHYSICIANSLISTID] = [HIS].[dbo].[HIS_PHYSICIANSLIST].[ID]
    JOIN [HIS].[dbo].[CHRNG_PFDETAIL]
        ON [HIS].[dbo].[HIS_PFRECORD].[ID] = [HIS].[dbo].[CHRNG_PFDETAIL].[PFRECORDID]
    JOIN [HIS].[dbo].[CHRNG_SALESHEADER]
        ON [HIS].[dbo].[CHRNG_PFDETAIL].[SALESHEADERID] = [HIS].[dbo].[CHRNG_SALESHEADER].[ID]
    LEFT JOIN [HIS].[dbo].[AR_IPDOPDPYMNTDETAIL]
        ON [HIS].[dbo].[HIS_PFRECORD].[ID] = [HIS].[dbo].[AR_IPDOPDPYMNTDETAIL].[PFRECID]


    WHERE
        [HIS].[dbo].[HIS_PFRECORD].[ID] BETWEEN :start_id AND :end_id
        AND [HIS].[dbo].[CHRNG_SALESHEADER].[TRANSDATETIME] BETWEEN :start_date_use AND :end_date_use
        AND [HIS].[dbo].[HIS_PFRECORD].[SERVICEEND] < :start_date_use
        AND [HIS].[dbo].[HIS_PFRECORD].[CANCELLED] = 'N'
    """
    ## 2nd query is about the payment to the docotor within the set range of provided, despite services rendered past behind the set range

    print()
    # Convert chunk_ranges to a list of dictionaries
    chunk_ranges_dict = [{'start_id': start, 'end_id': end} for start, end in chunk_ranges]

    # Combine the queries using UNION
    combined_query = f"{base_query} UNION {base_query2}"

    # List to hold the DataFrames from each chunk
    all_chunks = []


    # Iterate over chunk ranges and execute the query
    for start_id, end_id in tqdm(chunk_ranges):  # Unpacking tuple
        # Execute the combined query
        result_df = execute_query(engine, combined_query, {'start_id': start_id, 'end_id': end_id, 'start_date_use': start_date, 'end_date_use': end_date})

        # Append the result to the list
        all_chunks.append(result_df)

        # Optionally save each chunk to a separate file (if needed)
        # output_path = f"query_results_{start_id}_to_{end_id}.xlsx"
        # result_df.to_excel(output_path, index=False)  # Save without the index column
        print(f"Chunk {start_id} to {end_id} exported")

    # Merge all DataFrames into a single DataFrame
    merged_df = pd.concat(all_chunks, ignore_index=True)

    # to remove duplicate ID's and 
        # to remove duplicate ID's and 
    def omit_duplicate(df, id_column, timestamp_column):
        
        duplicated_ids = df[id_column].duplicated(keep=False)

        if duplicated_ids.any():
         return df.loc[df.groupby(id_column)[timestamp_column].idxmin()]
        

        
    # to exclude WH computation
    def wh_exclusion(df, jobtitle_column, ewt_payable_column):
        to_excludeJobTitle = [7, 18, 26, 27, 28, 56, 64, 107, 108, 125, 131, 146]

        #if the jobtitle in array, then set ewt_payable to 0
        df.loc[df[jobtitle_column].isin(to_excludeJobTitle), ewt_payable_column] = 0
        return df
        

    # Example usage:
    # Replace 'ID' with the actual name of your ID column
    cleaned_result = omit_duplicate(merged_df, 'ID', 'PAYTIME_PERSONAL')

    cleaned_df = wh_exclusion(cleaned_result, 'JOBTITLEID', 'EWT_PAYABLE')


    # Save the merged result to an Excel file
    merged_output_path = f"(Output) full_query_results_{start_date}_to_{end_date}.xlsx"
    merged_df.to_excel(merged_output_path, index=False)

    print(f"All chunks merged and exported to {merged_output_path}")

    # Display the result
    print(merged_df.head(70))
    print(start_id)
    print(end_id)
    print("Hello World")