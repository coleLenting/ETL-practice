import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

log_file = "log_file.txt"
target_file = "transformed_data.csv"

# Extraction functions
def extract_from_csv(file_to_process):
    df = pd.read_csv(file_to_process)
    return df

def extract_from_json(file_to_process):
    df = pd.read_json(file_to_process, lines=True)
    return df

def extract_from_xml(file_to_process):
    records = []
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    for record in root:
        model = record.find("car_model").text
        year = int(record.find("year_of_manufacture").text)
        price = float(record.find("price").text)
        fuel = record.find("fuel").text
        records.append({
            "car_model": model,
            "year_of_manufacture": year,
            "price": price,
            "fuel": fuel
        })
    df = pd.DataFrame(records)
    return df

def extract():
    extracted_data = pd.DataFrame(columns=['car_model','year_of_manufacture','price','fuel'])

    # Process CSV files
    for csvfile in glob.glob("*.csv"):
        if csvfile != target_file:
            df = extract_from_csv(csvfile)
            if not extracted_data.empty and not df.empty:
                extracted_data = pd.concat([extracted_data, df], ignore_index=True)
            elif extracted_data.empty and not df.empty:
                extracted_data = df.copy()

   # JSON files
    for jsonfile in glob.glob("*.json"):
        df = extract_from_json(jsonfile)
        if not extracted_data.empty and not df.empty:
            extracted_data = pd.concat([extracted_data, df], ignore_index=True)
        elif extracted_data.empty and not df.empty:
            extracted_data = df.copy()

# XML files
    for xmlfile in glob.glob("*.xml"):
        df = extract_from_xml(xmlfile)
        if not extracted_data.empty and not df.empty:
            extracted_data = pd.concat([extracted_data, df], ignore_index=True)
        elif extracted_data.empty and not df.empty:
            extracted_data = df.copy()
            
    return extracted_data

# Transformation 
def transform(data):
    ''' Round price to 2 decimal places '''
    data['price'] = data['price'].round(2)
    return data

# Loading and Logging
def load_data(target_file, transformed_data):
    transformed_data.to_csv(target_file, index=False)

def log_progress(message):
    timestamp_format = '%Y-%m-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(timestamp + ',' + message + '\n')

# Main ETL Pipeline
log_progress("ETL Job Started")

log_progress("Extract phase Started")
extracted_data = extract()
log_progress("Extract phase Ended")

log_progress("Transform phase Started")
transformed_data = transform(extracted_data)
print("Transformed Data")
print(transformed_data.head())
log_progress("Transform phase Ended")

log_progress("Load phase Started")
load_data(target_file, transformed_data)
log_progress("Load phase Ended")

log_progress("ETL Job Ended")
