#!/usr/bin/env python
# coding: utf-8

# # Import Library and Import Dataset through Kaggle API

# In[47]:


import pandas as pd
import os
import kaggle
import requests
import gspread
from google.oauth2.service_account import Credentials
import schedule
import time

def download_dataset():
# Set environment variable for Kaggle API (opsional)
    os.environ['KAGGLE_USERNAME'] = 'your_username'
    os.environ['KAGGLE_KEY'] = 'your_api_key'

# Download dataset
    kaggle.api.dataset_download_files('pdaasha/ga4-obfuscated-sample-ecommerce-jan2021', path='./data', unzip=True)


# # Preview and Transfrom Data

# In[48]:


df = pd.read_csv('./data/ga4_obfuscated_sample_ecommerce_Jan2021 - ga4_event_2021.csv')
    
# Convert format to datetime
df["event_date"] = pd.to_datetime(df["event_date"], format="%Y%m%d", errors='coerce')
df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], unit="us")
    
df.head(500)


# In[49]:


# ---------- STEP 2: TRANSFORM DATA ----------
def transform_data():
    # Load dataset
    df = pd.read_csv('./data/ga4_obfuscated_sample_ecommerce_Jan2021 - ga4_event_2021.csv')
    df.head(100)
    
    # Convert format to datetime
    df["event_date"] = pd.to_datetime(df["event_date"], format="%Y%m%d", errors='coerce')
    df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], unit="us")
    
    # Convert ID column to integer
    id_columns = [
        "event_bundle_sequence_id", "user_id", "user_pseudo_id",
        "device.vendor_id", "device.advertising_id", "app_info.id",
        "app_info.firebase_app_id", "stream_id", "ecommerce.transaction_id",
        "items.item_id"
    ]
    
    for col in id_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
    
    return df


# # Create a Relevant Metrics

# In[50]:


# ---------- STEP 3: CALCULATE METRICS ----------
def calculate_metrics(df):
    # Key metrics
    total_revenue = df['ecommerce.purchase_revenue_in_usd'].sum()
    total_transactions = df['ecommerce.transaction_id'].nunique() 
    aov = total_revenue / total_transactions if total_transactions > 0 else 0
    unique_users = df['user_id'].nunique()

    # Grouped metrics
    def create_group_metrics(df, group_col, metric_cols):
        return df.groupby(group_col).agg(metric_cols).reset_index()
    
    traffic_metrics = create_group_metrics(
        df, 
        'traffic_source.source',
        {'ecommerce.purchase_revenue_in_usd': 'sum', 'ecommerce.transaction_id': 'nunique'}
    )
    geo_metrics = create_group_metrics(
        df,
        'geo.country',
        {'ecommerce.purchase_revenue_in_usd': 'sum', 'ecommerce.transaction_id': 'nunique'}
    )
    device_metrics = create_group_metrics(
        df,
        'device.category',
        {'ecommerce.purchase_revenue_in_usd': 'sum', 'ecommerce.transaction_id': 'nunique'}
    )

    return {
        'total_revenue': total_revenue,
        'total_transactions': total_transactions,
        'aov': aov,
        'unique_users': unique_users,
        'traffic_metrics': traffic_metrics,
        'geo_metrics': geo_metrics,
        'device_metrics': device_metrics
    }


# # Load to Google Sheets use Google Sheets API

# In[51]:


# ---------- STEP 4: LOAD TO GOOGLE SHEETS ----------
def load_to_google_sheets(metrics_summary):
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    # Use an absolute path to credentials.json
    creds = Credentials.from_service_account_file(
        r'C:\Users\AMIF\Documents\Comms8\credentials.json', 
        scopes=scopes
    )
    client = gspread.authorize(creds)

    # Open spreadsheet
    sheet = client.open('Marketing Dashboard').sheet1
    
    # Update main data
    main_data = [
        ['Total Revenue', metrics_summary['total_revenue']],
        ['Total Transactions', metrics_summary['total_transactions']],
        ['AOV', metrics_summary['aov']],
        ['Unique Users', metrics_summary['unique_users']]
    ]
    sheet.update('A1', [['Metric', 'Value']] + main_data)

    # Traffic Metrics
    traffic_header = ['Traffic Source', 'Revenue', 'Transactions']
    traffic_data = metrics_summary['traffic_metrics'].values.tolist()
    sheet.update('A10', [traffic_header] + traffic_data)
    
    # Geo Metrics
    geo_header = ['Country', 'Revenue', 'Transactions']
    geo_data = metrics_summary['geo_metrics'].values.tolist()
    sheet.update('E10', [geo_header] + geo_data)  

    # Device Metrics
    device_header = ['Device Category', 'Revenue', 'Transactions']
    device_data = metrics_summary['device_metrics'].values.tolist()
    sheet.update('I10', [device_header] + device_data)  


    print("Dashboard updated successfully!")


# # Schedule Updates and Execution

# In[52]:


# ---------- MAIN EXECUTION ----------
def update_dashboard():
    download_dataset()
    df = transform_data()
    metrics = calculate_metrics(df)
    load_to_google_sheets(metrics)

# Schedule updates
schedule.every().hour.do(update_dashboard)  # Run every 1 hour 

if __name__ == "__main__":
    update_dashboard()


# # --END--

# In[ ]:




