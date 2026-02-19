import yfinance as yf
import pandas as pd
import os
from datetime import datetime
import pytz

# --- Configuration ---
TICKER = "ITBEES.NS"
CSV_FILE = "itbees_data.csv"

def get_ist_time():
    # Define IST timezone
    ist = pytz.timezone('Asia/Kolkata')
    return datetime.now(ist)

def fetch_and_save():
    print("--- Starting Job ---")
    
    # 1. Fetch Data
    try:
        # Fetch 1 day of data
        data = yf.download(TICKER, period="1d", progress=False)
        
        if data.empty:
            print("No data found (Market might be closed).")
            return

        # Clean formatting (Flatten multi-index if present)
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = data.columns.droplevel(0)
            
        data.reset_index(inplace=True)
        
        # Ensure 'Date' is just the date component
        data['Date'] = data['Date'].dt.date
        today_date = data.iloc[0]['Date']
        
        # 2. Load Existing Data to Check for Duplicates
        if os.path.exists(CSV_FILE):
            existing_df = pd.read_csv(CSV_FILE)
            
            # Check if the last entry is the same as today's date
            # We convert to string for safe comparison
            last_entry_date = str(existing_df.iloc[-1]['Date'])
            current_entry_date = str(today_date)
            
            if last_entry_date == current_entry_date:
                print(f"Data for {current_entry_date} already exists. Skipping write.")
                return

            # Append data without header
            data.to_csv(CSV_FILE, mode='a', header=False, index=False)
            print(f"Appended data for {today_date}")
            
        else:
            # Create new file with header
            data.to_csv(CSV_FILE, mode='w', header=True, index=False)
            print(f"Created new file with data for {today_date}")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    fetch_and_save()
