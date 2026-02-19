import yfinance as yf
import pandas as pd
import os
from datetime import datetime, timedelta

# --- CONFIGURATION ---
FILE_NAME = "itbees_master_dataset.csv"
TICKERS = {
    "Target": "ITBEES.NS",
    "Nifty_IT": "^CNXIT",
    "TCS": "TCS.NS",
    "Infosys": "INFY.NS",
    "Nasdaq_Fut": "NQ=F",  # Global Sentiment
    "USD_INR": "INR=X",    # Currency
    "India_VIX": "^INDIAVIX" # Volatility
}

def fetch_and_align_data():
    
    df_list = []
    
    for name, symbol in TICKERS.items():
        try:
            # Download 7 days of 1m data
            ticker_df = yf.download(symbol, period="7d", interval="1m", progress=False)
            
            if ticker_df.empty:
                print(f"Warning: No data found for {symbol}")
                continue
                
            # Keep only Close column and rename it
            # Handle MultiIndex columns if they exist (common in new yfinance versions)
            if isinstance(ticker_df.columns, pd.MultiIndex):
                close_col = ticker_df['Close']
            else:
                close_col = ticker_df['Close']
                
            close_col.name = f"{name}_Close"
            df_list.append(close_col)
            
        except Exception as e:
            print(f"Error fetching {symbol}: {e}")

    # Merge all into one DataFrame based on timestamp
    combined_df = pd.concat(df_list, axis=1)
    
    # --- CLEANING & ALIGNMENT ---
    # 1. Forward Fill: If Nasdaq updates but TCS doesn't (due to seconds diff), fill the gap.
    combined_df.ffill(inplace=True)
    
    # 2. Drop rows where the TARGET (ITBEES) is missing (Market was closed)
    combined_df.dropna(subset=['Target_Close'], inplace=True)
    
    # 3. Convert to IST (Indian Standard Time)
    # yfinance usually returns UTC. We convert to Asia/Kolkata
    if combined_df.index.tz is None:
        # If naive, assume UTC then convert
        combined_df.index = combined_df.index.tz_localize('UTC')
    combined_df.index = combined_df.index.tz_convert('Asia/Kolkata')
    
    # 4. Filter for Indian Market Hours only (09:15 to 15:30)
    combined_df = combined_df.between_time('09:15', '15:30')
    
    return combined_df

def update_csv(new_data):
    # Initialize stats
    rows_appended = 0
    total_rows = 0
    
    if not os.path.exists(FILE_NAME):
        new_data.to_csv(FILE_NAME)
        rows_appended = len(new_data)
        total_rows = rows_appended
        print(f"Created new dataset: {FILE_NAME} with {rows_appended} rows.")
    else:
        existing_df = pd.read_csv(FILE_NAME, index_col=0, parse_dates=True)
        if existing_df.index.tz is None:
             existing_df.index = existing_df.index.tz_localize('Asia/Kolkata')
        
        last_timestamp = existing_df.index.max()
        fresh_rows = new_data[new_data.index > last_timestamp]
        
        if not fresh_rows.empty:
            fresh_rows.to_csv(FILE_NAME, mode='a', header=False)
            rows_appended = len(fresh_rows)
            print(f"Appended {rows_appended} new rows.")
        else:
            print("No new data to append.")
            
        # Recount total rows after append
        total_rows = len(existing_df) + rows_appended

    return rows_appended, total_rows

if __name__ == "__main__":
    try:
        df = fetch_and_align_data()
        if df is not None and not df.empty:
            added, total = update_csv(df)
            status = "Success"
        else:
            added = 0
            # If fetch failed, try to count existing rows, or 0
            total = 0 
            if os.path.exists(FILE_NAME):
                total = sum(1 for line in open(FILE_NAME)) - 1 # rough count
            status = "No Data Fetched"

        # --- WRITE TO GITHUB OUTPUT ---
        if "GITHUB_OUTPUT" in os.environ:
            with open(os.environ["GITHUB_OUTPUT"], "a") as f:
                f.write(f"status={status}\n")
                f.write(f"added={added}\n")
                f.write(f"total={total}\n")
                
    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
        if "GITHUB_OUTPUT" in os.environ:
            with open(os.environ["GITHUB_OUTPUT"], "a") as f:
                f.write(f"status=Failed\n")
                f.write(f"added=0\n")
                f.write(f"total=Unknown\n")
