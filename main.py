import yfinance as yf
import pandas as pd
import os
import sys

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
    print("🚀 Fetching last 7 days of 1-minute data...")
    df_list = []
    
    for name, symbol in TICKERS.items():
        try:
            # Download 7 days of 1m data
            ticker_df = yf.download(symbol, period="7d", interval="1m", progress=False)
            
            if ticker_df.empty:
                print(f"Warning: No data found for {symbol}")
                continue
            
            # Handle MultiIndex columns (common in new yfinance versions)
            if isinstance(ticker_df.columns, pd.MultiIndex):
                close_col = ticker_df['Close']
            else:
                close_col = ticker_df['Close']
                
            close_col.name = f"{name}_Close"
            df_list.append(close_col)
            
        except Exception as e:
            print(f"Error fetching {symbol}: {e}")

    if not df_list:
        return None

    # Merge all into one DataFrame based on timestamp
    combined_df = pd.concat(df_list, axis=1)
    
    # --- CLEANING & ALIGNMENT ---
    combined_df.ffill(inplace=True)
    combined_df.dropna(subset=['Target_Close'], inplace=True)
    
    # Convert to IST
    if combined_df.index.tz is None:
        combined_df.index = combined_df.index.tz_localize('UTC')
    combined_df.index = combined_df.index.tz_convert('Asia/Kolkata')
    
    # Filter for Indian Market Hours (09:15 to 15:30)
    combined_df = combined_df.between_time('09:15', '15:30')
    
    return combined_df

def update_csv(new_data):
    rows_appended = 0
    total_rows = 0
    
    if not os.path.exists(FILE_NAME):
        new_data.to_csv(FILE_NAME)
        rows_appended = len(new_data)
        total_rows = rows_appended
        print(f"Created new dataset: {FILE_NAME} with {rows_appended} rows.")
    else:
        existing_df = pd.read_csv(FILE_NAME, index_col=0, parse_dates=True)
        # Ensure existing index is timezone-aware
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
            
        total_rows = len(existing_df) + rows_appended

    return rows_appended, total_rows

if __name__ == "__main__":
    status = "Failed"
    added = 0
    total = 0

    try:
        df = fetch_and_align_data()
        if df is not None and not df.empty:
            added, total = update_csv(df)
            status = "Success"
        else:
            status = "No Data Fetched"
            # Try to get existing total count even if fetch failed
            if os.path.exists(FILE_NAME):
                 with open(FILE_NAME) as f:
                     total = sum(1 for line in f) - 1
            
    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
        status = "Failed"

    # --- WRITE TO GITHUB OUTPUT ---
    # This is critical for the YAML file to read the results
    if "GITHUB_OUTPUT" in os.environ:
        with open(os.environ["GITHUB_OUTPUT"], "a") as f:
            f.write(f"status={status}\n")
            f.write(f"added={added}\n")
            f.write(f"total={total}\n")
    else:
        # For local testing
        print(f"\n[LOCAL DEBUG] Status: {status}, Added: {added}, Total: {total}")
