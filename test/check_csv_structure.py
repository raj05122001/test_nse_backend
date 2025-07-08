import pandas as pd
import os

def check_csv_structure():
    """Check the structure of the CSV file"""
    
    csv_file = "cm_contract_stream_info.csv"
    
    if not os.path.exists(csv_file):
        print(f"❌ File not found: {csv_file}")
        return
    
    try:
        # Try different encodings
        encodings = ['utf-8', 'latin-1', 'cp1252']
        df = None
        
        for encoding in encodings:
            try:
                df = pd.read_csv(csv_file, encoding=encoding)
                print(f"✅ Successfully read with encoding: {encoding}")
                break
            except UnicodeDecodeError:
                continue
        
        if df is None:
            print("❌ Could not read CSV with any encoding")
            return
            
        print(f"\n📊 CSV File Analysis:")
        print(f"   Shape: {df.shape[0]} rows × {df.shape[1]} columns")
        print(f"   File size: {os.path.getsize(csv_file)} bytes")
        
        print(f"\n📋 Column names:")
        for i, col in enumerate(df.columns):
            print(f"   {i+1}. '{col}'")
        
        print(f"\n🔍 First 3 rows:")
        print(df.head(3).to_string())
        
        print(f"\n📈 Data types:")
        print(df.dtypes.to_string())
        
        # Check for common NSE stock symbols
        if 'Symbol' in df.columns:
            symbol_col = 'Symbol'
        elif 'symbol' in df.columns:
            symbol_col = 'symbol'
        else:
            symbol_col = df.columns[0]  # Assume first column is symbol
            
        print(f"\n🔎 Sample symbols (using column '{symbol_col}'):")
        sample_symbols = df[symbol_col].head(10).tolist()
        for symbol in sample_symbols:
            print(f"   - {symbol}")
            
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")

if __name__ == "__main__":
    check_csv_structure()