import struct
import csv
import pandas as pd
from datetime import datetime
import os

class SecuritiesConverter:
    def __init__(self):
        # Different possible formats based on NSE versions
        self.header_format = '<HLH'  # 8 bytes: SHORT, LONG, SHORT
        
    def analyze_file_structure(self, file_path):
        """Analyze the file to understand its structure"""
        file_size = os.path.getsize(file_path)
        print(f"File size: {file_size} bytes")
        
        with open(file_path, 'rb') as f:
            # Read first few bytes to analyze
            first_bytes = f.read(100)
            print(f"First 20 bytes (hex): {first_bytes[:20].hex()}")
            
            # Try to find patterns
            f.seek(0)
            
            # Look for potential record boundaries
            record_count = 0
            positions = []
            
            while f.tell() < min(file_size, 10000):  # Analyze first 10KB
                pos = f.tell()
                try:
                    # Try reading header
                    header_data = f.read(8)
                    if len(header_data) < 8:
                        break
                        
                    transcode, timestamp, message_length = struct.unpack(self.header_format, header_data)
                    
                    if transcode == 7 and 100 < message_length < 200:  # Security info
                        positions.append(pos)
                        remaining = message_length - 8
                        f.read(remaining)  # Skip to next record
                        record_count += 1
                    else:
                        f.seek(pos + 1)  # Move one byte and try again
                        
                except:
                    f.seek(pos + 1)
                    
                if record_count > 10:  # Found enough records
                    break
            
            if len(positions) >= 2:
                record_size = positions[1] - positions[0]
                data_size = record_size - 8
                print(f"Detected record size: {record_size} bytes")
                print(f"Data portion size: {data_size} bytes")
                return data_size
            
        return None
    
    def extract_securities_dynamic(self, file_path):
        """Extract securities with dynamic parsing"""
        securities = []
        
        with open(file_path, 'rb') as f:
            file_size = os.path.getsize(file_path)
            
            while f.tell() < file_size:
                pos = f.tell()
                try:
                    # Read header
                    header_data = f.read(8)
                    if len(header_data) < 8:
                        break
                    
                    transcode, timestamp, message_length = struct.unpack(self.header_format, header_data)
                    
                    if transcode == 7:  # Security information
                        # Read the data portion
                        data_size = message_length - 8
                        data = f.read(data_size)
                        
                        if len(data) == data_size:
                            security = self.parse_security_dynamic(data)
                            if security:
                                securities.append(security)
                        else:
                            break
                    else:
                        # Skip unknown records
                        remaining = message_length - 8
                        if remaining > 0:
                            f.read(remaining)
                
                except Exception as e:
                    # If we can't parse, try moving one byte forward
                    f.seek(pos + 1)
                    continue
        
        return securities
    
    def parse_security_dynamic(self, data):
        """Parse security data dynamically based on data length"""
        try:
            if len(data) >= 113:  # Version 1.24 format
                return self.parse_v124_format(data)
            elif len(data) >= 100:  # Older format
                return self.parse_older_format(data)
            else:
                return self.parse_minimal_format(data)
        except:
            return None
    
    def parse_v124_format(self, data):
        """Parse Version 1.24 format (113 bytes)"""
        try:
            # Token Number (4) + Symbol (10) + Series (2) + Issued Capital (8)
            token_number = struct.unpack('<L', data[0:4])[0]
            symbol = data[4:14].decode('utf-8', errors='ignore').rstrip('\x00')
            series = data[14:16].decode('utf-8', errors='ignore').rstrip('\x00')
            
            # Try to extract more fields carefully
            issued_capital = struct.unpack('<d', data[16:24])[0] if len(data) >= 24 else 0
            settlement_cycle = struct.unpack('<H', data[24:26])[0] if len(data) >= 26 else 0
            
            # Company name (around position 50-75)
            company_name = ""
            for start_pos in range(40, min(80, len(data) - 25)):
                try:
                    name_candidate = data[start_pos:start_pos+25].decode('utf-8', errors='ignore').rstrip('\x00')
                    if len(name_candidate) > len(company_name) and name_candidate.isprintable():
                        company_name = name_candidate
                except:
                    continue
            
            # Permitted to trade (last 2 bytes before end)
            permitted_to_trade = 1  # Default
            if len(data) >= 113:
                try:
                    permitted_to_trade = struct.unpack('<H', data[111:113])[0]
                except:
                    permitted_to_trade = 1
            
            return {
                'token_number': token_number,
                'symbol': symbol,
                'series': series,
                'issued_capital': issued_capital,
                'settlement_cycle': settlement_cycle,
                'company_name': company_name,
                'permitted_to_trade': permitted_to_trade,
                'data_length': len(data)
            }
        except Exception as e:
            return None
    
    def parse_older_format(self, data):
        """Parse older format"""
        try:
            token_number = struct.unpack('<L', data[0:4])[0]
            symbol = data[4:14].decode('utf-8', errors='ignore').rstrip('\x00')
            series = data[14:16].decode('utf-8', errors='ignore').rstrip('\x00')
            
            return {
                'token_number': token_number,
                'symbol': symbol,
                'series': series,
                'issued_capital': 0,
                'settlement_cycle': 0,
                'company_name': '',
                'permitted_to_trade': 1,
                'data_length': len(data)
            }
        except:
            return None
    
    def parse_minimal_format(self, data):
        """Parse minimal format"""
        try:
            if len(data) >= 4:
                token_number = struct.unpack('<L', data[0:4])[0]
                symbol = data[4:min(14, len(data))].decode('utf-8', errors='ignore').rstrip('\x00')
                
                return {
                    'token_number': token_number,
                    'symbol': symbol,
                    'series': '',
                    'issued_capital': 0,
                    'settlement_cycle': 0,
                    'company_name': '',
                    'permitted_to_trade': 1,
                    'data_length': len(data)
                }
        except:
            pass
        return None
    
    def convert_to_csv(self, dat_file_path: str, csv_file_path: str) -> pd.DataFrame | None:
        """
        Convert Securities.dat → CSV, apply formatting, print stats.
        Returns the DataFrame if successful, else None.
        """
        # 1) Analyze (optional, just prints out)
        self.analyze_file_structure(dat_file_path)

        # 2) Extract
        securities = self.extract_securities_dynamic(dat_file_path)
        if not securities:
            securities = self.try_alternative_parsing(dat_file_path)

        if not securities:
            print("❌ No securities parsed.")
            return None

        # 3) Build DataFrame + add desc columns
        df = pd.DataFrame(securities)
        df['settlement_cycle_desc'] = df['settlement_cycle'].map({
            0: 'T+0', 1: 'T+1'
        }).fillna('Unknown')
        df['permitted_to_trade_desc'] = df['permitted_to_trade'].map({
            0: 'Listed but not permitted to trade',
            1: 'Permitted to trade',
            2: 'BSE listed (BSE exclusive security)',
        }).fillna('Unknown')

        # 4) Sort & write
        df = df.sort_values('token_number')
        df.to_csv(csv_file_path, index=False)

        # 5) Print sample & stats
        print(f"✅ Converted {len(df)} records → {csv_file_path}")
        print(df[['token_number','symbol','series','company_name']].head(5))
        print(f"Total records: {len(df)} | Unique symbols: {df['symbol'].nunique()}")
        print(f"Data lengths seen: {sorted(df['data_length'].unique())}")

        return df
    def try_alternative_parsing(self, file_path):
        """Try parsing without header structure"""
        securities = []
        
        with open(file_path, 'rb') as f:
            data = f.read()
            
            # Try to find token patterns (assuming 4-byte integers)
            for i in range(0, len(data) - 20, 4):
                try:
                    token = struct.unpack('<L', data[i:i+4])[0]
                    
                    # Valid token numbers are usually reasonable integers
                    if 1 <= token <= 1000000:
                        # Try to extract symbol
                        symbol_data = data[i+4:i+14]
                        symbol = symbol_data.decode('utf-8', errors='ignore').rstrip('\x00')
                        
                        # Check if symbol looks valid (alphabetic characters)
                        if symbol and len(symbol) >= 2 and symbol.replace('$', '').isalnum():
                            series_data = data[i+14:i+16]
                            series = series_data.decode('utf-8', errors='ignore').rstrip('\x00')
                            
                            securities.append({
                                'token_number': token,
                                'symbol': symbol,
                                'series': series,
                                'issued_capital': 0,
                                'settlement_cycle': 0,
                                'company_name': '',
                                'permitted_to_trade': 1,
                                'data_length': 0
                            })
                except:
                    continue
        
        # Remove duplicates
        seen = set()
        unique_securities = []
        for sec in securities:
            key = (sec['token_number'], sec['symbol'])
            if key not in seen:
                seen.add(key)
                unique_securities.append(sec)
        
        return unique_securities
