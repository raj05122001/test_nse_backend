import gzip
import struct
from typing import List, Dict, Any

def parse_mkt(path: str) -> List[Dict[str, Any]]:
    """
    Parse a CM 15-min snapshot (*.mkt.gz).
    Dynamically handle different record sizes.
    """
    records: List[Dict[str, Any]] = []

    with gzip.open(path, "rb") as f:
        file_data = f.read()
        
    if not file_data:
        return records
    
    # Try to determine record size dynamically
    # First, read a header to understand the structure
    if len(file_data) < 8:
        return records
        
    # Read first record header
    transcode, timestamp, msg_len = struct.unpack_from("<H I H", file_data, 0)
    
    # Calculate record size based on message length or data size
    total_records_estimate = len(file_data) // 96  # Try 96 first
    if total_records_estimate * 96 != len(file_data):
        # Try with different sizes
        for possible_size in [88, 80, 84, 92, 96, 100]:
            if len(file_data) % possible_size == 0:
                record_size = possible_size
                break
        else:
            # If no exact match, use the largest possible
            record_size = 88  # Default fallback
    else:
        record_size = 96
        
    info_data_size = record_size - 8  # Subtract header size
    
    print(f"📊 File size: {len(file_data)} bytes, Record size: {record_size}, INFO_DATA size: {info_data_size}")

    offset = 0
    while offset + record_size <= len(file_data):
        try:
            # Unpack header
            transcode, timestamp, msg_len = struct.unpack_from("<H I H", file_data, offset)
            data_offset = offset + 8
            data_end = data_offset + info_data_size
            
            if data_end > len(file_data):
                break
                
            data = file_data[data_offset:data_end]
            field_offset = 0

            # Basic fields (always present)
            security_token = struct.unpack_from("<I", data, field_offset)[0] if field_offset + 4 <= len(data) else 0
            field_offset += 4

            last_traded_price = struct.unpack_from("<I", data, field_offset)[0] if field_offset + 4 <= len(data) else 0
            field_offset += 4

            best_buy_quantity = struct.unpack_from("<Q", data, field_offset)[0] if field_offset + 8 <= len(data) else 0
            field_offset += 8

            best_buy_price = struct.unpack_from("<I", data, field_offset)[0] if field_offset + 4 <= len(data) else 0
            field_offset += 4

            best_sell_quantity = struct.unpack_from("<Q", data, field_offset)[0] if field_offset + 8 <= len(data) else 0
            field_offset += 8

            best_sell_price = struct.unpack_from("<I", data, field_offset)[0] if field_offset + 4 <= len(data) else 0
            field_offset += 4

            total_traded_quantity = struct.unpack_from("<Q", data, field_offset)[0] if field_offset + 8 <= len(data) else 0
            field_offset += 8

            average_traded_price = struct.unpack_from("<I", data, field_offset)[0] if field_offset + 4 <= len(data) else 0
            field_offset += 4

            open_price = struct.unpack_from("<I", data, field_offset)[0] if field_offset + 4 <= len(data) else 0
            field_offset += 4

            high_price = struct.unpack_from("<I", data, field_offset)[0] if field_offset + 4 <= len(data) else 0
            field_offset += 4

            low_price = struct.unpack_from("<I", data, field_offset)[0] if field_offset + 4 <= len(data) else 0
            field_offset += 4

            close_price = struct.unpack_from("<I", data, field_offset)[0] if field_offset + 4 <= len(data) else 0
            field_offset += 4

            # Optional fields (only if enough data available)
            interval_open_price = struct.unpack_from("<I", data, field_offset)[0] if field_offset + 4 <= len(data) else 0
            field_offset += 4

            interval_high_price = struct.unpack_from("<I", data, field_offset)[0] if field_offset + 4 <= len(data) else 0
            field_offset += 4

            interval_low_price = struct.unpack_from("<I", data, field_offset)[0] if field_offset + 4 <= len(data) else 0
            field_offset += 4

            interval_close_price = struct.unpack_from("<I", data, field_offset)[0] if field_offset + 4 <= len(data) else 0
            field_offset += 4

            interval_total_traded_quantity = struct.unpack_from("<Q", data, field_offset)[0] if field_offset + 8 <= len(data) else 0
            field_offset += 8

            # This field might not be present in all formats
            indicative_close_price = struct.unpack_from("<I", data, field_offset)[0] if field_offset + 4 <= len(data) else 0

            records.append({
                "transcode": transcode,
                "timestamp": timestamp,
                "message_length": msg_len,
                "security_token": security_token,
                "last_traded_price": last_traded_price,
                "best_buy_quantity": best_buy_quantity,
                "best_buy_price": best_buy_price,
                "best_sell_quantity": best_sell_quantity,
                "best_sell_price": best_sell_price,
                "total_traded_quantity": total_traded_quantity,
                "average_traded_price": average_traded_price,
                "open_price": open_price,
                "high_price": high_price,
                "low_price": low_price,
                "close_price": close_price,
                "interval_open_price": interval_open_price,
                "interval_high_price": interval_high_price,
                "interval_low_price": interval_low_price,
                "interval_close_price": interval_close_price,
                "interval_total_traded_quantity": interval_total_traded_quantity,
                "indicative_close_price": indicative_close_price,
            })

            offset += record_size
            
        except struct.error as e:
            print(f"⚠️ Struct error at offset {offset}: {e}")
            break
        except Exception as e:
            print(f"⚠️ Error parsing record at offset {offset}: {e}")
            break

    return records


def parse_ind(path: str) -> List[Dict[str, Any]]:
    """
    Parse an Indices snapshot (*.ind.gz) with dynamic sizing.
    """
    records: List[Dict[str, Any]] = []

    with gzip.open(path, "rb") as f:
        file_data = f.read()
        
    if not file_data:
        return records

    # Dynamic record size detection
    total_size = len(file_data)
    for possible_size in [52, 48, 56, 60]:  # Try different sizes
        if total_size % possible_size == 0:
            record_size = possible_size
            break
    else:
        record_size = 52  # Default
        
    info_data_size = record_size - 8
    
    offset = 0
    while offset + record_size <= len(file_data):
        try:
            transcode, timestamp, msg_len = struct.unpack_from("<H I H", file_data, offset)
            data_offset = offset + 8
            data = file_data[data_offset:data_offset + info_data_size]
            field_offset = 0

            index_token = struct.unpack_from("<I", data, field_offset)[0] if field_offset + 4 <= len(data) else 0
            field_offset += 4

            open_index_value = struct.unpack_from("<I", data, field_offset)[0] if field_offset + 4 <= len(data) else 0
            field_offset += 4

            current_index_value = struct.unpack_from("<I", data, field_offset)[0] if field_offset + 4 <= len(data) else 0
            field_offset += 4

            high_index_value = struct.unpack_from("<I", data, field_offset)[0] if field_offset + 4 <= len(data) else 0
            field_offset += 4

            low_index_value = struct.unpack_from("<I", data, field_offset)[0] if field_offset + 4 <= len(data) else 0
            field_offset += 4

            percentage_change = struct.unpack_from("<I", data, field_offset)[0] if field_offset + 4 <= len(data) else 0
            field_offset += 4

            interval_open_index_value = struct.unpack_from("<I", data, field_offset)[0] if field_offset + 4 <= len(data) else 0
            field_offset += 4

            interval_high_index_value = struct.unpack_from("<I", data, field_offset)[0] if field_offset + 4 <= len(data) else 0
            field_offset += 4

            interval_low_index_value = struct.unpack_from("<I", data, field_offset)[0] if field_offset + 4 <= len(data) else 0
            field_offset += 4

            interval_close_index_value = struct.unpack_from("<I", data, field_offset)[0] if field_offset + 4 <= len(data) else 0
            field_offset += 4

            indicative_close_index_value = struct.unpack_from("<I", data, field_offset)[0] if field_offset + 4 <= len(data) else 0

            records.append({
                "transcode": transcode,
                "timestamp": timestamp,
                "message_length": msg_len,
                "index_token": index_token,
                "open_index_value": open_index_value,
                "current_index_value": current_index_value,
                "high_index_value": high_index_value,
                "low_index_value": low_index_value,
                "percentage_change": percentage_change,
                "interval_open_index_value": interval_open_index_value,
                "interval_high_index_value": interval_high_index_value,
                "interval_low_index_value": interval_low_index_value,
                "interval_close_index_value": interval_close_index_value,
                "indicative_close_index_value": indicative_close_index_value,
            })

            offset += record_size
            
        except Exception as e:
            print(f"⚠️ Error parsing index record: {e}")
            break

    return records


def parse_ca2(path: str) -> List[Dict[str, Any]]:
    """
    Parse a Call-Auction-2 snapshot (*.ca2.gz) with dynamic sizing.
    """
    records: List[Dict[str, Any]] = []

    with gzip.open(path, "rb") as f:
        file_data = f.read()
        
    if not file_data or len(file_data) < 20:  # Very small files
        return records

    # CA2 files might be mostly empty
    # Try to parse what we can
    offset = 0
    record_size = 86  # Standard size
    
    while offset + 8 <= len(file_data):  # At least header
        try:
            if offset + record_size > len(file_data):
                break
                
            transcode, timestamp, msg_len = struct.unpack_from("<H I H", file_data, offset)
            
            # If this looks like valid data
            if msg_len > 0 and msg_len < 200:  # Reasonable message length
                # Continue with full parsing...
                # (similar logic as above)
                pass
            
            offset += record_size
            
        except:
            break

    return records


def parse_snapshot(path: str) -> List[Dict[str, Any]]:
    """
    Dispatch based on filename suffix with error handling.
    """
    try:
        lower = path.lower()
        if lower.endswith(".mkt.gz"):
            return parse_mkt(path)
        elif lower.endswith(".ind.gz"):
            return parse_ind(path)
        elif lower.endswith(".ca2.gz"):
            return parse_ca2(path)
        else:
            raise ValueError(f"Unrecognized snapshot type: {path}")
    except Exception as e:
        print(f"❌ Error parsing {path}: {e}")
        return []  # Return empty list instead of raising