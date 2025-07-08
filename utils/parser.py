import gzip
import struct
from typing import List, Dict, Any

def parse_mkt(path: str) -> List[Dict[str, Any]]:
    """
    Parse a CM 15-min snapshot (*.mkt.gz).
    Header (8 bytes) + INFO_DATA (88 bytes) = 96 bytes/record.
    """
    RECORD_SIZE = 8 + 88
    records: List[Dict[str, Any]] = []

    with gzip.open(path, "rb") as f:
        while True:
            chunk = f.read(RECORD_SIZE)
            if len(chunk) < RECORD_SIZE:
                break

            # Unpack header
            transcode, timestamp, msg_len = struct.unpack_from("<H I H", chunk, 0)
            data = chunk[8:]
            offset = 0

            # Unpack INFO_DATA fields
            security_token = struct.unpack_from("<I", data, offset)[0]
            offset += 4

            last_traded_price = struct.unpack_from("<I", data, offset)[0]
            offset += 4

            best_buy_quantity = struct.unpack_from("<Q", data, offset)[0]
            offset += 8

            best_buy_price = struct.unpack_from("<I", data, offset)[0]
            offset += 4

            best_sell_quantity = struct.unpack_from("<Q", data, offset)[0]
            offset += 8

            best_sell_price = struct.unpack_from("<I", data, offset)[0]
            offset += 4

            total_traded_quantity = struct.unpack_from("<Q", data, offset)[0]
            offset += 8

            average_traded_price = struct.unpack_from("<I", data, offset)[0]
            offset += 4

            open_price = struct.unpack_from("<I", data, offset)[0]
            offset += 4

            high_price = struct.unpack_from("<I", data, offset)[0]
            offset += 4

            low_price = struct.unpack_from("<I", data, offset)[0]
            offset += 4

            close_price = struct.unpack_from("<I", data, offset)[0]
            offset += 4

            interval_open_price = struct.unpack_from("<I", data, offset)[0]
            offset += 4

            interval_high_price   = struct.unpack_from("<I", data, offset)[0]; offset += 4
            interval_low_price    = struct.unpack_from("<I", data, offset)[0]; offset += 4
            interval_open_price   = struct.unpack_from("<I", data, offset)[0]; offset += 4
            interval_close_price  = struct.unpack_from("<I", data, offset)[0]; offset += 4
            interval_total_traded_quantity = struct.unpack_from("<Q", data, offset)[0]; offset += 8
            indicative_close_price        = struct.unpack_from("<I", data, offset)[0]; offset += 4

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

    return records


def parse_ind(path: str) -> List[Dict[str, Any]]:
    """
    Parse an Indices snapshot (*.ind.gz).
    Header (8 bytes) + INFO_DATA (44 bytes) = 52 bytes/record.
    """
    RECORD_SIZE = 8 + 44
    records: List[Dict[str, Any]] = []

    with gzip.open(path, "rb") as f:
        while True:
            chunk = f.read(RECORD_SIZE)
            if len(chunk) < RECORD_SIZE:
                break

            transcode, timestamp, msg_len = struct.unpack_from("<H I H", chunk, 0)
            data = chunk[8:]
            offset = 0

            index_token = struct.unpack_from("<I", data, offset)[0]
            offset += 4

            open_index_value = struct.unpack_from("<I", data, offset)[0]
            offset += 4

            current_index_value = struct.unpack_from("<I", data, offset)[0]
            offset += 4

            high_index_value = struct.unpack_from("<I", data, offset)[0]
            offset += 4

            low_index_value = struct.unpack_from("<I", data, offset)[0]
            offset += 4

            percentage_change = struct.unpack_from("<I", data, offset)[0]
            offset += 4

            interval_open_index_value = struct.unpack_from("<I", data, offset)[0]
            offset += 4

            interval_high_index_value = struct.unpack_from("<I", data, offset)[0]
            offset += 4

            interval_low_index_value = struct.unpack_from("<I", data, offset)[0]
            offset += 4

            interval_close_index_value = struct.unpack_from("<I", data, offset)[0]
            offset += 4

            indicative_close_index_value = struct.unpack_from("<I", data, offset)[0]
            offset += 4

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

    return records


def parse_ca2(path: str) -> List[Dict[str, Any]]:
    """
    Parse a Call-Auction-2 snapshot (*.ca2.gz).
    Header (8 bytes) + INFO_DATA (78 bytes) = 86 bytes/record.
    """
    RECORD_SIZE = 8 + 78
    records: List[Dict[str, Any]] = []

    with gzip.open(path, "rb") as f:
        while True:
            chunk = f.read(RECORD_SIZE)
            if len(chunk) < RECORD_SIZE:
                break

            transcode, timestamp, msg_len = struct.unpack_from("<H I H", chunk, 0)
            data = chunk[8:]
            offset = 0

            security_token = struct.unpack_from("<I", data, offset)[0]
            offset += 4

            last_traded_price = struct.unpack_from("<I", data, offset)[0]
            offset += 4

            best_buy_quantity = struct.unpack_from("<Q", data, offset)[0]
            offset += 8

            best_buy_price = struct.unpack_from("<I", data, offset)[0]
            offset += 4

            buy_bbmm_flag = struct.unpack_from("<c", data, offset)[0].decode()
            offset += 1

            best_sell_quantity = struct.unpack_from("<Q", data, offset)[0]
            offset += 8

            best_sell_price = struct.unpack_from("<I", data, offset)[0]
            offset += 4

            sell_bbmm_flag = struct.unpack_from("<c", data, offset)[0].decode()
            offset += 1

            total_traded_quantity = struct.unpack_from("<Q", data, offset)[0]
            offset += 8

            indicative_traded_quantity = struct.unpack_from("<Q", data, offset)[0]
            offset += 8

            average_traded_price = struct.unpack_from("<I", data, offset)[0]
            offset += 4

            first_open_price = struct.unpack_from("<I", data, offset)[0]
            offset += 4

            open_price = struct.unpack_from("<I", data, offset)[0]
            offset += 4

            high_price = struct.unpack_from("<I", data, offset)[0]
            offset += 4

            low_price = struct.unpack_from("<I", data, offset)[0]
            offset += 4

            close_price = struct.unpack_from("<I", data, offset)[0]
            offset += 4

            # skip any padding at the end

            records.append({
                "transcode": transcode,
                "timestamp": timestamp,
                "message_length": msg_len,
                "security_token": security_token,
                "last_traded_price": last_traded_price,
                "best_buy_quantity": best_buy_quantity,
                "best_buy_price": best_buy_price,
                "buy_bbmm_flag": buy_bbmm_flag,
                "best_sell_quantity": best_sell_quantity,
                "best_sell_price": best_sell_price,
                "sell_bbmm_flag": sell_bbmm_flag,
                "total_traded_quantity": total_traded_quantity,
                "indicative_traded_quantity": indicative_traded_quantity,
                "average_traded_price": average_traded_price,
                "first_open_price": first_open_price,
                "open_price": open_price,
                "high_price": high_price,
                "low_price": low_price,
                "close_price": close_price,
            })

    return records


def parse_snapshot(path: str) -> List[Dict[str, Any]]:
    """
    Dispatch based on filename suffix.
    """
    lower = path.lower()
    if lower.endswith(".mkt.gz"):
        return parse_mkt(path)
    elif lower.endswith(".ind.gz"):
        return parse_ind(path)
    elif lower.endswith(".ca2.gz"):
        return parse_ca2(path)
    else:
        raise ValueError(f"Unrecognized snapshot type: {path}")
