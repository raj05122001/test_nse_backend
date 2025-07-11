# historical_async.py

import os
import asyncio
import time
from datetime import datetime, timedelta

import requests
from sqlalchemy import select, insert
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import SQLAlchemyError

from db.connection import AsyncSessionLocal
from db.models import CMTokenMaster, CMStockHistorical
import sys

# Windows/Linux दोनों पर stdout को UTF-8 में री-कॉन्फिगर कर देता है
sys.stdout.reconfigure(encoding='utf-8')

# ——————————————————————————————————————————————————————————————
# 1) Groww/Moneycontrol API के लिए URL टेम्प्लेट और Headers
URL_TMPL = (
    "https://priceapi.moneycontrol.com/techCharts/indianMarket/stock/history"
    "?symbol={symbol}"
    "&resolution=D"     # D = daily bars
    "&from={from_ts}"
    "&to={to_ts}"
    "&currencyCode=INR"
)

HEADERS = {
    "Accept":             "application/json, text/plain, */*",
    "Accept-Language":    "en-IN,en-GB;q=0.9,en-US;q=0.8,hi;q=0.6",
    "Cache-Control":      "no-cache",
    "Pragma":             "no-cache",
    "Content-Type":       "application/json",
    "Origin":             "https://groww.in",
    "Referer":            "https://groww.in/stocks/sectors",
    "User-Agent":         "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",

    # Sec-CH-UA headers must use straight quotes " and only ASCII:
    "Sec-CH-UA":          '"Not(A:Brand";v="99", "Google Chrome";v="133", "Chromium";v="133"',
    "Sec-CH-UA-Mobile":   "?0",
    "Sec-CH-UA-Platform": '"Linux"',
    "Sec-Fetch-Site":     "same-origin",
    "Sec-Fetch-Mode":     "cors",
    "Sec-Fetch-Dest":     "empty",

    # Groww-specific (इनमें सिर्फ ASCII रखें)
    "X-App-Id":           "growwWeb",
    "X-Device-Id":        "YOUR_DEVICE_ID_HERE",
    "X-Device-Type":      "desktop",
    "X-Platform":         "web",
    "X-Request-Id":       "YOUR_REQUEST_ID_HERE",
    "X-Request-Checksum": "YOUR_CHECKSUM_HERE",

    # Optional: अपना Cookie header ASCII में
    "Cookie": (
        "we_luid=...; _uetvid=...; _clck=...; _gcl_au=...; "
        "_ga_BNCSRMD1F4=...;"
    )
}

# ——————————————————————————————————————————————————————————————
def epoch(dt: datetime) -> int:
    """Convert datetime to Unix timestamp (seconds)."""
    return int(time.mktime(dt.timetuple()))

# ——————————————————————————————————————————————————————————————
async def fetch_all_symbols() -> list[str]:
    """
    CMTokenMaster से सभी distinct symbols लाओ
    (जिनका पहला अक्षर नंबर नहीं है, series = 'EQ' है और डुप्लीकेट नहीं हैं)।
    """
    async with AsyncSessionLocal() as session:
        stmt = (
            select(CMTokenMaster.symbol)
            .where(CMTokenMaster.series == 'EQ')
            .distinct()
        )
        result = await session.execute(stmt)
        symbols: list[str] = result.scalars().all()

    filtered: list[str] = []
    seen: set[str] = set()
    for sym in symbols:
        if not sym or sym[0].isdigit():
            continue
        if sym in seen:
            continue
        seen.add(sym)
        filtered.append(sym)

    return filtered


async def fetch_data_for(symbol: str) -> dict:
    """Synchronous requests.get को थ्रेड में चलाकर JSON वापस दे।"""
    url = f"https://priceapi.moneycontrol.com/techCharts/indianMarket/stock/history?symbol={symbol}&resolution=1D&from=1710115200&to=1752105600&countback=1973&currencyCode=INR"
    print("url : ",url)
    def sync_call():
        resp = requests.get(url, headers=HEADERS)
        return resp.json()
    return await asyncio.to_thread(sync_call)

async def save_to_db(symbol: str, data: dict):
    """API JSON से records बनाकर upsert करो."""
    if data.get("s") != "ok":
        print(f"[{symbol}] API error status: {data.get('s')}")
        return

    # JSON के parallel arrays को dicts में बदलो
    records = []
    for ts, o, h, l, c, v in zip(
        data["t"], data["o"], data["h"],
        data["l"], data["c"], data["v"]
    ):
        records.append({
            "symbol":      symbol,
            "timestamp":   ts,
            "open_price":  o,
            "high_price":  h,
            "low_price":   l,
            "close_price": c,
            "volume":      int(v),
        })

    # Bulk upsert: ON CONFLICT DO NOTHING
    stmt = pg_insert(CMStockHistorical).values(records)
    stmt = stmt.on_conflict_do_nothing(
        index_elements=["symbol", "timestamp"]
    )

    async with AsyncSessionLocal() as session:
        try:
            await session.execute(stmt)
            await session.commit()
            print(f"[{symbol}] saved {len(records)} rows")
        except SQLAlchemyError as e:
            await session.rollback()
            print(f"[{symbol}] DB error:", e)

# ——————————————————————————————————————————————————————————————
async def main():
    # 1) Symbols निकालो
    symbols = await fetch_all_symbols()

    # # 2) एक-एक करके fetch & save करो
    for sym in symbols:
        print("→ Processing", sym)
        try:
            data = await fetch_data_for(sym)
            await save_to_db(sym, data)
        except Exception as e:
            print(f"[{sym}] failed:", e)

        await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
