from pydantic import BaseModel, Field, validator, EmailStr
from typing import List, Optional, Literal
from datetime import datetime, date
from pydantic import ConfigDict


from random import randint



class CMSnapshot(BaseModel):
    id: int
    transcode: int
    timestamp: int
    message_length: int
    security_token: Optional[int]
    last_traded_price: Optional[int]
    best_buy_quantity: Optional[int]
    best_buy_price: Optional[int]
    best_sell_quantity: Optional[int]
    best_sell_price: Optional[int]
    total_traded_quantity: Optional[int]
    average_traded_price: Optional[int]
    open_price: Optional[int]
    high_price: Optional[int]
    low_price: Optional[int]
    close_price: Optional[int]
    interval_open_price: Optional[int]
    interval_high_price: Optional[int]
    interval_low_price: Optional[int]
    interval_close_price: Optional[int]
    interval_total_traded_quantity: Optional[int]
    indicative_close_price: Optional[int]

    class Config:
        orm_mode = True


class CMContractStreamInfo(BaseModel):
    id: int
    timestamp: int
    record_count: int
    segment: str
    symbol_token: int
    instrument_type: str
    symbol: str
    zero1: Optional[int]
    zero2: Optional[int]
    series: str

    class Config:
        orm_mode = True


# Response wrappers
class SnapshotListResponse(BaseModel):
    snapshots: List[CMSnapshot]


class ContractStreamInfoListResponse(BaseModel):
    contracts: List[CMContractStreamInfo]
