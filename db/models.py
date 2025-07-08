from sqlalchemy import Column, Integer, BigInteger, SmallInteger, String, Index
from db.connection import Base

class StockHistorical(Base):
    __tablename__ = "stock_historical"

    id = Column(Integer, primary_key=True, index=True)
    Sgmt = Column(String, nullable=True)  
    Src = Column(String, nullable=True) 
    SctySrs = Column(String, nullable=True)  
    OpnPric = Column(String, nullable=True)  
    HghPric = Column(String, nullable=True)  
    LwPric = Column(String, nullable=True) 
    ClsPric = Column(String, nullable=True) 
    LastPric = Column(String, nullable=True)  
    TtlTradgVol = Column(String, nullable=True) 
    TradDt = Column(String, nullable=True)
    TckrSymb = Column(String, nullable=True)

class CMSnapshot(Base):
    __tablename__ = 'cm_snapshot'
    __table_args__ = (
        Index('idx_cm_snapshot_transcode_timestamp', 'transcode', 'timestamp'),
        Index('idx_cm_snapshot_security_token', 'security_token'),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    transcode = Column(SmallInteger, nullable=False)
    timestamp = Column(BigInteger, nullable=False)
    message_length = Column(SmallInteger, nullable=False)
    security_token = Column(BigInteger, nullable=True)
    last_traded_price = Column(BigInteger, nullable=True)
    best_buy_quantity = Column(BigInteger, nullable=True)
    best_buy_price = Column(BigInteger, nullable=True)
    best_sell_quantity = Column(BigInteger, nullable=True)
    best_sell_price = Column(BigInteger, nullable=True)
    total_traded_quantity = Column(BigInteger, nullable=True)
    average_traded_price = Column(BigInteger, nullable=True)
    open_price = Column(BigInteger, nullable=True)
    high_price = Column(BigInteger, nullable=True)
    low_price = Column(BigInteger, nullable=True)
    close_price = Column(BigInteger, nullable=True)
    interval_open_price = Column(BigInteger, nullable=True)
    interval_high_price = Column(BigInteger, nullable=True)
    interval_low_price = Column(BigInteger, nullable=True)
    interval_close_price = Column(BigInteger, nullable=True)
    interval_total_traded_quantity = Column(BigInteger, nullable=True)
    indicative_close_price = Column(BigInteger, nullable=True)

# Add Index model for .ind.gz files
class CMIndexSnapshot(Base):
    __tablename__ = 'cm_index_snapshot'
    __table_args__ = (
        Index('idx_cm_index_snapshot_timestamp', 'timestamp'),
        Index('idx_cm_index_snapshot_index_token', 'index_token'),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    transcode = Column(SmallInteger, nullable=False)
    timestamp = Column(BigInteger, nullable=False)
    message_length = Column(SmallInteger, nullable=False)
    index_token = Column(BigInteger, nullable=False)
    open_index_value = Column(BigInteger, nullable=True)
    current_index_value = Column(BigInteger, nullable=True)
    high_index_value = Column(BigInteger, nullable=True)
    low_index_value = Column(BigInteger, nullable=True)
    percentage_change = Column(BigInteger, nullable=True)
    interval_high_index_value = Column(BigInteger, nullable=True)
    interval_low_index_value = Column(BigInteger, nullable=True)
    interval_open_index_value = Column(BigInteger, nullable=True)
    interval_close_index_value = Column(BigInteger, nullable=True)
    indicative_close_value = Column(BigInteger, nullable=True)

# Add Call Auction model for .ca2.gz files
class CMCallAuctionSnapshot(Base):
    __tablename__ = 'cm_call_auction_snapshot'
    __table_args__ = (
        Index('idx_cm_call_auction_timestamp', 'timestamp'),
        Index('idx_cm_call_auction_security_token', 'security_token'),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    transcode = Column(SmallInteger, nullable=False)
    timestamp = Column(BigInteger, nullable=False)
    message_length = Column(SmallInteger, nullable=False)
    security_token = Column(BigInteger, nullable=True)
    last_traded_price = Column(BigInteger, nullable=True)
    best_buy_quantity = Column(BigInteger, nullable=True)
    best_buy_price = Column(BigInteger, nullable=True)
    buy_bbmm_flag = Column(String(1), nullable=True)
    best_sell_quantity = Column(BigInteger, nullable=True)
    best_sell_price = Column(BigInteger, nullable=True)
    sell_bbmm_flag = Column(String(1), nullable=True)
    total_traded_quantity = Column(BigInteger, nullable=True)
    indicative_traded_quantity = Column(BigInteger, nullable=True)
    average_traded_price = Column(BigInteger, nullable=True)
    first_open_price = Column(BigInteger, nullable=True)
    open_price = Column(BigInteger, nullable=True)
    high_price = Column(BigInteger, nullable=True)
    low_price = Column(BigInteger, nullable=True)
    close_price = Column(BigInteger, nullable=True)

class CMContractStreamInfo(Base):
    __tablename__ = 'cm_contract_stream_info'

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(BigInteger, nullable=False)
    record_count = Column(Integer, nullable=False)
    segment = Column(String(4), nullable=False)
    symbol_token = Column(BigInteger, nullable=False, index=True)
    instrument_type = Column(String(16), nullable=False)
    symbol = Column(String(32), nullable=False)
    zero1 = Column(Integer, nullable=True)
    zero2 = Column(Integer, nullable=True)
    series = Column(String(8), nullable=False)