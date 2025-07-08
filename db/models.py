from sqlalchemy import Column, Integer, String, JSON, DateTime,func, ARRAY, Text, ForeignKey,Float, Date, Boolean
from db.connection import Base
from datetime import datetime
from sqlalchemy.orm import relationship, declarative_base
import uuid
import pytz

from sqlalchemy import Column, Integer, BigInteger, SmallInteger, String, Index

Base = declarative_base()

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

