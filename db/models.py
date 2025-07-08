from sqlalchemy import Column, Integer, BigInteger, SmallInteger, String, Index, Float
from db.connection import Base


class CMTokenMaster(Base):
    """
    Capital Market Token Master table for NSE securities information
    Updated to match your existing schema
    """
    __tablename__ = 'cm_token_master'

    # Primary key - token number
    token_number = Column(Integer, primary_key=True, comment="NSE Token Number")
    
    # Core security identification
    symbol = Column(String(100), nullable=False, index=True, comment="Security Symbol")
    series = Column(String(3), nullable=False, index=True, comment="Trading Series (EQ, BE, etc.)")
    
    # Financial data
    issued_capital = Column(Float, nullable=False, default=0.0, comment="Issued Capital")
    
    # Trading parameters
    settlement_cycle = Column(Integer, nullable=False, default=1, index=True, comment="Settlement Cycle (0=T+0, 1=T+1)")
    
    # Company information
    company_name = Column(String(100), nullable=False, default='', comment="Company Name")
    
    # Trading permissions
    permitted_to_trade = Column(Integer, nullable=False, default=1, comment="0=Not Permitted, 1=Permitted, 2=BSE Listed")
    
    # Technical fields
    data_length = Column(Integer, nullable=True, comment="Original data length from file")
    
    # Description fields (computed)
    settlement_cycle_desc = Column(String(10), nullable=True, comment="Settlement Cycle Description")
    permitted_to_trade_desc = Column(String(32), nullable=False, default='Permitted to trade', comment="Permission Description")
    
    # Tracking field
    last_updated = Column(String(20), nullable=True, comment="Last update date (YYYY-MM-DD)")
    
    # Indexes for performance
    __table_args__ = (
        Index('idx_symbol_series', 'symbol', 'series'),
        Index('idx_company_name', 'company_name'),
        Index('idx_permitted_to_trade', 'permitted_to_trade'),
        Index('idx_last_updated', 'last_updated'),
    )
    
    def __repr__(self):
        return f"<CMTokenMaster(token={self.token_number}, symbol='{self.symbol}', series='{self.series}')>"
    
    @property
    def full_symbol(self):
        """Get full symbol with series"""
        if self.series:
            return f"{self.symbol}-{self.series}"
        return self.symbol
    
    def to_dict(self):
        """Convert to dictionary for API responses"""
        return {
            'token_number': self.token_number,
            'symbol': self.symbol,
            'series': self.series,
            'full_symbol': self.full_symbol,
            'company_name': self.company_name,
            'issued_capital': self.issued_capital,
            'settlement_cycle': self.settlement_cycle,
            'settlement_cycle_desc': self.settlement_cycle_desc,
            'permitted_to_trade': self.permitted_to_trade,
            'permitted_to_trade_desc': self.permitted_to_trade_desc,
            'data_length': self.data_length,
            'last_updated': self.last_updated,
        }

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


