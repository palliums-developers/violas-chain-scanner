from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, SmallInteger, Integer, BigInteger, String, Numeric, Index, Text

Base = declarative_base()

class LibraTransaction(Base):
    __tablename__ = "transactions"

    version = Column(BigInteger, primary_key = True)
    sender = Column(String(64), nullable = True)
    receiver = Column(String(64), nullable = True)
    sequence_number = Column(Integer, nullable = True)
    currency = Column(Text, nullable = True)
    transaction_type = Column(Text, nullable = True)
    status = Column(String(32), nullable = True)

class LibraAddressInfo(Base):
    __tablename__ = "address_info"

    id = Column(BigInteger, primary_key = True, autoincrement = True)
    address = Column(String(64), nullable = True)
    first_seen = Column(BigInteger, nullable = True)
    received_amount = Column(Numeric, nullable = True)
    sent_amount = Column(Numeric, nullable = True)
    sent_tx_count = Column(BigInteger, nullable = True)
    received_tx_count = Column(BigInteger, nullable = True)
    sent_minted_tx_count = Column(BigInteger, nullable = True)
    received_minted_tx_count = Column(BigInteger, nullable = True)
    sent_failed_tx_count = Column(BigInteger, nullable = True)
    received_failed_tx_count = Column(BigInteger, nullable = True)
