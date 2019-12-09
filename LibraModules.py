from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, SmallInteger, Integer, BigInteger, String, Numeric

Base = declarative_base()

class LibraTransaction(Base):
    __tablename__ = "transactions"

    id = Column(BigInteger, primary_key = True, autoincrement = True)
    transaction_type = Column(SmallInteger, nullable = False)
    sequence_number = Column(Integer, nullable = False)
    sender = Column(String(64), nullable = False)
    receiver = Column(String(64), nullable = False)
    amount = Column(Numeric, nullable = False)
    gas_fee = Column(Numeric, nullable = False)
    gas_max = Column(Numeric, nullable = False)
    expiration_time = Column(Integer, nullable = False)
    public_key = Column(String(64), nullable = False)
    signature = Column(String(128), nullable = False)
    transaction_status = Column(SmallInteger, nullable = False)

class LibraAddressInfo(Base):
    __tablename__ = "address_info"

    id = Column(Integer, primary_key = True, autoincrement = True)
    address = Column(String(64), nullable = False)
    address_type = Column(SmallInteger, nullable = False)
    balance = Column(Numeric, nullable = False)
    sequence_number = Column(Integer, nullable = False)
