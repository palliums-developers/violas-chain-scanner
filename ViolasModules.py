from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, SmallInteger, Integer, BigInteger, String, Numeric, Index, Text

Base = declarative_base()

class ViolasTransaction(Base):
    __tablename__ = "transactions"

    id = Column(BigInteger, primary_key = True, autoincrement = True)
    sequence_number = Column(Integer, nullable = True)
    sender = Column(String(64), nullable = True)
    receiver = Column(String(64), nullable = True)
    currency = Column(String(16), nullable = True)
    gas_currency = Column(String(16), nullable = True)
    amount = Column(Numeric, nullable = True)
    gas_used = Column(Numeric, nullable = True)
    gas_unit_price = Column(Numeric, nullable = True)
    max_gas_amount = Column(Numeric, nullable = True)
    expiration_time = Column(Integer, nullable = True)
    transaction_type = Column(String(64), nullable = True)
    data = Column(Text(), nullable = True)
    public_key = Column(Text(), nullable = True)
    script_hash = Column(String(64), nullable = True)
    signature = Column(Text(), nullable = True)
    signature_scheme = Column(String(32), nullable = True)
    status = Column(String(32), nullable = True)
    event = Column(Text(), nullable = True)
    confirmed_time = Column(BigInteger, nullable = True)

class ViolasAddressInfo(Base):
    __tablename__ = "address_info"

    id = Column(BigInteger, primary_key = True, autoincrement = True)
    address = Column(String(64), nullable = True)
    type = Column(SmallInteger, nullable = True) # 0: Minter, 1: Faucet, 2: Normal
    first_seen = Column(BigInteger, nullable = True)
    sent_amount = Column(Numeric, nullable = True)
    received_amount = Column(Numeric, nullable = True)
    sent_tx_count = Column(BigInteger, nullable = True)
    received_tx_count = Column(BigInteger, nullable = True)
    sent_minted_tx_count = Column(BigInteger, nullable = True)
    received_minted_tx_count = Column(BigInteger, nullable = True)
    sent_failed_tx_count = Column(BigInteger, nullable = True)
    received_failed_tx_count = Column(BigInteger, nullable = True)

class ViolasIncentiveIssueRecord(Base):
    __tablename__ = "incentive_issue_record"

    id = Column(BigInteger, primary_key = True, autoincrement = True)
    address = Column(String(64), nullable = False)
    amount = Column(Numeric, nullable = False)
    date = Column(Integer, nullable = False)
    status = Column(SmallInteger, nullable = False) # 0: not issued; 1: issued; -1: issued failed
    type = Column(SmallInteger, nullable = False) # 0: Register; 1: invite; 2: be invited; 3: deposit; 4: withdraw; 5: borrow; 6: repayment; 7: bank extract; 8: pool add; 9: pool remove; 10: pool extract
