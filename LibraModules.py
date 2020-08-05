from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, SmallInteger, Integer, BigInteger, String, Numeric, Index, Text

Base = declarative_base()

class LibraTransaction(Base):
    __tablename__ = "transactions"

    id = Column(BigInteger, primary_key = True)
    sequence_number = Column(Integer, nullable = False)
    sender = Column(String(64), nullable = False)
    receiver = Column(String(64), nullable = True)
    currency = Column(String(16), nullable = True)
    gas_currency = Column(String(16), nullable = True)
    amount = Column(Numeric, nullable = False)
    gas_used = Column(Numeric, nullable = False)
    gas_unit_price = Column(Numeric, nullable = False)
    max_gas_amount = Column(Numeric, nullable = False)
    expiration_time = Column(BigInteger, nullable = False)
    transaction_type = Column(String(64), nullable = False)
    data = Column(Text(), nullable = True)
    public_key = Column(Text(), nullable = True)
    script_hash = Column(String(64), nullable = True)
    signature = Column(Text(), nullable = True)
    signature_scheme = Column(String(32), nullable = True)
    status = Column(String(32), nullable = False)
    confirmed_time = Column(BigInteger, nullable = True)


Index("address_index", LibraTransaction.sender, LibraTransaction.receiver)
Index("sender_index", LibraTransaction.sender)
Index("receiver_index", LibraTransaction.receiver)
Index("currency_index", LibraTransaction.currency)

class LibraAddressInfo(Base):
    __tablename__ = "address_info"

    id = Column(BigInteger, primary_key = True, autoincrement = True)
    address = Column(String(64), nullable = True)
    type = Column(SmallInteger, nullable = False) # 0: Minter, 1: proposer, 2: Normal
    first_seen = Column(BigInteger, nullable = False)
    received_amount = Column(Numeric, nullable = False)
    sent_amount = Column(Numeric, nullable = False)
    sent_tx_count = Column(BigInteger, nullable = False)
    received_tx_count = Column(BigInteger, nullable = False)
    sent_minted_tx_count = Column(BigInteger, nullable = False)
    received_minted_tx_count = Column(BigInteger, nullable = False)
    sent_failed_tx_count = Column(BigInteger, nullable = False)
    received_failed_tx_count = Column(BigInteger, nullable = False)

Index("address_info_index", LibraAddressInfo.address)
