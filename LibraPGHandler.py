import logging
from time import sleep

from LibraModules import LibraAddressInfo, LibraTransaction

from sqlalchemy import create_engine, or_
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError

class LibraPGHandler():
    def __init__(self, dbUrl):
        self.engine = create_engine(dbUrl)
        self.session = sessionmaker(bind = self.engine)

        return

    def Commit(self, session):
        for i in range(5):
            try:
                session.commit()
            except OperationalError:
                session.close()
                logging.debug(f"ERROR: Commit failed! Retry after {i} second.")
                sleep(i)
                session = self.session()
                continue

        session.close()

    def Query(self, session, table):
        for i in range(5):
            try:
                return session, session.query(table)
            except OperationalError:
                session.close()
                logging.debug(f"ERROR: Query failed! Retry after {i} second.")
                sleep(i)
                session = self.session()
                continue

        return session, False

    def InsertTransaction(self, data):
        s = self.session()

        tran = LibraTransaction(
            version = i.get("version"),
            sequence_number=data.get("sequence_number"),
            sender=data.get("sender"),
            receiver=data.get("receiver"),
            transaction_type=data.get("transaction_type"),
            status=data.get("status")
        )

        s.add(tran)
        self.Commit(s)

        return

    def InsertTransactions(self, data):
        s = self.session()

        transactions = []
        for i in data:
            tran = LibraTransaction(
                version = i.get("version"),
                sequence_number = i.get("sequence_number"),
                sender = i.get("sender"),
                receiver = i.get("receiver"),
                currency = i.get("currency"),
                transaction_type = i.get("transaction_type"),
                status = i.get("status")
            )

            transactions.append(tran)

        s.add_all(transactions)
        self.Commit(s)

        logging.debug(f"DEBUG: Insert transactions over.")
        return

    def HandleSenderAddressInfo(self, data):
        s = self.session()

        if data["transaction_type"] == "MINT" or data["transaction_type"] == "MINT_LBR":
            sent_minted_tx_count = 1
        else:
            sent_minted_tx_count = 0

        if data["status"] != 4001:
            sent_failed_tx_count = 1
            sent_amount = 0
        else:
            sent_failed_tx_count = 0
            sent_amount = data["amount"]

        s, query = self.Query(s, LibraAddressInfo)
        if query:
            result = query.filter(LibraAddressInfo.address == data["sender"]).first()
        else:
            logging.critical(f"CRITICAL: Lost connection to the database!")
            exit()

        if result is None:
            info = LibraAddressInfo(
                address = data["sender"],
                first_seen = data["version"],
                sent_amount = sent_amount,
                sent_tx_count = 1,
                sent_minted_tx_count = sent_minted_tx_count,
                sent_failed_tx_count = sent_failed_tx_count,
                received_amount = 0,
                received_tx_count = 0,
                received_minted_tx_count = 0,
                received_failed_tx_count = 0
            )

            s.add(info)
        else:
            result.sent_amount += sent_amount
            result.sent_tx_count += 1
            result.sent_minted_tx_count += sent_minted_tx_count
            result.sent_failed_tx_count += sent_failed_tx_count

        self.Commit(s)

        return

    def HandleReceiverAddressInfo(self, data):
        s = self.session()

        if data["transaction_type"] == "MINT" or data["transaction_type"] == "MINT_LBR":
            received_minted_tx_count = 1
        else:
            received_minted_tx_count = 0

        if data["status"] != 4001:
            received_failed_tx_count = 1
            received_amount = 0
        else:
            received_failed_tx_count = 0
            received_amount = data["amount"]

        s, query = self.Query(s, LibraAddressInfo)
        if query:
            result = query.filter(LibraAddressInfo.address == data["receiver"]).first()
        else:
            logging.critical(f"CRITICAL: Lost connection to the database!")
            exit()

        if result is None:
            info = LibraAddressInfo(
                address = data["receiver"],
                first_seen = data["version"],
                received_amount = received_amount,
                received_tx_count = 1,
                received_minted_tx_count = received_minted_tx_count,
                received_failed_tx_count = received_failed_tx_count,
                sent_amount = 0,
                sent_tx_count = 0,
                sent_minted_tx_count = 0,
                sent_failed_tx_count = 0
            )

            s.add(info)
        else:
            result.received_amount += received_amount
            result.received_tx_count += 1
            result.received_minted_tx_count += received_minted_tx_count
            result.received_failed_tx_count += received_failed_tx_count

        self.Commit(s)

        return

    def GetLastTransactionVersion(self):
        s = self.session()

        try:
            result = s.query(LibraTransaction.version).order_by(LibraTransaction.version.desc()).first()

            if result is None:
                return True, 0
            else:
                return True, result[0]
        except OperationalError:
            return False, None
        finally:
            s.close()
