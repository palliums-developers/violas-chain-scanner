import logging
from time import sleep

from ViolasModules import ViolasAddressInfo, ViolasTransaction

from sqlalchemy import create_engine, or_
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError

class ViolasPGHandler():
    def __init__(self, dbUrl):
        self.engine = create_engine(dbUrl)
        self.session = sessionmaker(bind = self.engine)

        return

    def Commit(self, session):
        for i in range(5):
            try:
                session.commit()
                session.close()
                return True
            except OperationalError:
                session.close()
                logging.debug(f"ERROR: Commit failed! Retry after {i} second.")
                sleep(i)
                session = self.session()

                continue

        session.close()
        return False

    def Query(self, session, table):
        for i in range(5):
            try:
                return session, session.query(table)
            except OperationalError:
                session.close()
                logging.debug(f"ERROR: Query Failed! Retry after {i} second.")
                sleep(i)
                session = self.session()

                continue

        return session, False

    def InsertTransaction(self, data):
        tran = ViolasTransaction(
            sender = data.get("sender"),
            sequence_number = data.get("sequence_number"),
            max_gas_amount = data.get("max_gas_amount"),
            gas_unit_price = data.get("gas_unit_price"),
            expiration_time = data.get("expiration_time"),
            transaction_type = data.get("transaction_type"),
            receiver = data.get("receiver"),
            amount = data.get("amount"),
            module = data.get("module"),
            data = data.get("data"),
            public_key = data.get("public_key"),
            signature = data.get("signature"),
            transaction_hash = data.get("transaction_hash"),
            state_root_hash = data.get("state_root_hash"),
            event_root_hash = data.get("event_root_hash"),
            gas_used = data.get("gas_used"),
            status = data.get("status")
        )
        s = self.session()

        s.add(tran)
        self.Commit(s)

        return

    def InsertTransactions(self, data):
        s = self.session()

        transactions = []
        for i in data:
            tran = ViolasTransaction(
                sequence_number = i.get("sequence_number"),
                sender = i.get("sender"),
                receiver = i.get("receiver"),
                currency = i.get("currency"),
                gas_currency = i.get("gas_currency"),
                amount = i.get("amount"),
                gas_used = i.get("gas_used"),
                gas_unit_price = i.get("gas_unit_price"),
                max_gas_amount = i.get("max_gas_amount"),
                expiration_time = i.get("expiration_time"),
                transaction_type = i.get("transaction_type"),
                data = i.get("data"),
                public_key = i.get("public_key"),
                script_hash = i.get("script_hash"),
                signature = i.get("signature"),
                signature_scheme = i.get("signature_scheme"),
                status = i.get("status")
            )

            transactions.append(tran)

        s.add_all(transactions)
        self.Commit(s)

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

        s, query = self.Query(s, ViolasAddressInfo)
        if query:
            result = query.filter(ViolasAddressInfo.address == data["sender"]).first()
        else:
            logging.critical(f"CRITICAL: Lost connection to the database!")
            exit()

        if result is None:
            info = ViolasAddressInfo(
                address = data["sender"],
                type = data["address_type"],
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

        s, query = self.Query(s, ViolasAddressInfo)
        if query:
            result = query.filter(ViolasAddressInfo.address == data["receiver"]).first()
        else:
            logging.critical(f"CRITICAL: Lost connection to the database!")
            exit()

        if result is None:
            info = ViolasAddressInfo(
                address = data["receiver"],
                type = data["address_type"],
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

    def GetTransactionCount(self):
        s = self.session()

        try:
            result = s.query(ViolasTransaction.id).order_by(ViolasTransaction.id.desc()).limit(1).first()
            s.close()

            if result is None:
                result = 0
            else:
                result = result[0]

            return True, result
        except OperationalError:
            s.close()
            return False, None
