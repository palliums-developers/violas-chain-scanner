from LibraModules import LibraAddressInfo, LibraTransaction

from sqlalchemy import create_engine, or_
from sqlalchemy.orm import sessionmaker

class LibraPGHandler():
    def __init__(self, dbUrl):
        self.engine = create_engine(dbUrl)
        self.session = sessionmaker(bind = self.engine)

        return

    def GetAddressInfo(self, address):
        s = self.session()
        result = s.query(LibraAddressInfo).filter(LibraAddressInfo.address == address).first()

        info = {}
        info["address"] = result.address
        info["balance"] = int(result.balance)
        info["sequence_number"] = result.sequence_number
        info["address_type"] = result.address_type

        s.close()
        return info

    def GetTransactionsByAddress(self, address, limit, offset):
        s = self.session()
        query = s.query(LibraTransaction).filter(or_(LibraTransaction.sender == address, LibraTransaction.receiver == address)).order_by(LibraTransaction.id.desc()).offset(offset).limit(limit).all()

        infoList = []
        for i in query:
            info = {}
            info["version"] = i.id - 1
            info["sender"] = i.sender
            info["receiver"] = i.receiver
            info["amount"] = int(i.amount)
            info["expiration_time"] = i.expiration_time

            infoList.append(info)

        s.close()
        return infoList

    def GetTransactionByVersion(self, version):
        s = self.session()
        result = s.query(LibraTransaction).filter(LibraTransaction.id == (version + 1)).first()

        info = {}
        info["version"] = result.id - 1
        info["sequence_number"] = result.sequence_number
        info["sender"] = result.sender
        info["receiver"] = result.receiver
        info["amount"] = int(result.amount)
        info["max_gas_amount"] = int(result.max_gas_amount)
        info["gas_unit_price"] = int(result.gas_unit_price)
        info["expiration_time"] = result.expiration_time
        info["public_key"] = result.public_key
        info["signature"] = result.signature
        info["status"] = result.status

        s.close()
        return info

    def GetRecentTransaction(self, limit, offset):
        s = self.session()
        query = s.query(LibraTransaction).order_by(LibraTransaction.id.desc()).offset(offset).limit(limit).all()

        infoList = []
        for i in query:
            info = {}
            info["version"] = i.id - 1
            info["sender"] = i.sender
            info["receiver"] = i.receiver
            info["amount"] = int(i.amount)
            info["expiration_time"] = i.expiration_time

            infoList.append(info)

        s.close()
        return infoList

    def InsertTransaction(self, data):
        s = self.session()

        tran = LibraTransaction(
            sender = data.get("sender"),
            sequence_number = data.get("sequence_number"),
            max_gas_amount = data.get("max_gas_amount"),
            gas_unit_price = data.get("gas_unit_price"),
            expiration_time = data.get("expiration_time"),
            transaction_type = data.get("transaction_type"),
            receiver = data.get("receiver"),
            amount = data.get("amount"),
            public_key = data.get("public_key"),
            signature = data.get("signature"),
            transaction_hash = data.get("transaction_hash"),
            state_root_hash = data.get("state_root_hash"),
            event_root_hash = data.get("event_root_hash"),
            gas_used = data.get("gas_used"),
            status = data.get("status")
        )

        s.add(tran)
        s.commit()
        s.close()
        return

    def InsertTransactions(self, data):
        s = self.session()

        transactions = []
        for i in data:
            tran = LibraTransaction(
                sender = i.get("sender"),
                sequence_number = i.get("sequence_number"),
                max_gas_amount = i.get("max_gas_amount"),
                gas_unit_price = i.get("gas_unit_price"),
                expiration_time = i.get("expiration_time"),
                transaction_type = i.get("transaction_type"),
                receiver = i.get("receiver"),
                amount = i.get("amount"),
                public_key = i.get("public_key"),
                signature = i.get("signature"),
                transaction_hash = i.get("transaction_hash"),
                state_root_hash = i.get("state_root_hash"),
                event_root_hash = i.get("event_root_hash"),
                gas_used = i.get("gas_used"),
                status = i.get("status")
            )

            transactions.append(tran)

        s.add_all(transactions)
        s.commit()
        s.close()
        return

    def HandleAddressInfo(self, data):
        s = self.session()

        result = s.query(LibraAddressInfo).filter(LibraAddressInfo.address == data["address"]).first()
        if result is None:
            if data["sequence_number"] is None:
                data["sequence_number"] = 0

            info = LibraAddressInfo(
                address = data["address"],
                balance = data["balance"],
                sequence_number = data["sequence_number"],
                address_type = data["address_type"]
            )

            s.add(info)
        else:
            result.balance = result.balance + data["balance"]
            if data["sequence_number"] is not None:
                result.sequence_number = data["sequence_number"]

        s.commit()
        s.close()
        return

    def GetTransactionCount(self):
        s = self.session()
        result = s.query(LibraTransaction).count()
        s.close()

        return result
