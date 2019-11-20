from LibraModules import LibraAddressInfo, LibraTransaction

from sqlalchemy import create_engine
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

        s.close()
        return info

    def GetTransactionsByAddress(self, address, limit, offset):
        s = self.session()
        query = s.query(LibraTransaction).filter(LibraTransaction.sender == address).order_by(LibraTransaction.id.desc()).offset(offset).limit(limit).all()

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
        info["gas_fee"] = int(result.gas_fee)
        info["gas_max"] = int(result.gas_max)
        info["expiration_time"] = result.expiration_time
        info["public_key"] = result.public_key
        info["signature"] = result.signature
        info["transaction_status"] = result.transaction_status

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
            transaction_type = data["transaction_type"],
            sequence_number = data["sequence_number"],
            sender = data["sender"],
            receiver = data["receiver"],
            amount = data["amount"],
            gas_fee = data["gas_fee"],
            gas_max = data["gas_max"],
            expiration_time = data["expiration_time"],
            public_key = data["public_key"],
            signature = data["signature"],
            transaction_status = data["transaction_status"]
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
                transaction_type = i["transaction_type"],
                sequence_number = i["sequence_number"],
                sender = i["sender"],
                receiver = i["receiver"],
                amount = i["amount"],
                gas_fee = i["gas_fee"],
                gas_max = i["gas_max"],
                expiration_time = i["expiration_time"],
                public_key = i["public_key"],
                signature = i["signature"],
                transaction_status = i["transaction_status"]
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
                sequence_number = data["sequence_number"]
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
