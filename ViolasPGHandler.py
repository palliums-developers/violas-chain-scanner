from ViolasModules import ViolasAddressInfo, ViolasTransaction

from sqlalchemy import create_engine, or_
from sqlalchemy.orm import sessionmaker

class ViolasPGHandler():
    def __init__(self, dbUrl):
        self.engine = create_engine(dbUrl)
        self.session = sessionmaker(bind = self.engine)

        return

    def GetAddressInfo(self, address):
        s = self.session()
        result = s.query(ViolasAddressInfo).filter(ViolasAddressInfo.address == address).first()

        info = {}
        info["address"] = result.address
        info["balance"] = int(result.balance)
        info["sequence_number"] = result.sequence_number
        info["address_type"] = result.address_type

        s.close()
        return info

    def GetTransactionsByAddress(self, address, limit, offset):
        s = self.session()
        query = s.query(ViolasTransaction).filter(or_(ViolasTransaction.sender == address, ViolasTransaction.receiver == address)).order_by(ViolasTransaction.id.desc()).offset(offset).limit(limit).all()

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
        result = s.query(ViolasTransaction).filter(ViolasTransaction.id == (version + 1)).first()

        info = {}
        info["version"] = result.id - 1
        info["sequence_number"] = result.sequence_number
        info["sender"] = result.sender
        info["receiver"] = result.receiver
        info["module"] = result.module
        info["amount"] = int(result.amount)
        info["gas_unit_price"] = int(result.gas_unit_price)
        info["max_gas_amount"] = int(result.max_gas_amount)
        info["expiration_time"] = result.expiration_time
        info["public_key"] = result.public_key
        info["signature"] = result.signature
        info["status"] = result.status
        info["data"] = result.data

        s.close()
        return info

    def GetRecentTransaction(self, limit, offset):
        s = self.session()
        query = s.query(ViolasTransaction).order_by(ViolasTransaction.id.desc()).offset(offset).limit(limit).all()

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

        tran = ViolasTransaction(
            sender = data["sender"],
            sequence_number = data["sequence_number"],
            max_gas_amount = data["max_gas_amount"],
            gas_unit_price = data["gas_unit_price"],
            expiration_time = data["expiration_time"],
            transaction_type = data["transaction_type"],
            receiver = data["receiver"],
            amount = data["amount"],
            module = data["module"],
            module_address = data["module_address"],
            data = data["data"],
            etype = data["etype"],
            price = data["price"],
            public_key = data["public_key"],
            signature = data["signature"],
            transaction_hash = data["transaction_hash"],
            state_root_hash = data["state_root_hash"],
            event_root_hash = data["event_root_hash"],
            gas_used = data["gas_used"],
            status = data["status"]
        )

        s.add(tran)
        s.commit()
        s.close()
        return

    def InsertTransactions(self, data):
        s = self.session()

        transactions = []
        for i in data:
            tran = ViolasTransaction(
                sender = i["sender"],
                sequence_number = i["sequence_number"],
                max_gas_amount = i["max_gas_amount"],
                gas_unit_price = i["gas_unit_price"],
                expiration_time = i["expiration_time"],
                transaction_type = i["transaction_type"],
                receiver = i["receiver"],
                amount = i["amount"],
                module = i["module"],
                module_address = i["module_address"],
                data = i["data"],
                etype = i["etype"],
                price = i["price"],
                public_key = i["public_key"],
                signature = i["signature"],
                transaction_hash = i["transaction_hash"],
                state_root_hash = i["state_root_hash"],
                event_root_hash = i["event_root_hash"],
                gas_used = i["gas_used"],
                status = i["status"]
            )

            transactions.append(tran)

        s.add_all(transactions)
        s.commit()
        s.close()
        return

    def HandleAddressInfo(self, data):
        s = self.session()

        result = s.query(ViolasAddressInfo).filter(ViolasAddressInfo.address == data["address"]).first()
        if result is None:
            if data["sequence_number"] is None:
                data["sequence_number"] = 0

            info = ViolasAddressInfo(
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
        result = s.query(ViolasTransaction).count()
        s.close()

        return result
