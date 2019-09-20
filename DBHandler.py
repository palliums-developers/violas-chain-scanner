from pymongo import MongoClient

DB_URL = "mongodb://localhost:27017";

class DBHandler:
    def __init__(self, url = DB_URL):
        self.client = MongoClient(url);

    def GetNextID(self):
        dbo = self.client.libra_local.transactions;
        lastTx = dbo.find().sort({"_id":-1}).limit(1);
        length = lastTx.count();

        if length == 0:
            return 1;
        else:
            return lastTx[0]["_id"];

    def InsertTransactionInfo(self, txInfo):
        dbo = self.client.libra_local.transactions;
        result = dbo.insert_one(txInfo);

    def ProcessToAddresses(self, address, version):
        dbo = self.client.libra_local.addresses;
        addressInfo = dbo.find_one({"_id": address});

        if addressInfo != None:
            result = dbo.update_one({"_id": address}, {"$addToSet": {"rececived": version}});
        else:
            result = dbo.insert_one({"_id": address});

    def ProcessFromAddresses(self, address, version):
        dbo = self.client.libra_local.addresses;
        addressInfo = dbo.find_one({"_id": address});

        if addressInfo != None:
            result = dbo.update_one({"_id": address}, {"$addToSet": {"sent": version}});
        else:
            result = dbo.insert_one({"_id": address});
