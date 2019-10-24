from DBHandler import DBHandler
from libra import Client
from time import sleep

dbh = DBHandler();
cli = Client("testnet");

while True:
    nextID = dbh.GetNextID();
    # print(nextID);
    limit = 1;

    txInfos = cli.get_transactions(nextID, limit);
    if len(txInfos) == 0:
        sleep(1 / 1000 * 50);
        continue;

    for txInfo in txInfos:
        # print(txInfo);

        data = {};
        data["_id"] = nextID;
        seq = txInfo.sequence_number;
        data["seq"] = seq;
        sender = "".join(["{:02x}".format(i) for i in txInfo.sender]);
        data["sender"] = sender;
        addr = "".join(["{:02x}".format(i) for i in txInfo.payload.value.args[0].value]);
        data["address"] = addr;
        val = txInfo.payload.value.args[1].value;
        data["value"] = val;
        maxGas = txInfo.max_gas_amount;
        data["gas_max"] = maxGas;
        gas = txInfo.gas_unit_price;
        data["gas_price"] = gas;
        expTime = txInfo.expiration_time;
        data["time"] = expTime;
        pubKey = "".join(["{:02x}".format(i) for i in txInfo.public_key]);
        data["public_key"] = pubKey;
        sig = "".join(["{:02x}".format(i) for i in txInfo.signature]);
        data["signature"] = sig;

        # print(data);

        dbh.InsertTransactionInfo(data);

        dbh.ProcessFromAddresses(data["sender"], data["_id"]);
        dbh.ProcessToAddresses(data["address"], data["_id"]);
