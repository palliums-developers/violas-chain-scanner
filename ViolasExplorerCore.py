import logging
import configparser

from libra import Client
from time import sleep

from  ViolasPGHandler import ViolasPGHandler

logging.basicConfig(filename = "ViolasLog.out", level = logging.DEBUG)

config = configparser.ConfigParser()
config.read("./config.ini")

VIOLAS_HOST = "52.27.228.84"
VIOLAS_PORT = 40001

violasDBInfo = config["VIOLAS DB INFO"]
violasDBUrl = f"{violasDBInfo['DBTYPE']}+{violasDBInfo['DRIVER']}://{violasDBInfo['USERNAME']}:{violasDBInfo['PASSWORD']}@{violasDBInfo['HOSTNAME']}:{violasDBInfo['PORT']}/{violasDBInfo['DATABASE']}"
HViolas = ViolasPGHandler(violasDBUrl)

while True:
    nextID = HViolas.GetTransactionCount()
    logging.debug("Get next id is %d", nextID)
    limit = 10

    cli = Client.new(VIOLAS_HOST, VIOLAS_PORT, "/tmp/consensus_peers.config.toml")

    try:
        txInfos = cli.get_transactions(nextID, limit, True)
    except Exception as e:
        logging.error(f"Get transaction failed: {e}")
        continue

    if len(txInfos) == 0:
        sleep(1 / 1000 * 500)
        continue

    datas = []
    for txInfo in txInfos:
        logging.debug("Get transaction info: %s", txInfo.to_json())

        data = {}
        data["version"] = txInfo.version
        data["sender"] = txInfo.raw_txn.sender
        data["sequence_number"] = txInfo.raw_txn.sequence_number
        data["max_gas_amount"] = txInfo.raw_txn.max_gas_amount
        data["gas_unit_price"] = txInfo.raw_txn.gas_unit_price
        data["expiration_time"] = txInfo.raw_txn.expiration_time
        data["address_type"] = 1
        data["transaction_type"] = txInfo.raw_txn.type.type

        txType = txInfo.raw_txn.type.type
        logging.debug(f"DEBUG:::Transaction Type: {txType}")

        if txInfo.raw_txn.type.type == "write_set":
            data["address_type"] = 0
            data["expiration_time"] = 0

        if hasattr(txInfo.raw_txn.type, "receiver"):
            data["receiver"] = txInfo.raw_txn.type.receiver

        if hasattr(txInfo.raw_txn.type, "amount"):
            data["amount"] = txInfo.raw_txn.type.amount
        else:
            data["amount"] = 0

        if len(txInfo.events) > 0:
            if hasattr(txInfo.events[0].tag, "module"):
                data["module"] = txInfo.events[0].tag.module

            if hasattr(txInfo.events[0].tag, "address"):
                data["module_address"] = txInfo.events[0].tag.address

            if hasattr(txInfo.events[0].event, "data"):
                data["data"] = txInfo.events[0].event.data

            if hasattr(txInfo.events[0].event, "etype"):
                data["etype"] = txInfo.events[0].event.etype

            if hasattr(txInfo.events[0].event, "price"):
                data["price"] = txInfo.events[0].event.price

        data["public_key"] = txInfo.public_key
        data["signature"] = txInfo.signature
        data["transaction_hash"] = txInfo.info.transaction_hash
        data["state_root_hash"] = txInfo.info.state_root_hash
        data["event_root_hash"] = txInfo.info.event_root_hash
        data["gas_used"] = txInfo.info.gas_used
        data["status"] = txInfo.info.major_status

        logging.debug("Final result: %s", data)

        datas.append(data)

        senderInfo = {}
        senderInfo["address"] = data["sender"]
        senderInfo["balance"] = data["amount"] * -1
        senderInfo["sequence_number"] = data["sequence_number"]
        senderInfo["address_type"] = data["address_type"]
        HViolas.HandleAddressInfo(senderInfo)

        if hasattr(data, "receiver"):
            receiverInfo = {}
            receiverInfo["address"] = data["receiver"]
            receiverInfo["balance"] = data["amount"]
            receiverInfo["sequence_number"] = None
            receiverInfo["address_type"] = data["address_type"]
            HViolas.HandleAddressInfo(receiverInfo)

    HViolas.InsertTransactions(datas)
