import logging
import configparser

from libra import Client
from time import sleep

from LibraPGHandler import LibraPGHandler

logging.basicConfig(filename = "libra_explorer_core.log", level = logging.DEBUG)

config = configparser.ConfigParser()
config.read("./config.ini")

VIOLAS_HOST = "52.27.228.84"
VIOLAS_PORT = 40001

libraClient = Client("testnet");
violasClient = Client.new(VIOLAS_HOST, VIOLAS_PORT, "../../documents/consensus_peers.config.toml")

libraDBInfo = config["LIBRA DB INFO"]
libraDBUrl = f"{libraDBInfo['DBTYPE']}+{libraDBInfo['DRIVER']}://{libraDBInfo['USERNAME']}:{libraDBInfo['PASSWORD']}@{libraDBInfo['HOSTNAME']}:{libraDBInfo['PORT']}/{libraDBInfo['DATABASE']}"
HLibra = LibraPGHandler(libraDBUrl)

violasDBInfo = config["VIOLAS DB INFO"]
violasDBUrl = f"{violasDBInfo['DBTYPE']}+{violasDBInfo['DRIVER']}://{violasDBInfo['USERNAME']}:{violasDBInfo['PASSWORD']}@{violasDBInfo['HOSTNAME']}:{violasDBInfo['PORT']}/{violasDBInfo['DATABASE']}"

while True:
    nextID = HLibra.GetTransactionCount()
    logging.debug("Get next id is %d", nextID)
    limit = 1000

    try:
        txInfos = libraClient.get_transactions(nextID, limit, True)
    except:
        logging.error("Get transaction error: %s", e.msg)
        continue

    if len(txInfos) == 0:
        sleep(1 / 1000 * 500)
        continue

    datas = []
    for txInfo in txInfos:
        logging.debug("Get transaction info: %s", txInfo.to_json())
        data = {}
        data["version"] = txInfo.version
        data["sequence_number"] = txInfo.raw_txn.sequence_number
        data["expiration_time"] = txInfo.raw_txn.expiration_time
        if txInfo.raw_txn.type.type == "write_set":
            data["transaction_type"] = 0
            data["sender"] = "0"
            data["receiver"] = "0"
            data["amount"] = 0
            data["expiration_time"] = 0
        elif txInfo.raw_txn.type.type == "rotate_authentication_key":
            data["transaction_type"] = 0
            data["sender"] = txInfo.raw_txn.type.sender
            data["receiver"] = "0"
            data["amount"] = 0
        else:
            if txInfo.raw_txn.type.type == "mint":
                data["transaction_type"] = 1
            elif txInfo.raw_txn.type.type == "peer_to_peer_transfer":
                data["transaction_type"] = 2

            data["sender"] = txInfo.raw_txn.type.sender
            data["receiver"] = txInfo.raw_txn.type.receiver
            data["amount"] = txInfo.raw_txn.type.amount

        data["gas_max"] = txInfo.raw_txn.max_gas_amount
        data["gas_fee"] = txInfo.raw_txn.gas_unit_price
        data["public_key"] = txInfo.public_key
        data["signature"] = txInfo.signature
        data["transaction_status"] = txInfo.info.major_status

        logging.debug("Final result: %s", data)

        datas.append(data)

        senderInfo = {}
        senderInfo["address"] = data["sender"]
        senderInfo["balance"] = data["amount"] * -1
        senderInfo["sequence_number"] = data["sequence_number"]
        HLibra.HandleAddressInfo(senderInfo)

        receiverInfo = {}
        receiverInfo["address"] = data["receiver"]
        receiverInfo["balance"] = data["amount"]
        receiverInfo["sequence_number"] = None
        HLibra.HandleAddressInfo(receiverInfo)

    HLibra.InsertTransactions(datas)
