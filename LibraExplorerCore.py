import logging
import configparser

from violas import Client
from time import sleep

from LibraPGHandler import LibraPGHandler

logging.basicConfig(filename = "LibraLog.out", level = logging.DEBUG)

config = configparser.ConfigParser()
config.read("./config.ini")

libraDBInfo = config["LIBRA DB INFO"]
libraDBUrl = f"{libraDBInfo['DBTYPE']}+{libraDBInfo['DRIVER']}://{libraDBInfo['USERNAME']}:{libraDBInfo['PASSWORD']}@{libraDBInfo['HOSTNAME']}:{libraDBInfo['PORT']}/{libraDBInfo['DATABASE']}"
HLibra = LibraPGHandler(libraDBUrl)

while True:
    nextID = HLibra.GetTransactionCount()
    logging.debug(f"Get next id is: {nextID}")
    limit = 1000

    cli = Client("libra_testnet")

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
        logging.debug(f"Get transaction info: {txInfo}")

        data = {}
        data["version"] = txInfo.version

        try:
            if hasattr(txInfo, "proposer"):
                data["sender"] = txInfo.proposer
                data["transaction_type"] = "metadata"
                data["expiration_time"] = txInfo.timestamp_usec / 1000000
                data["amount"] = 0
                data["max_gas_amount"] = 0
                data["gas_unit_price"] = 0
                data["sequence_number"] = 0
                data["address_type"] = 1
            else:
                data["sender"] = txInfo.raw_txn.sender
                data["sequence_number"] = txInfo.raw_txn.sequence_number
                data["max_gas_amount"] = txInfo.raw_txn.max_gas_amount
                data["gas_unit_price"] = txInfo.raw_txn.gas_unit_price
                if txInfo.raw_txn.expiration_time > 2147483647:
                    data["expiration_time"] = 0
                else:
                    data["expiration_time"] = txInfo.raw_txn.expiration_time
                data["address_type"] = 1
                data["transaction_type"] = txInfo.raw_txn.type.type

                if txInfo.raw_txn.type.type == "write_set":
                    data["address_type"] = 0
                    data["expiration_time"] = 0

                if hasattr(txInfo.raw_txn.type, "receiver"):
                    data["receiver"] = txInfo.raw_txn.type.receiver

                if hasattr(txInfo.raw_txn.type, "amount"):
                    data["amount"] = txInfo.raw_txn.type.amount
                else:
                    data["amount"] = 0

                data["public_key"] = txInfo.public_key
                data["signature"] = txInfo.signature

            data["transaction_hash"] = txInfo.info.transaction_hash
            data["state_root_hash"] = txInfo.info.state_root_hash
            data["event_root_hash"] = txInfo.info.event_root_hash
            data["gas_used"] = txInfo.info.gas_used
            data["status"] = txInfo.info.major_status

        except Exception as e:
            logging.error(f"Parse txInfo failed: {txInfo}")
            exit(1)

        datas.append(data)

        HLibra.HandleSenderAddressInfo(data)

        if "receiver" in data:
            HLibra.HandleReceiverAddressInfo(data)

    HLibra.InsertTransactions(datas)
