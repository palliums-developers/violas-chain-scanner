import logging
import configparser
from time import sleep

from violas import Client
from violas import TransactionType
from LibraPGHandler import LibraPGHandler

logging.basicConfig(filename = "LibraLog.out", level = logging.WARNING)

config = configparser.ConfigParser()
config.read("./config.ini")

libraDBInfo = config["LIBRA DB INFO"]
libraDBUrl = f"{libraDBInfo['DBTYPE']}+{libraDBInfo['DRIVER']}://{libraDBInfo['USERNAME']}:{libraDBInfo['PASSWORD']}@{libraDBInfo['HOSTNAME']}:{libraDBInfo['PORT']}/{libraDBInfo['DATABASE']}"
HLibra = LibraPGHandler(libraDBUrl)

while True:
    succ, nextID = HLibra.GetTransactionCount()
    if not succ:
        logging.error(f"ERROR: Get count of transactions failed, retry after 500ms.")
        sleep(1 / 1000 * 500)
        continue

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
        data["version"] = txInfo.get_version()

        try:
            if txInfo.get_transaction_type() == TransactionType.BLOCK_METADATA:
                data["sender"] = txInfo.get_proposer()
                data["expiration_time"] = txInfo.get_timestamp_usec() / 1000000
                data["amount"] = 0
                data["max_gas_amount"] = 0
                data["gas_unit_price"] = 0
                data["sequence_number"] = 0
                data["address_type"] = 1
            elif txInfo.get_transaction_type() == TransactionType.SIGNED_TRANSACTION:
                data["sender"] = txInfo.get_sender()
                data["sequence_number"] = txInfo.get_sequence_number()
                data["max_gas_amount"] = txInfo.get_max_gas_amount()
                data["gas_unit_price"] = txInfo.get_gas_unit_price()

                if txInfo.get_version() == 0:
                    data["expiration_time"] = 0
                    data["amount"] = 0
                    data["address_type"] = 0
                else:
                    data["expiration_time"] = txInfo.get_expiration_time()
                    data["amount"] = txInfo.get_amount() if txInfo.get_amount() is not None else 0
                    data["receiver"] = txInfo.get_receiver()
                    data["address_type"] = 2

                data["public_key"] = txInfo.get_public_key()
                data["signature"] = txInfo.get_signature()

            data["transaction_type"] = txInfo.get_code_type().name
            data["transaction_hash"] = txInfo.get_transaction_hash()
            data["state_root_hash"] = txInfo.get_state_root_hash()
            data["event_root_hash"] = txInfo.get_event_root_hash()
            data["gas_used"] = txInfo.get_gas_used()
            data["status"] = txInfo.get_major_status()

        except Exception as e:
            logging.error(f"Parse txInfo failed: {e}")
            exit(1)

        logging.debug(f"Paser result: {data}")

        HLibra.HandleSenderAddressInfo(data)

        if "receiver" in data and data["receiver"] is not None:
            HLibra.HandleReceiverAddressInfo(data)

        datas.append(data)

    HLibra.InsertTransactions(datas)
