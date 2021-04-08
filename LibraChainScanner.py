import logging
import configparser
from time import sleep

from libra_client import Client
from LibraPGHandler import LibraPGHandler
from libra_client.lbrtypes.bytecode import CodeType

logging.basicConfig(filename = "LibraLog.out", level = logging.WARNING)

config = configparser.ConfigParser()
config.read("./config.ini")

libraDBInfo = config["LIBRA DB INFO"]
libraDBUrl = f"{libraDBInfo['DBTYPE']}+{libraDBInfo['DRIVER']}://{libraDBInfo['USERNAME']}:{libraDBInfo['PASSWORD']}@{libraDBInfo['HOSTNAME']}:{libraDBInfo['PORT']}/{libraDBInfo['DATABASE']}"
HLibra = LibraPGHandler(libraDBUrl)

cli = Client(config["NODE INFO"]["LIBRA_HOST"])

limit = 1000
succ, nextID = HLibra.GetLastTransactionVersion()
if not succ:
    logging.error(f"ERROR: Get count of transactions failed.")
    exit()

if nextID:
    nextID += 1

while True:
    logging.debug(f"Get next id is: {nextID}")

    try:
        txInfos = cli.get_transactions(nextID, limit, True)
    except Exception as e:
        logging.error(f"Get transaction failed: {e}")
        cli = Client(config["NODE INFO"]["LIBRA_HOST"])
        continue

    if len(txInfos) == 0:
        sleep(1 / 1000 * 500)
        continue
    elif len(txInfos) == limit:
        nextID += limit
    else:
        nextID += len(txInfos)

    datas = []
    for index, txInfo in enumerate(txInfos):
        data = {}

        try:
            if txInfo.transaction.enum_name == "UserTransaction":
                logging.debug(f"Get transaction info: {txInfo}")

                data["version"] = txInfo.get_version()
                data["sequence_number"] = txInfo.get_sequence_number()
                data["sender"] = txInfo.get_sender()
                data["receiver"] = txInfo.get_receiver()
                data["currency"] = txInfo.get_currency_code() if txInfo.get_currency_code() is not None else txInfo.get_gas_currency()

                data["transaction_type"] = txInfo.get_code_type().name if txInfo.get_code_type() is not None else ""
                data["status"] = txInfo.get_vm_status().enum_name
            else:
                continue

        except Exception as e:
            logging.error(f"Parse txInfo failed: {e}")
            exit(1)

        logging.debug(f"Paser result: {data}")

        if txInfo.transaction.enum_name == "UserTransaction":
            HLibra.HandleSenderAddressInfo(data)
            if data["receiver"] is not None:
                HLibra.HandleReceiverAddressInfo(data)

        datas.append(data)
    HLibra.InsertTransactions(datas)
