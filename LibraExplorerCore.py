import logging
import configparser
from time import sleep

from libra_client import Client
from LibraPGHandler import LibraPGHandler

logging.basicConfig(filename = "LibraLog.out", level = logging.WARNING)

config = configparser.ConfigParser()
config.read("./config.ini")

libraDBInfo = config["LIBRA DB INFO"]
libraDBUrl = f"{libraDBInfo['DBTYPE']}+{libraDBInfo['DRIVER']}://{libraDBInfo['USERNAME']}:{libraDBInfo['PASSWORD']}@{libraDBInfo['HOSTNAME']}:{libraDBInfo['PORT']}/{libraDBInfo['DATABASE']}"
HLibra = LibraPGHandler(libraDBUrl)
cli = Client("libra_testnet")

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
        cli = Client("libra_testnet")
        continue

    if len(txInfos) == 0:
        sleep(1 / 1000 * 500)
        continue

    datas = []
    for txInfo in txInfos:
        logging.debug(f"Get transaction info: {txInfo}")
        transactionType = txInfo.transaction.enum_name
        logging.debug(f"Transaction type is {transactionType}")

        data = {}
        data["version"] = txInfo.get_version()

        try:
            if transactionType == "UserTransaction":
                data["sender"] = txInfo.get_sender()
                data["receiver"] = txInfo.get_receiver()
                data["sequence_number"] = txInfo.transaction.value.get_sequence_number()
                data["amount"] = txInfo.get_amount() if txInfo.get_amount() is not None else 0
                data["max_gas_amount"] = txInfo.transaction.value.get_max_gas_amount()
                data["gas_unit_price"] = txInfo.transaction.value.get_gas_unit_price()
                data["expiration_time"] = txInfo.transaction.value.get_expiration_time()
                data["public_key"] = txInfo.transaction.value.get_public_key()
                data["signature"] = txInfo.transaction.value.get_signature()
                data["script_hash"] = txInfo.transaction.value.get_script_hash()
                data["signature_scheme"] = txInfo.transaction.value.get_signature_scheme()
                data["transaction_type"] = txInfo.transaction.value.get_script().enum_name
                data["address_type"] = 2
            elif transactionType == "BlockMetadata":
                data["expiration_time"] = txInfo.transaction.value.get_timestamp_usecs() / 1000000
                data["amount"] = 0
                data["max_gas_amount"] = 0
                data["gas_unit_price"] = 0
                data["sequence_number"] = txInfo.get_events()[0].sequence_number
                data["address_type"] = 1
                data["transaction_type"] = transactionType
            elif transactionType == "WriteSet":
                events = txInfo.get_events()

                for event in events:
                    data["sequence_number"] = event.sequence_number
                    eventData = event.data

                    if eventData.value is None:
                        continue

                    if eventData.enum_name == "SentPayment":
                        data["sender"] = eventData.value.receiver
                        break
                    elif eventData.enum_name == "ReceivedPayment":
                        data["sender"] = eventData.value.sender
                        break

                data["max_gas_amount"] = 0
                data["gas_unit_price"] = 0
                data["expiration_time"] = 0
                data["amount"] = 0
                data["address_type"] = 0
                data["public_key"] = ""
                data["signature"] = ""
                data["transaction_type"] = transactionType

            data["gas_used"] = txInfo.get_gas_used()
            data["status"] = txInfo.get_vm_status()

        except Exception as e:
            logging.error(f"Parse txInfo failed: {e}")
            exit(1)

        logging.debug(f"Paser result: {data}")

        if data["transaction_type"] == "UserTransaction":
            HLibra.HandleSenderAddressInfo(data)
            HLibra.HandleReceiverAddressInfo(data)

        datas.append(data)

    HLibra.InsertTransactions(datas)
