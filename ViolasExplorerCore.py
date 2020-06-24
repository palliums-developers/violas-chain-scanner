import logging
import configparser
from time import sleep

from violas_client import Client
from ViolasPGHandler import ViolasPGHandler

logging.basicConfig(filename = "ViolasLog.out", level = logging.WARNING)

config = configparser.ConfigParser()
config.read("./config.ini")

violasDBInfo = config["VIOLAS DB INFO"]
violasDBUrl = f"{violasDBInfo['DBTYPE']}+{violasDBInfo['DRIVER']}://{violasDBInfo['USERNAME']}:{violasDBInfo['PASSWORD']}@{violasDBInfo['HOSTNAME']}:{violasDBInfo['PORT']}/{violasDBInfo['DATABASE']}"
HViolas = ViolasPGHandler(violasDBUrl)
cli = Client.new(config['NODE INFO']['VIOLAS_HOST'])

while True:
    succ, nextID = HViolas.GetTransactionCount()

    if not succ:
        logging.error(f"ERROR: Get count of transactions failed, retry after 500ms.")
        sleep(1 / 1000 * 500)
        continue

    logging.debug(f"Get next id is: {nextID}")
    limit = 1000

    try:
        txInfos = cli.get_transactions(nextID, limit, True)
    except Exception as e:
        logging.error(f"Get transaction failed: {e}")
        cli = Client.new(config['NODE INFO']['VIOLAS_HOST'])
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
                data["sequence_number"] = txInfo.get_sequence_number()
                data["sender"] = txInfo.get_sender()
                data["receiver"] = txInfo.get_receiver()
                data["currency"] = txInfo.get_currency_code() if txInfo.get_currency_code() is not None else txInfo.get_gas_currency()
                data["gas_currency"] = txInfo.get_gas_currency()
                data["amount"] = txInfo.get_amount() if txInfo.get_amount() is not None else 0
                data["max_gas_amount"] = txInfo.transaction.value.get_max_gas_amount()
                data["gas_unit_price"] = txInfo.transaction.value.get_gas_unit_price()
                data["expiration_time"] = txInfo.get_expiration_time()
                data["public_key"] = txInfo.transaction.value.get_public_key()
                data["script_hash"] = txInfo.transaction.value.get_script_hash()
                data["signature"] = txInfo.transaction.value.get_signature()
                data["signature_scheme"] = txInfo.transaction.value.get_signature_scheme()
                data["data"] = txInfo.get_data() if txInfo.get_data() is not None and len(txInfo.get_data()) != 0 else None
                data["data_signature"] = txInfo.get_data() if txInfo.get_data() is not None and len(txInfo.get_data()) != 0 else None
                data["transaction_type"] = txInfo.get_code_type().name if txInfo.get_code_type() is not None else ""
                data["address_type"] = 2
            elif transactionType == "BlockMetadata":
                data["sequence_number"] = txInfo.get_events()[0].get_sequence_number()
                data["sender"] = txInfo.get_events()[0].get_data().value.proposer
                data["expiration_time"] = int(txInfo.transaction.value.get_timestamp_usecs() / 1000000)
                data["amount"] = 0
                data["max_gas_amount"] = 0
                data["gas_unit_price"] = 0
                data["transaction_type"] = txInfo.get_code_type().name
                data["address_type"] = 1
            elif transactionType == "WriteSet":
                data["sequence_number"] = 0
                data["sender"] = ""
                data["amount"] = 0
                data["max_gas_amount"] = 0
                data["gas_unit_price"] = 0
                data["expiration_time"] = 0
                data["transaction_type"] = txInfo.get_code_type().name
                data["address_type"] = 0

            data["gas_used"] = txInfo.get_gas_used()
            data["status"] = txInfo.get_vm_status()

        except Exception as e:
            logging.error(f"Final result: {e}")
            exit(1)

        logging.debug(f"Paser result: {data}")

        if data["transaction_type"] == "UserTransaction":
            HViolas.HandleSenderAddressInfo(data)
            HViolas.HandleReceiverAddressInfo(data)

        datas.append(data)

    HViolas.InsertTransactions(datas)
