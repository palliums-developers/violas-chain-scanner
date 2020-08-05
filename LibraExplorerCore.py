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
cli = Client("libra_testnet")

while True:
    succ, nextID = HLibra.GetTransactionCount()
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
        cli = Client("libra_testnet")
        continue

    if len(txInfos) == 0:
        sleep(1 / 1000 * 500)
        continue

    try:
        while True:
            ltx = txInfos[-1]
            if ltx.get_code_type() != CodeType.BLOCK_METADATA:
                sleep(1 / 1000 * 500)
                tx = cli.get_transaction(ltx.get_version()+1)
                if tx is not None:
                    txInfos.append(tx)
                continue
            break

    except Exception as e:
        logging.error(f"Get transaction failed: {e}")
        cli = Client.new(config['NODE INFO']['VIOLAS_HOST'])
        continue
    datas = []
    for index, txInfo in enumerate(txInfos):
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
                data["transaction_type"] = txInfo.get_code_type().name if txInfo.get_code_type() is not None else ""
                data["address_type"] = 2
                next_index = index + 1
                while True:
                    if txInfos[next_index].get_code_type() == CodeType.BLOCK_METADATA:
                        data["confirmed_time"] = txInfos[next_index].get_expiration_time()
                        break
                    next_index = next_index + 1

            elif transactionType == "BlockMetadata":
                data["sequence_number"] = txInfo.get_events()[0].sequence_number
                data["sender"] = txInfo.get_events()[0].data.value.proposer
                data["expiration_time"] = int(txInfo.transaction.value.get_timestamp_usecs())
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
            data["status"] = txInfo.get_vm_status().enum_name

        except Exception as e:
            logging.error(f"Parse txInfo failed: {e}")
            exit(1)

        logging.debug(f"Paser result: {data}")

        if transactionType == "UserTransaction":
            HLibra.HandleSenderAddressInfo(data)
            if data["receiver"] is not None:
                HLibra.HandleReceiverAddressInfo(data)

        datas.append(data)
    HLibra.InsertTransactions(datas)
