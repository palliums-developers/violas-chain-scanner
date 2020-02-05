import logging
import configparser
from time import sleep

from violas import Client
from violas import TransactionType
from ViolasPGHandler import ViolasPGHandler

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
    logging.debug(f"Get next id is: {nextID}")
    limit = 1000

    cli = Client.new(config["NODE INFO"]["VIOLAS_HOST"], config["NODE INFO"]["VIOLAS_POST"])

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
                    data["address_type"] = 0
                    data["expiration_time"] = 0
                    data["amount"] = 0
                else:
                    data["address_type"] = 2
                    data["expiration_time"] = txInfo.get_expiration_time
                    data["amount"] = txInfo.get_amount()
                    data["receiver"] = txInfo.get_receiver()

                data["module"] = txInfo.get_module_address()
                data["data"] = txInfo.get_data()
                data["public_key"] = txInfo.public_key
                data["signature"] = txInfo.signature

            data["transaction_type"] = txInfo.get_code_type().name
            data["transaction_hash"] = txInfo.get_transaction_hash()
            data["state_root_hash"] = txInfo.get_state_root_hash()
            data["event_root_hash"] = txInfo.get_event_root_hash()
            data["gas_used"] = txInfo.get_gas_used()
            data["status"] = txInfo.get_major_status()

        except Exception as e:
            logging.error(f"Final result: {e}")
            exit(1)

        HViolas.HandleSenderAddressInfo(data)

        if "receiver" in data:
            HViolas.HandleReceiverAddressInfo(data)

        datas.append(data)

    HViolas.InsertTransactions(datas)
