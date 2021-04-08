import logging
import configparser
from time import sleep
import requests

from violas_client import Client
from ViolasPGHandler import ViolasPGHandler
from violas_client.lbrtypes.account_config.constants.lbr import CORE_CODE_ADDRESS
from violas_client.lbrtypes.account_config import association_address
from violas_client.lbrtypes.bytecode import CodeType

logging.basicConfig(filename = "ViolasLog.out", level = logging.WARNING)
import Common


config = configparser.ConfigParser()
config.read("./config.ini")

violasDBInfo = config["VIOLAS DB INFO"]
violasDBUrl = f"{violasDBInfo['DBTYPE']}+{violasDBInfo['DRIVER']}://{violasDBInfo['USERNAME']}:{violasDBInfo['PASSWORD']}@{violasDBInfo['HOSTNAME']}:{violasDBInfo['PORT']}/{violasDBInfo['DATABASE']}"
HViolas = ViolasPGHandler(violasDBUrl)

cli = Client.new(config['NODE INFO']['VIOLAS_HOST'])

cli.set_exchange_module_address(CORE_CODE_ADDRESS)
cli.set_exchange_owner_address(config["NODE INFO"]["EXCHANGE_MODULE_ADDRESS"])

cli.set_bank_module_address(CORE_CODE_ADDRESS)
cli.set_bank_owner_address(config["NODE INFO"]["BANK_MODULE_ADDRESS"])

limit = 1000
succ, nextID = HViolas.GetLastTransactionVersion()
if not succ:
    logging.critical(f"ERROR: Get count of transactions failed.")
    exit()

if nextID:
    nextID += 1

while True:
    logging.debug(f"Get next id is: {nextID}")

    try:
        txInfos = cli.get_transactions(nextID, limit, True)
    except Exception as e:
        logging.error(f"Get transaction failed: {e}")
        cli = Client.new(config['NODE INFO']['VIOLAS_HOST'])
        continue

    if len(txInfos) == 0:
        sleep(1 / 1000 * 500)
        continue
    elif len(txInfos) == limit:
        nextID += limit
    else:
        nextID += len(txInfos)

    datas = []
    incentives = []
    for index, txInfo in enumerate(txInfos):
        incentive = None
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

                if data["transaction_type"] in Common.BANK_TRANSACTIONS:
                    incentive = {
                        "address": txInfo.get_sender(),
                        "amount": txInfo.get_incentive() if txInfo.get_incentive() is not None else 0,
                        "date": txInfo.get_expiration_time(),
                        "status": 1,
                        "type": Common.INCENTIVE_TYPE.get(data["transaction_type"])
                    }
                elif data["transaction_type"] in Common.EXCHANGE_TRANSACTIONS:
                    incentive = {
                        "address": txInfo.get_sender(),
                        "amount": txInfo.get_swap_reward_amount() if txInfo.get_swap_reward_amount() is not None else 0,
                        "date": txInfo.get_expiration_time(),
                        "status": 1,
                        "type": Common.INCENTIVE_TYPE.get(data["transaction_type"])
                    }
            else:
                continue

        except Exception as e:
            logging.error(f"Final result: {e}")
            exit(1)

        logging.debug(f"Paser result: {data}")

        if txInfo.transaction.enum_name == "UserTransaction":
            HViolas.HandleSenderAddressInfo(data)
            if data.get("receiver") is not None:
                HViolas.HandleReceiverAddressInfo(data)

        datas.append(data)
        if incentive is not None and incentive.get("amount") != 0:
            incentives.append(incentive)

        if data["transaction_type"] in ["PEER_TO_PEER_WITH_METADATA"]:
            try:
                resp = requests.post(
                    "http://127.0.0.1:4006/violas/push/message",
                    json = {
                        "service": "violas_01",
                        "version":data["version"]
                    }
                )
            except Exception as e:
                logging.error(f"ERROR: Request push server failed, except: {e}")

    HViolas.InsertTransactions(datas)
    HViolas.InsertIncentives(incentives)
