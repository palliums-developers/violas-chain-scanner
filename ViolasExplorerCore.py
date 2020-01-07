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
    logging.debug(f"Get next id is: {nextID}")
    limit = 1000

    cli = Client("violas_testnet", "/tmp/consensus_peers.config.toml", "/tmp/faucet_keys")

    try:
        txInfos = cli.get_transactions(nextID, limit, False)
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

                if hasattr(txInfo.raw_txn.type, "sender_module_address"):
                    data["module"] = txInfo.raw_txn.type.sender_module_address

                if hasattr(txInfo.raw_txn.type, "data"):
                    data["data"] = txInfo.raw_txn.type.data

                # if len(txInfo.events) > 0:
                #     if hasattr(txInfo.events[0].tag, "module"):
                #         data["module"] = txInfo.events[0].tag.module

                #     if hasattr(txInfo.events[0].tag, "address"):
                #         data["module_address"] = txInfo.events[0].tag.address

                #     if hasattr(txInfo.events[0].event, "data"):
                #         data["data"] = txInfo.events[0].event.data

                #     if hasattr(txInfo.events[0].event, "etype"):
                #         data["etype"] = txInfo.events[0].event.etype

                #     if hasattr(txInfo.events[0].event, "price"):
                #         data["price"] = txInfo.events[0].event.price

                data["public_key"] = txInfo.public_key
                data["signature"] = txInfo.signature

            data["transaction_hash"] = txInfo.info.transaction_hash
            data["state_root_hash"] = txInfo.info.state_root_hash
            data["event_root_hash"] = txInfo.info.event_root_hash
            data["gas_used"] = txInfo.info.gas_used
            data["status"] = txInfo.info.major_status

        except Exception as e:
            logging.error(f"Final result: {txInfo}")
            exit(1)

        HViolas.HandleSenderAddressInfo(data)

        if "receiver" in data:
            HViolas.HandleReceiverAddressInfo(data)

        datas.append(data)

    HViolas.InsertTransactions(datas)
