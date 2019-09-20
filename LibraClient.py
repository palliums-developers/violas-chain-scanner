from io import BytesIO
from grpc import insecure_channel

import base64

from proto.admission_control_pb2_grpc import AdmissionControlStub
from proto.get_with_proof_pb2 import UpdateToLatestLedgerRequest
from proto.transaction_pb2 import RawTransaction
from proto.transaction_pb2 import SignedTransaction
from proto.transaction_pb2 import Program

LIBRA_MAINNET_HOST = "";
LIBRA_TESTNET_HOST = "ac.testnet.libra.org:8000";

class Client():
    def __init__(self, channel_url = LIBRA_TESTNET_HOST):
        self.channel = insecure_channel(channel_url);
        self.stub = AdmissionControlStub(self.channel);

    def GetTransactions(self, param):
        request = UpdateToLatestLedgerRequest();
        requested_item = request.requested_items.add();

        requested_item.get_transactions_request.start_version = param["startVersion"];
        requested_item.get_transactions_request.limit = param["limit"];
        requested_item.get_transactions_request.fetch_events = param["fetchEvents"];

        response = self.stub.UpdateToLatestLedger(request);
        results = [];
        i = 0;
        # print(response);
        for item in response.response_items:
            # print(item);
            for tx in item.get_transactions_response.txn_list_with_proof.transactions:
                print(tx);

                rawTxInfo = RawTransaction();
                rawTx = b"\n \000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\nU\014\030(\340\305\0108\240\237\274\353\005\032\371\001\n\304\001LIBRAVM\n\001\000\007\001J\000\000\000\006\000\000\000\003P\000\000\000\006\000\000\000\014V\000\000\000\006\000\000\000\r\\\000\000\000\006\000\000\000\005b\000\000\0003\000\000\000\004\225\000\000\000 \000\000\000\007\265\000\000\000\017\000\000\000\000\000\000\001\000\002\000\003\000\001\004\000\002\000\002\004\002\000\003\000\003\002\004\002\006<SELF>\014LibraAccount\tLibraCoin\004main\017mint_to_address\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\001\000\002\001\004\000\014\000\014\001\023\001\000\002\022$\010\001\022 \020\263\206`\230!0!\331!\274\367\374\365\253,\021By\007\331\224\002\345\250\371\307e\366\207\313\343\022\n\022\010\000\351\2445\000\000\000\000";

                # rawTxInfo.ParseFromString(rawTx);
                rawTxInfo.ParseFromString(tx.raw_txn_bytes);
                print(rawTxInfo);

                txInfo = {};
                txInfo["_id"] = param["startVersion"] + i;
                txInfo["from"] = rawTxInfo.sender_account.hex();
                print(rawTxInfo.sender_account.hex());
                txInfo["to"] = rawTxInfo.program.arguments[0].data.hex();
                print(rawTxInfo.program.arguments[0].data.hex());
                txInfo["value"] = int.from_bytes(rawTxInfo.program.arguments[1].data, "little");
                txInfo["time"] = rawTxInfo.expiration_time;
                print(rawTxInfo.expiration_time);
                print(int.from_bytes(rawTxInfo.program.arguments[1].data, "little"));
                txInfo["seqNum"] = rawTxInfo.sequence_number;
                print(rawTxInfo.sequence_number);
                txInfo["gasMax"] = rawTxInfo.max_gas_amount;
                print(rawTxInfo.max_gas_amount);
                txInfo["gasPrice"] = rawTxInfo.gas_unit_price;
                print(rawTxInfo.gas_unit_price);

                results.append(txInfo);
                ++i;

        return results;

    def get_account_states(self, addresses):
        request = UpdateToLatestLedgerRequest()
        for address in addresses:
            print(address)
            requested_item = request.requested_items.add()
            requested_item.get_account_state_request.address = bytes.fromhex(address)

        response = self.stub.UpdateToLatestLedger(request)
        results = []
        for response in response.response_items:
            blob = (
                response.get_account_state_response.account_state_with_proof.blob.blob
            )
            buffer = BytesIO(blob)
            blob_len = int.from_bytes(buffer.read(4), byteorder="little")
            key_values = {}
            for idx in range(blob_len):
                key_len = int.from_bytes(buffer.read(4), byteorder="little")
                key = buffer.read(key_len)
                val_len = int.from_bytes(buffer.read(4), byteorder="little")
                val = buffer.read(val_len)
                key_values[key] = val

            print(key_values)
        return
