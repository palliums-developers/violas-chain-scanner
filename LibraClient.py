from io import BytesIO
from grpc import insecure_channel

from proto.admission_control_pb2_grpc import AdmissionControlStub
from proto.get_with_proof_pb2 import UpdateToLatestLedgerRequest
from proto.transaction_pb2 import RawTransaction
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

        for item in response.response_items:
            for tx in item.get_transactions_response.txn_list_with_proof.transactions:
                print(tx)
                rawTxInfo = RawTransaction();
                rawTxInfo.ParseFromString(tx.raw_txn_bytes);
                print(rawTxInfo);
                rawProgram = rawTxInfo.program;
                print(rawProgram);

        return;

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
