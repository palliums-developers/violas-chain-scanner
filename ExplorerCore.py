from LibraClient import Client
from DBHandler import DBHandler

# dbh = DBHandler();
cli = Client();

while True:
    nextID = 1;# dbh.GetNextID;
    limit = 1000;

    param = {};
    param["startVersion"] = nextID;
    param["limit"] = limit;
    param["fetchEvents"] = True;

    results = cli.GetTransactions(param);

    print(results);
    # dbh.InsertTransactionInfo(results);

    # for result in results:
    #     dbh.ProcessFromAddresses(result["from"], results["_id"]);
    #     dbh.ProcessToAddresses(result["to"], results["_id"]);
