from LibraClient import Client

cli = Client()

param = {}
param["startVersion"] = 1
param["limit"] = 1
param["fetchEvents"] = True

# cli.get_account_states(["0fc36d52be67289088d77ee285a1edab560df207fbb7905f8fd37e23b943bafe "])
cli.GetTransactions(param)
