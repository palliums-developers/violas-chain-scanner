import configparser
from sqlalchemy import create_engine

from ViolasModules import Base

config = configparser.ConfigParser()
config.read("./config.ini")

violasDBInfo = config["VIOLAS DB INFO"]
violasDBUrl = f"{violasDBInfo['DBTYPE']}+{violasDBInfo['DRIVER']}://{violasDBInfo['USERNAME']}:{violasDBInfo['PASSWORD']}@{violasDBInfo['HOSTNAME']}:{violasDBInfo['PORT']}/{violasDBInfo['DATABASE']}"

engine = create_engine(violasDBUrl)

Base.metadata.tables["transactions"].drop(engine)
Base.metadata.tables["address_info"].drop(engine)
Base.metadata.tables["incentive_issue_record"].drop(engine)

Base.metadata.tables["transactions"].create(engine)
Base.metadata.tables["address_info"].create(engine)
Base.metadata.tables["incentive_issue_record"].create(engine)
