import configparser
from sqlalchemy import create_engine

from LibraModules import Base

config = configparser.ConfigParser()
config.read("./config.ini")

libraDBInfo = config["LIBRA DB INFO"]
libraDBUrl = f"{libraDBInfo['DBTYPE']}+{libraDBInfo['DRIVER']}://{libraDBInfo['USERNAME']}:{libraDBInfo['PASSWORD']}@{libraDBInfo['HOSTNAME']}:{libraDBInfo['PORT']}/{libraDBInfo['DATABASE']}"

engine = create_engine(libraDBUrl)

Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)
