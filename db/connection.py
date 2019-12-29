import mysql.connector as mysql
from mysql.connector.pooling import MySQLConnectionPool

dbConfig={
    "host" : "localhost",
    "user": "root",
    "passwd": "password",
    "database":"ADDRESS_DATA"
}

pool=MySQLConnectionPool(pool_name="mySqlPool",pool_size=15,**dbConfig)