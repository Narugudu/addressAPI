# AddressAPI
The repository is a sample project for Matrixian group. It takes input from CSV file and updates databse every month or inserts new data.

![Image of Yaktocat](https://github.com/Narugudu/addressAPI/blob/master/Screenshot%20from%202020-01-05%2013-14-11.png)


# Getting started
The application works in two parts.
* DBLoader
* AddressFlaskAPI

### DBLoader.py
This script acts as a loader job which will take input bagaddress-full.csv and loads data into ADDRESS_DETAILS table so it can be used by API later for serachung and generating report

**Prerequisites:**
* bag-adres.csv file under inputdata directory
* Database ADDRESS_DATA 
* ADDRESS_DETAILS Table under above database as per query.sql

How to run?
```
python app/DBLoader.py <year> <month>
```

### AddressFlaskAPI.py
It is a Flask application which connects to database and provides endpoints for API and reports.

**Prerequisites:**
* Database ADDRESS_DATA 
* ADDRESS_DETAILS Table under above database as per query.sql
* JOB_RUN_DETAIL Table under above database as per query.sql


How to run?
```
python app/AddressFlaskAPI.py <year> <month>
```

Address search:
```
http://localhost:5000/house/?postcode=<post code>&houseNumber=<house number>&houseNumExt=<additionalHouse number>
```
Example:
http://localhost:5000/house/?postcode=1011AB&houseNumber=105&houseNumExt=1


Reports:
```
http://localhost:5000/reports/<year>/<month>
```
Example:
http://localhost:5000/reports/2019/03



## Author

* **Narendra Tripathi** 

## Built With

* [Python 3](https://www.python.org/) - The programming lang
* [Flask](http://flask.palletsprojects.com/en/1.1.x/) - Web Framework
* [ MySQL](https://www.mysql.com/) - Database

