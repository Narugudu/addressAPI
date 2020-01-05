#!/usr/bin/env python
from db import connection as conpool;
from flask import request,Response
from flask import Flask,render_template
import json
import logging

app= Flask(__name__)
house_search_query='''
SELECT 
    PUBLIC_SPACE,HOUSE_NUMBER,HOUSE_LETTER,
    HOUSE_NM_ADD,POSTCODE,WOONPLATS,GEEMENTE,
    PROVINCE,INDICATOR_NUM,USAGE_PURPOSE,SURFACE_RES_OBJ,
    RES_OBJ_STATUS,OBJECT_ID,OBJECT_TYPE,SECONDARY_ADDRESS,
    CANDIDATE_ID,PLEGE_STATUS,BUILD_YEAR,X_NUM,Y_NUM,
    LANGITUDE ,LATTITUDE FROM ADDRESS_DETAILS 
    
    WHERE HOUSE_NUMBER=%s AND POSTCODE=%s AND HOUSE_NM_ADD=%s
'''
@app.route("/house/")
def search_house():
    """ Searches house on input params
    
    Search house API allows client to search House data on postcode,house number 
    and house number extra info. 

    If mandantory parameters are not provided it will throw 400 - Bad request
    If address is not found it throw 404 - Not found HTTP status code
    
    On successfull execution API will return valid JSON with data with house details

    """
    con=conpool.pool.get_connection()
    cur=con.cursor()
    house_number=request.args.get("houseNumber")
    house_number_addition=request.args.get("houseNumExt")
    postcode=request.args.get("postcode")
    if(house_number==None or house_number_addition==None or postcode==None):
        return Response("{'error':'House Number,Postcode and Extension all are required attributes'}",400) 
    cur.execute(house_search_query,
    (house_number,postcode,house_number_addition))
    row=cur.fetchone()
    if(row==None):
        logging.warn("House not found for %s,%s,%s",(house_number,house_number_addition,postcode))
        return Response("{'error':'can not find house with given details'}",404)
    output={
    "openbareruimte":row[0],"huisnummer":row[1],"houseLetter":row[2],
    "huisnummertoevoeging":row[3],"postCode":row[4],"woonPlats":row[5],"geemente":row[6],
    "provincie":row[7],"nummeraanduiding":row[8],"verblijfsobjectgebruiksdoel":row[9],
    "oppervlakteverblijfsobject":row[10],"verblijfsobjectstatus":row[11],"object_id":row[12],
    "object_type":row[13],"nevenadres":row[14],"pandid":row[15],"pandstatus":row[16],
    "pandstatus":row[17],"x":row[18],"y":row[19],"longitude":row[20],"lattitude":row[21]        
    }

    loggin.debug(row)
    return json.dumps(output)

@app.route("/reports/<int:year>/<int:month>")
def batch_report(year,month):
    """Batch report is web page for job status.
    
    This page gives us a pie chart for insert and update count 
    as well as how current job run compares with last job runs.    
    """
    con=conpool.pool.get_connection()
    cur=con.cursor()
    cur.execute("SELECT INSERTED,UPDATED,STARTTIME,COMPLETEDTIME FROM JOB_RUN_DETAIL WHERE YEAR<=%s AND MONTH<=%s ORDER BY JOB_ID DESC LIMIT 4 ",(year,month))
    records=cur.fetchall()
    reportsDetailList=[]
    for row in records:    
        reportDetails={
            "insertCount":row[0],
            "updateCount":row[1],
            "startedTime":row[2],
            "endTime":row[3],
            "totalTime":(row[3]-row[2]).total_seconds()
                }
        reportsDetailList.append(reportDetails)

    cur.close()
    con.close()
    return render_template("reports.html",title="Report",year=year,month=month,reportsDetailList=reportsDetailList)




if(__name__=='__main__'):
    app.run(debug=True)