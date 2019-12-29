#!/usr/bin/env python
from db import connection as conpool;
from flask import request,Response
from flask import Flask,render_template
import json

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
    con=conpool.pool.get_connection()
    cur=con.cursor()
    house_number=request.args.get("houseNumber")
    house_number_addition=request.args.get("houseNumExt")
    postcode=request.args.get("postcode")
    if(house_number==None or house_number_addition==None or postcode==None):
        return Response("{'error':'House Number,Postcode and Extension all are required attributes'}",400) 
    cur.execute(house_search_query,
    (house_number,postcode,house_number_addition));
    row=cur.fetchone();
    if(row==None):
        return Response("{'error':'can not find house with given details'}",404)
    output={
    "openbareruimte":row[0],"huisnummer":row[1],"houseLetter":row[2],
    "huisnummertoevoeging":row[3],"postCode":row[4],"woonPlats":row[5],"geemente":row[6],
    "provincie":row[7],"nummeraanduiding":row[8],"verblijfsobjectgebruiksdoel":row[9],
    "oppervlakteverblijfsobject":row[10],"verblijfsobjectstatus":row[11],"object_id":row[12],
    "object_type":row[13],"nevenadres":row[14],"pandid":row[15],"pandstatus":row[16],
    "pandstatus":row[17],"x":row[18],"y":row[19],"longitude":row[20],"lattitude":row[21]        
    }

    print(row)
    return json.dumps(output);

@app.route("/reports/<int:year>/<int:month>")
def batch_report(year,month):
    con=conpool.pool.get_connection();
    cur=con.cursor()
    cur.execute("SELECT INSERTED,UPDATED FROM JOB_RUN_DETAIL WHERE YEAR=%s AND MONTH=%s",(year,month))
    row=cur.fetchone()
    cur.close()
    con.close()

    if(row==None):
        insertCount=-1
        updateCount=-1
    else:
        insertCount=row[0]
        updateCount=row[1]

    return render_template("reports.html",title="Report",year=year,month=month,insertCount=insertCount,
    updateCount=updateCount)




if(__name__=='__main__'):
    app.run(debug=True)