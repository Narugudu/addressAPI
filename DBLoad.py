from db import connection as conpool
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
import os
import logging
import sys

pool=conpool.pool
insertCount=0
updateCount=0
invalidRecords=[]
lock =Lock()

insertQuery="""INSERT INTO ADDRESS_DETAILS
    (
        PUBLIC_SPACE,HOUSE_NUMBER ,HOUSE_LETTER ,HOUSE_NM_ADD ,POSTCODE ,WOONPLATS,
        GEEMENTE ,PROVINCE ,INDICATOR_NUM ,USAGE_PURPOSE ,SURFACE_RES_OBJ,
        RES_OBJ_STATUS ,OBJECT_ID,OBJECT_TYPE ,SECONDARY_ADDRESS ,CANDIDATE_ID,
        PLEGE_STATUS ,BUILD_YEAR ,X_NUM ,Y_NUM ,LANGITUDE ,LATTITUDE ) 
    VALUES
    (
            %s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s
    ) ON DUPLICATE KEY UPDATE 
        WOONPLATS=VALUES(WOONPLATS),
        GEEMENTE=VALUES(GEEMENTE),
        PROVINCE=VALUES(PROVINCE),
        INDICATOR_NUM =VALUES(INDICATOR_NUM),
        USAGE_PURPOSE =VALUES(USAGE_PURPOSE),
        SURFACE_RES_OBJ=VALUES(SURFACE_RES_OBJ),
        RES_OBJ_STATUS=VALUES(RES_OBJ_STATUS),
        OBJECT_ID=VALUES(OBJECT_ID),
        OBJECT_TYPE =VALUES(OBJECT_TYPE),
        SECONDARY_ADDRESS =VALUES(SECONDARY_ADDRESS),
        CANDIDATE_ID=VALUES(CANDIDATE_ID),
        PLEGE_STATUS=VALUES(PLEGE_STATUS) ,
        BUILD_YEAR =VALUES(BUILD_YEAR),
        X_NUM =VALUES(X_NUM),
        Y_NUM =VALUES(Y_NUM),
        LANGITUDE =VALUES(LANGITUDE)
        ,LATTITUDE=VALUES(LATTITUDE);"""


def insertDatabase(batchInputValue):
    global insertCount
    global updateCount
    global insertQuery
    global invalidRecords

    con=pool.get_connection()
    logging.info("PID %d: using connection %s" % (os.getpid(), con))
    cursor=con.cursor()

    for insertData in batchInputValue:
        try:
            cursor.execute(insertQuery,insertData)
            lock.acquire()
            if(cursor.rowcount==1):
                insertCount+=1
            elif(cursor.rowcount==2):
                updateCount+=1  
            lock.release();  
        except Exception as e:
           logging.warn(invalidRecords)
           logging.error(e)
           lock.acquire()
           invalidRecords.append(insertData)
           lock.release()

        con.commit()
    cursor.close()
    con.close()

def main():
    executor=ThreadPoolExecutor(10)
    try:
        inputFile= open("./inputdata/bagadres-full.csv")
    except FileNotFoundError as fne:
        logging.error("Please put bagadres-full.csv under -/inputData first and then run job")
        sys.exit(1)

    #Skipping line
    line = inputFile.readline()
    counter=1
    batchInsertValues=[]
    batchInsertCount=0
    
    while line:
        line=inputFile.readline()
        lineArray=line.split(";")
        try:
            lineArray[1]=None if lineArray[1]=="" else int(lineArray[1])
            lineArray[8]=None if lineArray[8]=="" else int(lineArray[8])
            lineArray[10]=None if lineArray[10]=="" else int(lineArray[10])
            lineArray[12]=None if lineArray[12]=="" else int(lineArray[12])
            lineArray[15]=None if lineArray[15]=="" else int(lineArray[15])
            lineArray[17]=None if lineArray[17]=="" else int(lineArray[17])
            lineArray[18]=None if lineArray[18]=="" else float(lineArray[18])
            lineArray[19]=None if lineArray[19]=="" else float(lineArray[19])
            lineArray[20]=None if lineArray[20]=="" else float(lineArray[20])
            lineArray[21]=None if lineArray[21]=="" else float(lineArray[21])
            
            recordInsertvalues=(lineArray[0],lineArray[1],lineArray[2],lineArray[3],lineArray[4],lineArray[5],
            lineArray[6],lineArray[7],lineArray[8],lineArray[9],lineArray[10],
            lineArray[11],lineArray[12],lineArray[13],lineArray[14],lineArray[15],
            lineArray[16],lineArray[17],float(lineArray[18]),float(lineArray[19]),float(lineArray[20]),float(lineArray[21]))

            batchInsertValues.append(recordInsertvalues)
            batchInsertCount+=1
            if(batchInsertCount==50000):
                executor.submit(insertDatabase,(batchInsertValues))
                batchInsertValues=[]
                batchInsertCount=0
                print("batch "+str(counter)) 
            counter+=1
        except IndexError as ie:
            logging.warn("Line is not complete")

    executor.submit(insertDatabase,(batchInsertValues))

    executor.shutdown(wait=True)
    inputFile.close()


if __name__=="__main__":
    if(len(sys.argv)<3):
        print("Please enter year and month for Addess update JOB.")
    else:
        main()
        con=pool.get_connection()
        cur=con.cursor()
        cur.execute("INSERT INTO JOB_RUN_DETAIL(YEAR,MONTH,INSERTED,UPDATED) VALUES(%s,%s,%s,%s)",(sys.argv[1],sys.argv[2],insertCount,updateCount))
        cur.close()
        con.commit()
        con.close()
        badAddressFile=open("./inputdata/errorFile.csv","w+")
        for tupple in invalidRecords:
            outputLine=""
            for field in tupple:
                outputLine+=";"+str(field)
            badAddressFile.write(outputLine)
        badAddressFile.close()