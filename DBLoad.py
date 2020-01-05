from db import connection as conpool
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from datetime import datetime 
import logging
import sys

pool=conpool.pool
insertCount:int=0
updateCount:int=0
invalidRecords=[]
invalidLines=[]

# Mutex lock to avoid threds concurrently updating global vars
lock =Lock()

#Insert or update query string
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
    """Method to insert data in tables. Accepts array of tuples. 

    Every array element is mapped to a row in database. It also global 
    invalidRecords,inserCount and updateCount variables"""
    global insertCount
    global updateCount
    global insertQuery
    global invalidRecords

    con=pool.get_connection()
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
            con.commit()
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
    """Reads bagaddres-full.csv from folder under inputdata

    Read bagadres-ful.csv line by line and will submit 50000 lines for insertion to one thread.
    Threds are cteated and submmitted to thread pool so we can speed up insetion depending on 
    machine specifications
    """
    #Depending on CPU and Machine specs change threads number
    executor=ThreadPoolExecutor(10)
    global invalidLines
    try:
        inputFile= open("./app/inputdata/bagadres-full.csv")
    except FileNotFoundError as fne:
        logging.error("Please put bagadres-full.csv under -/inputData first and then run job")
        sys.exit(1)

    #Skipping the header line
    line = inputFile.readline()
    batchInsertValues=[]
    batchInsertCount=0
    counter=1
    
    while line:
        line=inputFile.readline()
    
        if(line!=None or line!=""):
            lineArray=line.split(";")
            try:
                #with better understanding of file and data more cleaning and validation rules 
                #can be applied.
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
                
                #Read 50000 lines and submit to a thread
                if(batchInsertCount==50000):
                    executor.submit(insertDatabase,(batchInsertValues))
                    batchInsertValues=[]
                    batchInsertCount=0
                    logging.info("batch "+str(counter)) 
                counter+=1
            except IndexError as ie:
                logging.warn("Line is not complete %s",(counter))
                invalidLines.append(line)
                logging.warn(line)
                logging.warn(ie)
    #Submit last batch of lines to thread after
    #complete file has finished reading
    executor.submit(insertDatabase,(batchInsertValues))
    inputFile.close()

    #Waites for all threads to complete before exiting out of main 
    executor.shutdown(wait=True)
    


if __name__=="__main__":
    #validate job command line inputs
    if(len(sys.argv)<3):
        print("Please enter year and month for Addess update JOB.")
    else:
        jobId=-1;
        con=pool.get_connection()
        cur=con.cursor()
        #Note start time of JOB
        cur.execute("INSERT INTO JOB_RUN_DETAIL(YEAR,MONTH,INSERTED,UPDATED) VALUES(%s,%s,%s,%s)",(sys.argv[1],sys.argv[2],-1,-1))
        jobId=cur.lastrowid;
        cur.close()
        con.commit()
        con.close()
        
        #Reads all data and submits to thread executor
        main()

        # The notification logic could be added here to send email or push notifs
        # If I get SMTP details I can add the functionality

        #Updates job end time in job run details table with update and insert count
        con=pool.get_connection()
        cur=con.cursor()
        cur.execute("UPDATE JOB_RUN_DETAIL SET INSERTED=%s,UPDATED=%s, COMPLETEDTIME=%s WHERE JOB_ID=%s",(insertCount,updateCount,datetime.now().strftime("%Y-%m-%d %H:%M:%S"),jobId))
        cur.close()
        con.commit()
        con.close()
        #Flush the invalid records to errorFile.csv
        #Creates file in write and append mode to not loose previous records
        badAddressFile=open("./app/inputdata/errorFile.csv","w+")
        
        for line in invalidLines:
            badAddressFile.write(line+"\n")

        for tupple in invalidRecords:
            outputLine=""
            for field in tupple:
                outputLine+=";"+str(field)
            badAddressFile.write(outputLine+"\n")
        badAddressFile.close()