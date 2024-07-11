import json
import logging
import os
import tempfile
import time
import urllib.parse
from datetime import datetime, timedelta

import azure.functions as func
import pyodbc
import requests

from variables import table


def getIds(branch,endpoint,dateEncoded,item):
    # Gathers the necessary api keys and secret data needed to create connections from data sources/destinations.
    secret = {
        "odbcString" : "Driver={ODBC Driver 17 for SQL Server};Server=tcp:moxiereportingserver.database.windows.net,1433;Database=PestroutesDB;Encrypt=yes;Uid=powerbiadmin;Pwd=KJXiET15TuDp;TrustServerCertificate=no;Connection Timeout=30;",        
        "authenticationToken" : branch["authenticationToken"],
        "authenticationKey" : branch["authenticationKey"],
        "officeID" : branch["officeID"]
    }
    if 'dateUpdate' in endpoint:
        idUrl = endpoint["dateUpdate"]
    else: 
        return
    statusEncoded = urllib.parse.quote('{"operator":"IN","value":["-3","-2","-1","0","1","2"]}')
    # Checks which class the function is currently loading. Certain classes require statuses to be specified in order for the post request to gather all data from the endpoint.
    if endpoint["dateBool"] is True:
        if item == "Appointment":
            url = idUrl.format(secret["officeID"],secret["authenticationToken"],secret["authenticationKey"],dateEncoded,urllib.parse.quote('{"operator":"IN","value":["-2","-1","0","1","2"]}'))
        elif item == "Payment":
            url = idUrl.format(secret["officeID"],secret["authenticationToken"],secret["authenticationKey"],dateEncoded,urllib.parse.quote('{"operator":"IN","value":["0","1","2"]}'))
        elif item == "Ticket":
            url = idUrl.format(secret["officeID"],secret["authenticationToken"],secret["authenticationKey"],dateEncoded,urllib.parse.quote('{"operator":"IN","value":["0","1"]}'))
        elif item == "Subscription":
            url = idUrl.format(secret["officeID"],secret["authenticationToken"],secret["authenticationKey"],dateEncoded,urllib.parse.quote('{"operator":"IN","value":["0","1"]}'))
        elif item == "Lead":
            url = idUrl.format(secret["officeID"],secret["authenticationToken"],secret["authenticationKey"],dateEncoded,'&active={"operator":"IN", "value":[-3]}')
        elif item == "Customer":
            url = idUrl.format(secret["officeID"],secret["authenticationToken"],secret["authenticationKey"],dateEncoded,urllib.parse.quote('{"operator":"IN","value":["0","1"]}'))
        elif item == "Door":
            url = idUrl.format(secret["officeID"],secret["authenticationToken"],secret["authenticationKey"],dateEncoded,urllib.parse.quote('{"operator":"IN","value":["-2","-1","0","1","2"]}'))
        elif item == "ServicePlan":
            url = idUrl.format(secret["officeID"],secret["authenticationToken"],secret["authenticationKey"],dateEncoded,urllib.parse.quote('{"operator":"IN","value":["-2","-1","0","1","2"]}'))
        elif item == "Task":
            url = idUrl.format(secret["officeID"],secret["authenticationToken"],secret["authenticationKey"],dateEncoded,urllib.parse.quote('{"operator":"IN","value":["0","1","2","3"]}'),urllib.parse.quote('{"operator":"IN","value":["0","1"]}'))
        else:
            url = idUrl.format(secret["officeID"],secret["authenticationToken"],secret["authenticationKey"],dateEncoded)
    elif item == "Employee":
        url = idUrl.format(secret["officeID"],secret["authenticationToken"],secret["authenticationKey"],dateEncoded,urllib.parse.quote('{"operator":"IN","value":["0","1"]}'))    
    else:
        return
    # else:
    #     url = idUrl.format(secret["officeID"],secret["authenticationToken"],secret["authenticationKey"])
    # Variables that contain the response from the post request. This initial request only gathers the ID numbers of the rows from the search request.
    # pestroutesData = requests.get(url)
    # data = pestroutesData.json()
    # # Checks if the request was successful
    # if data["success"] != True:
    #     print(data["errorMessage"])
    #     return data
    # ids = data[endpoint["queryPlural"]]
    # # Sends the ID's to another function which will go through the list of ID's by 1000 each time.
    # iterateIds(ids,secret,endpoint)
    
   
    finalList = []
    results = [0]   
    maxValue = 0
    while len(results) > 0:         
        data = {    
            '{}'.format(endpoint['queryPlural']): '{"operator":">","value": ' + str(maxValue) + '}' ##Method 2. Uncomment everything that's commented and then comment this to swap
        }     
        headers = {
        'Content-Type': 'application/json',
        }
        # url = "https://moxie{}.pestroutes.com/api/{}/search?authenticationToken={}&authenticationKey={}".format(officeID,tableName,aToken,aKey)
        response = requests.get(url,headers=headers,json=data)
        # print(url)
        if response.status_code != 200:
            response = requests.get(url,headers=headers,json=data)
            response = response.json()
        else:
            response = response.json()
        
        results = response["{}".format(endpoint['queryPlural'])] 
        # print('Result Length: '+str(len(results)))    
        finalList += results
        
        if len(finalList) < 1:
            print('No {} records for office {}'.format(endpoint['urlEndpoint'],secret['officeID']))
            return
        
        # print(endpoint['urlEndpoint'])
        
        maxValue = finalList[-1]

    total = len(finalList)
    
    print('Total count of IDs: {}'.format(str(total)))
    
    iterateIds(finalList,secret,endpoint,item)


def iterateIds(ids,secret,endpoint,item):
    # Logs the total of ID's to load for a specific class and branch
    length = len(ids)
    print((""+ str(length) + " total {}'s to dateUpdate in officeID: " + str(secret["officeID"]) +"").format(item))
    # Creates the connection to the SQL Server and queries for all ID's for that class to compare whether the ID's we are requesting from PR already exists in the database

    # While loop to iterate through the ID's by 1000 as it is the maximum amount you can bulk request from the api.
    while length > 1000:
        # 
        processedIDs = ids[0:999]
        ids = ids[999:]
        # Sends the next 1000 ID's to another function for the bulk load request.
        data = getBulkData(processedIDs,secret,endpoint)
        # After getting the bulk data for the 1000 ID's, if the class is payment, it gets sent to another function to specifically transform some of that data to be suitable for the database.
        # Otherwise it is inserted into the database as the raw data.
        # print(str(data))
        status = insertData(data,secret,endpoint,processedIDs)
        # Reset the total ID's to the total after subtracting the data that has just been loaded.
        length = len(ids)
    # Continutes the bulk loading of data after the total ID's left to request is less than 1000
    if length > 0:
        data = getBulkData(ids,secret,endpoint)
        # print(str(data))
        status = insertData(data,secret,endpoint,ids)


def getBulkData(ids,secret,endpoint):
    # Creates the url for extracting the bulk data of the ID's sent from the previous function
    bulkUrl = endpoint["bulkUrl"]
    url = bulkUrl.format(secret["officeID"],secret["authenticationToken"],secret["authenticationKey"])
    # print(url)
    payload = {
    endpoint["payloadKey"] : str(ids)
    }
    # The response after sending the post request
    try:
        res = requests.post(url,payload).json()
    except Exception as e:
        res = requests.post(url,payload).json()
    # If the request contains an error message, waits 60 seconds before sending another request.
    if "errorMessage" in res:
        print(res["errorMessage"])
        time.sleep(60)
        res = requests.post(url,payload).json()
        data = res[endpoint["dataPlural"]]
        print(res)
    else:
        data = res[endpoint["dataPlural"]]
    return data

def insertData(data,secret,endpoint,ids):
    sqlConnection = pyodbc.connect(secret["odbcString"])
    cursor = sqlConnection.cursor()
    cursor.execute('SET QUOTED_IDENTIFIER OFF')
    dbColumns = []
    columnQuery = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'{}'".format(endpoint['sqlTable'])
    status = cursor.execute(columnQuery)
    for row in status:
        dbColumns.append(str(row[0]))
        
    nonDBColumns = []
    prColumns = list(set().union(*(d.keys() for d in data)))
    
    for column in prColumns:
        if column not in dbColumns:
            nonDBColumns.append(str(column))
            
            with open('{}/newColumns.txt'.format(tempfile.gettempdir()), 'r') as f:
                newColumns = f.read()
            if column not in newColumns:
                with open('{}/newColumns.txt'.format(tempfile.gettempdir()), 'a+') as f:
                    f.write('Table: {} - Column: {} - Sample Data: {} \n'.format(endpoint['sqlTable'],column,data[0][column]))

                print('Tried to document new column to temp file: {}'.format(column))

                data = [{k: v for k, v in d.items() if k != column} for d in data]
            else:
                
                data = [{k: v for k, v in d.items() if k != column} for d in data]
    # nonDBColumns = list(set(dbColumns).difference(set(prColumns)))
    # if len(nonDBColumns) > 0:
    #     sendEmail(nonDBColumns,endpoint['sqlTable'],fp)

    # if endpoint['sqlTable'] == 'Payment':
    #     data = [{k: v for k, v in d.items() if k != 'transactionID'} for d in data]

    # print(next(item for item in data if item["subscriptionID"] == '1937020'))
    for row in data:
        for key in row:
            if endpoint['sqlTable'] == 'Subscription':
                if key == 'cancellationNotes' and len(row[key]) > 0:
                    reason = row[key]
                    reason = reason[0]
                    # print(reason)
                    reason = reason['cancellationReason']
                    # print(reason)
                    row[key] = reason
            if "date" in key:
                if row[key] == None or row[key] == '0000-00-00' or row[key] == '0000-00-00 00:00:00' or row[key] == 'None':
                    continue

                try:
                    if len(row[key]) == 10:
                        checkDate = datetime.strptime(row[key], "%Y-%m-%d")
                    elif len(row[key]) == 19:
                        checkDate = datetime.strptime(row[key], "%Y-%m-%d %H:%M:%S")
                    # print(row[key])
                    # print("Date Valid")
                    
                except Exception as e:
                    print(e)
                    print(row[key])
                    print(key)
                    print("Date Invalid")
                    row[key] = "2000-01-01 00:00:00"
            # elif endpoint['sqlTable'] == 'Payment':
            #     if key == 'transactionID':
        # if endpoint['sqlTable'] == 'Payment':
        #     row['transactionIDNew'] = row.pop('transactionID')


    try:
        nestedList = [[key for key in data[0].keys()], *[list(idx.values()) for idx in data ]]
    except:
        nestedList = [[key for key in data[0].keys()], *[list(idx.values()) for idx in data ]]
    # for el in nestedList:
    #     if '1937020' in str(el):
    #         print(el)
    status = mergeData(nestedList,endpoint,secret)
    # status2 = mergeData2(nestedList,endpoint,secret)
    if endpoint["sqlTable"] == "Customer":
        insertCustomerFlag(data,secret,ids)

def insertPaymentData(data,secret,endpoint):

    for row in data:
        for key in row:
            if key == 'paymentApplications':
                for paymentApplication in row[key]:
                    row[key]['paymentApplications']
    nestedList = [[key for key in data[0].keys()], *[list(idx.values()) for idx in data ]]
    
    status = mergeData(nestedList,endpoint,secret)
def insertCustomerFlag(data,secret,ids):
    # If the class is customer, this will update the flag columns of that customer.
    # This gathers the customer flags of the customer ID's that was received
    endpoint = table.tables["CustomerFlag"]
    idsEncoded = urllib.parse.quote(str(ids))
    bulkUrl = endpoint["bulkUrl"]
    url = bulkUrl.format(secret["officeID"],secret["authenticationToken"],secret["authenticationKey"])
    payload = {
    endpoint["payloadKey"] : str(ids)
    }
    # The response after sending the post request
    try:
        res = requests.post(url,payload).json()
    except Exception as e:
        res = requests.post(url,payload).json()
    # If the request contains an error message, waits 60 seconds before sending another request.
    if "errorMessage" in res:
        print(res["errorMessage"])
        time.sleep(60)
        res = requests.post(url,payload).json()
        data = res[endpoint["dataPlural"]]
        print(res)
    else:
        data = res[endpoint["dataPlural"]]
    sqlConnection = pyodbc.connect(secret["odbcString"])
    cursor = sqlConnection.cursor()
    cursor.execute('SET QUOTED_IDENTIFIER OFF')
    # Iterates through the flags and creates an update statement to update that customers flag
    for row in data:
        id = int(row[endpoint["sqlPK"]])
        column = row["flag"]
        if (column == "pendingCancellation") or (column == "paidInFull") or (column == "switchOver") or (column == "salesmanAPay") or (column == "prefersPaper") or (column == "purpleDragon") or (column == "Pending Cancellation") or (column == "Paid In Full") or (column == "Switch Over") or (column == "Sales Rep APay") or (column == "Prefers Paper") or (column == "Purple Dragon") :
            if column == 'Prefers Paper':
                column = 'prefersPaper'
            if column == "Prefers Paper":
                column = "prefersPaper"
            if column == "Paid In Full":
                column = "paidInFull"
            if column == "Switch Over":
                column = "switchOver"
            if column == "Sales Rep APay":
                column = "salesmenAPay"
            if column == "Purple Dragon":
                column = "purpleDragon"
            value = row["flagValue"]
            if value == 'On':
                value = '1'
            if value == 'Off':
                value = '0'
            initStatment = "UPDATE Customer SET salesmanAPay = 0, pendingCancellation = 0, switchOver = 0, prefersPaper = 0, purpleDragon = 0 where customerID = {}".format(id)
            cursor.execute(initStatment)
            insertStatement = "UPDATE Customer SET {} = '{}' WHERE customerID = '{}'".format(column,value,id)
            # print(insertStatement)
            cursor.execute(insertStatement)
    # Commit and close connection
    sqlConnection.commit()
    sqlConnection.close()  

def executeStatement(statement,secret,data):
    # Create sql connection
    sqlConnection = pyodbc.connect(secret["odbcString"])
    cursor = sqlConnection.cursor()
    try:
        cursor.execute('SET QUOTED_IDENTIFIER OFF')
        # Execute insert statement
        
        cursor.execute(statement,list(data.values()))
        sqlConnection.commit()
        sqlConnection.close()  
        return False
    except Exception as e:
        # If there is an error, it returns True which will make the previous function go back and execute this function again. This is to mitigate any SQL connection issues.
        print(e)
        sqlConnection.close()  
        print("Fail")
        return True
    
################ WORKING VERSION ################################
def mergeData(data,endpoint,secret):
    # Create sql connection
    sqlConnection = pyodbc.connect(secret["odbcString"])
    cursor = sqlConnection.cursor()
    cursor.execute('SET QUOTED_IDENTIFIER OFF')
    nestedList = data
    columns = nestedList.pop(0)
    newNestedList = []
    # if "parentID" in columns:
    #     index1 = columns.index("parentID")
    #     del columns[index1]
    # if "templateType" in columns:
    #     index2 = columns.index("templateType")
    #     if index1 < index2:
    #         del columns[index2 - 1]
    #     else:
    #         del columns[index2]
    
    # print(nestedList)
    for listdata in nestedList:
        newList = []
        
        # del listdata[index1]
        # if index1 < index2:
        #     del listdata[index2 - 1]
        # else:
        #     del listdata[index2]
        for element in listdata:
            if type(element) == str and len(element) > 0:
                element = str(element)
                if endpoint["sqlTable"] in ('Appointment','Subscription','Lead','Note','Task','Changelog','Review','Route','Customer','Insect','Diagram'):
                    element = element.replace("'","''")
                element = element.replace("\\","")
                # element = element.replace('[','')
                # element = element.replace(']','')
                # element = element.replace('{','')
                # element = element.replace('}','')
                element = element.replace('\r','')
                element = element.replace('\n','')
            elif element != None and (str(element) == "0000-00-00 00:00:00" or len(str(element)) < 1 or element == "0000-00-00" or element == "00:00:00"):
                element = None
            elif type(element) == list:
                if len(element) < 1:
                    element = None
                element = str(element)
                if endpoint["sqlTable"] in ('Appointment','Subscription','Lead','Note','Task','Changelog','Review','Route','Customer','Insect','Diagram'):
                    element = element.replace("'","''")
                    element = element.replace("{''",'{"')
                    element = element.replace("''}",'"}')
                    element = element.replace("'':",'":')
                    element = element.replace(": ''",': "')
                    element = element.replace(", ''",', "')
                    element = element.replace("'',",'",')
                element = element.replace("\\","")
                element = element.replace('}','} ')
                element = element.replace(' {','{')
            elif type(element) == dict:
                if 'items' in element:
                    del element['items']
                element = str(element)
                if endpoint["sqlTable"] in ('Appointment','Subscription','Lead','Note','Task','Changelog','Review','Route','Customer','Insect','Diagram'):
                    element = element.replace("'","''")
                    element = element.replace("{''",'{"')
                    element = element.replace("''}",'"}')
                    element = element.replace("'':",'":')
                    element = element.replace(": ''",': "')
                    element = element.replace(", ''",', "')
                    element = element.replace("'',",'",')
                element = element.replace("\\","")
            elif type(element) == int or type(element) == float:
                element = element
            elif element == None:
                element = element
            else:
                element = str(element)
                if endpoint["sqlTable"] in ('Appointment','Subscription','Lead','Note','Task','Changelog','Review','Route','Customer','Insect','Diagram'):
                    element = element.replace("'","''")
                element = element.replace("\\","")
            newList.append(element)
        newNestedList.append(newList)

    nestedTuple = [tuple(l) for l in newNestedList]
    dataString = str(nestedTuple)
    dataString = dataString[1:-1]
    dataString = dataString.replace(", None", ", NULL")
    dataString = dataString.replace("\\", "")
    dataString = dataString.replace(", '0000-00-00 00:00:00'", ", NULL")
    dataString = dataString.replace(", '00:00:00'", ", NULL")
    dataString = dataString.replace(", '0000-00-00'", ", NULL")
    # dataString = dataString.replace(", [", ", '")
    # dataString = dataString.replace("],", "',")
    # dataString = dataString.replace("])", "')")
    # dataString = dataString.replace("})", "}')")
    # dataString = dataString.replace(", {", ", '{")
    # dataString = dataString.replace("},", "}',")
    dataString = dataString.replace(", []", ", NULL")
    dataString = dataString.replace(", {}", ", NULL")
    primaryKey = endpoint['sqlPK']
    table = endpoint['sqlTable']
    sourceColumns = ''
    sourceColumnsPre = ''
    targetColumns = ''
    matchColumns = ''
    onStatement = 'tgt.{} = src.new{}'.format(primaryKey,primaryKey)
    for column in columns:
        if column == 'end':
            formatStr = '[end]'
            matchColumn = 'tgt.{} = src.new{} ,'.format(formatStr,column)
            sourceColumn = 'new{} ,'.format(column)
            sourceColumnPre = 'src.new{} ,'.format(column)
            targetColumn = '{} ,'.format(formatStr)
        elif column == 'open':
            formatStr = '[open]'
            matchColumn = 'tgt.{} = src.new{} ,'.format(formatStr,column)
            sourceColumn = 'new{} ,'.format(column)
            sourceColumnPre = 'src.new{} ,'.format(column)
            targetColumn = '{} ,'.format(formatStr)
        else:
            matchColumn = 'tgt.{} = src.new{} ,'.format(column,column)
            sourceColumn = 'new{} ,'.format(column)
            sourceColumnPre = 'src.new{} ,'.format(column)
            targetColumn = '{} ,'.format(column)
        matchColumns += matchColumn
        sourceColumns += sourceColumn
        sourceColumnsPre += sourceColumnPre
        targetColumns += targetColumn

    matchColumns = matchColumns[:-1]
    sourceColumns = sourceColumns[:-1]
    sourceColumnsPre = sourceColumnsPre[:-1]
    targetColumns = targetColumns[:-1]
    sample = '''MERGE INTO dbo.[{a}] AS tgt
    USING (SELECT * FROM (VALUES {b}) AS s 
    ({c}) ) AS src 
    ON {d} 
    WHEN MATCHED THEN 
    UPDATE SET 
    {e} 
    WHEN NOT MATCHED BY TARGET THEN 
    INSERT 
    ({f}) 
    VALUES 
    ({g}); '''.format(
        a = endpoint['sqlTable'],
        b = dataString,
        c = sourceColumns,
        d = onStatement,
        e = matchColumns,
        f = targetColumns,
        g = sourceColumnsPre
    )
    # print(sample)
    # if str(1684285) in sample:
    #     print("LETS GOOOOO")
    # with open('query2.txt', 'w',encoding="utf-8") as f:
    #     f.write(str(sample))
    success = False
    count = 0

    while success != True:
        try:
            status = cursor.execute(('''MERGE INTO dbo.[{a}] AS tgt
                USING (SELECT * FROM (VALUES {b}) AS s 
                ({c}) ) AS src 
                ON {d} 
                WHEN MATCHED THEN 
                UPDATE SET 
                {e} 
                WHEN NOT MATCHED BY TARGET THEN 
                INSERT 
                ({f}) 
                VALUES 
                ({g}); '''.format(
                    a = endpoint['sqlTable'],
                    b = dataString,
                    c = sourceColumns,
                    d = onStatement,
                    e = matchColumns,
                    f = targetColumns,
                    g = sourceColumnsPre
                )
            ))
            success = True
        except Exception as e:
            count += 1
            print(e)
            if "Incorrect syntax" in str(e) or "would be truncated in table" in str(e):
                print("Syntax preventing merge statement")
                break
            if count == 5:
                break
            continue

    sqlConnection.commit()
    sqlConnection.close()

def createConnection(secret):
    maxRetries = 5
    retry = 1
    success = False
    while success == False:
        try:
            sqlConnection = pyodbc.connect(secret["odbcString"])
            success = True
            continue

        except Exception as e:
            error = str(e)
            retry = retry + 1
            time.sleep(600)
            continue

    return sqlConnection


def searchLoop(tableName, primaryKey, officeID, aToken, aKey): 
    #finalList = [-1]   
    finalList = []
    results = [0]   
    maxValue = 0
    while len(results) > 0:         
        data = {    
            '{}'.format(primaryKey): '{"operator":">","value": ' + str(maxValue) + '}' ##Method 2. Uncomment everything that's commented and then comment this to swap
        }     
        headers = {
        'Content-Type': 'application/json',
        }
        url = "https://moxie{}.pestroutes.com/api/{}/search?authenticationToken={}&authenticationKey={}".format(officeID,tableName,aToken,aKey)
        response = requests.get(url,headers=headers,json=data).json()
        results = response["{}".format(primaryKey)] 
        print('Result Length: '+str(len(results)))    
        finalList += results
        maxValue = finalList[-1]
    print('Final List Length: '+str(len(finalList)))
    return finalList