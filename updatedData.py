import json
import logging
import os
import tempfile
import time
import urllib.parse
from datetime import datetime, timedelta
import pytz  # Add this import for timezone handling

import pyodbc
import requests

from variables import table


def convert_pacific_to_branch_timezone(datetime_str, branch_timezone):
    """
    Convert datetime from Pacific timezone to branch-specific timezone
    
    Args:
        datetime_str: String datetime in format "YYYY-MM-DD" or "YYYY-MM-DD HH:MM:SS"
        branch_timezone: Target timezone string (e.g., "America/New_York")
    
    Returns:
        Converted datetime string in the same format as input
    """
    if not datetime_str or datetime_str in [None, '0000-00-00', '0000-00-00 00:00:00', 'None']:
        return datetime_str
    
    try:
        # Define Pacific timezone
        pacific_tz = pytz.timezone('America/Los_Angeles')
        
        # Define target timezone
        target_tz = pytz.timezone(branch_timezone)
        
        # Parse the datetime string
        if len(datetime_str) == 10:  # Date only: "YYYY-MM-DD"
            dt = datetime.strptime(datetime_str, "%Y-%m-%d")
            # For date-only, assume midnight Pacific time
            dt_pacific = pacific_tz.localize(dt)
            dt_target = dt_pacific.astimezone(target_tz)
            return dt_target.strftime("%Y-%m-%d")
        elif len(datetime_str) == 19:  # Datetime: "YYYY-MM-DD HH:MM:SS"
            dt = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")
            # Localize to Pacific timezone
            dt_pacific = pacific_tz.localize(dt)
            # Convert to target timezone
            dt_target = dt_pacific.astimezone(target_tz)
            return dt_target.strftime("%Y-%m-%d %H:%M:%S")
        else:
            # If format is unexpected, return original
            return datetime_str
            
    except Exception as e:
        print(f"Error converting timezone for {datetime_str}: {e}")
        return datetime_str


def getIds(branch,endpoint,dateEncoded,item):
    # Gathers the necessary api keys and secret data needed to create connections from data sources/destinations.
    secret = {
        "odbcString" : "Driver={ODBC Driver 18 for SQL Server};Server=tcp:moxiereportingserver.database.windows.net,1433;Database=StagingDB;Encrypt=yes;Uid=powerbiadmin;Pwd=KJXiET15TuDp;TrustServerCertificate=no;Connection Timeout=30;",        
        "authenticationToken" : branch["authenticationToken"],
        "authenticationKey" : branch["authenticationKey"],
        "officeID" : branch["officeID"],
        "timeZone" : branch.get("timeZone", "America/Los_Angeles")  # Add timezone to secret, default to Pacific
    }
    if 'dateUpdate' in endpoint:
        idUrl = endpoint["dateUpdate"]
    else: 
        print("No dateUpdate endpoint found")
        return

    finalList = []
    results = [0]   
    maxValue = 0
    while len(results) > 0:         
        data = {    
            '{}'.format(endpoint['queryPlural']): '{"operator":">","value": ' + str(maxValue) + '}' 
        }     
        headers = {
        'Content-Type': 'application/json',
        }
        
        # Creates the url for extracting the bulk data of the ID's sent from the previous function
        statusEncoded = urllib.parse.quote('{"operator":"IN","value":["-3","-2","-1","0","1","2"]}')
        # Checks which class the function is currently loading. Certain classes require statuses to be specified in order for the post request to gather all data from the endpoint.
        if endpoint["dateBool"] is True:
            if item == "Appointment":
                url = idUrl.format(secret["officeID"],secret["authenticationKey"],secret["authenticationToken"],dateEncoded,urllib.parse.quote('{"operator":"IN","value":["-2","-1","0","1","2"]}'))
            elif item == "Payment":
                url = idUrl.format(secret["officeID"],secret["authenticationKey"],secret["authenticationToken"],dateEncoded,urllib.parse.quote('{"operator":"IN","value":["0","1","2"]}'))
            elif item == "Ticket":
                url = idUrl.format(secret["officeID"],secret["authenticationKey"],secret["authenticationToken"],dateEncoded,urllib.parse.quote('{"operator":"IN","value":["0","1"]}'))
            elif item == "Subscription":
                url = idUrl.format(secret["officeID"],secret["authenticationKey"],secret["authenticationToken"],dateEncoded,urllib.parse.quote('{"operator":"IN","value":["0","1"]}'))
            elif item == "Lead":
                url = idUrl.format(secret["officeID"],secret["authenticationKey"],secret["authenticationToken"],dateEncoded,'&active={"operator":"IN", "value":[-3]}')
            elif item == "Customer":
                url = idUrl.format(secret["officeID"],secret["authenticationKey"],secret["authenticationToken"],dateEncoded,urllib.parse.quote('{"operator":"IN","value":["0","1"]}'))
            elif item == "Door":
                url = idUrl.format(secret["officeID"],secret["authenticationKey"],secret["authenticationToken"],dateEncoded,urllib.parse.quote('{"operator":"IN","value":["-2","-1","0","1","2"]}'))
            elif item == "ServicePlan":
                url = idUrl.format(secret["officeID"],secret["authenticationKey"],secret["authenticationToken"],dateEncoded,urllib.parse.quote('{"operator":"IN","value":["-2","-1","0","1","2"]}'))
            elif item == "Task":
                url = idUrl.format(secret["officeID"],secret["authenticationKey"],secret["authenticationToken"],dateEncoded,urllib.parse.quote('{"operator":"IN","value":["0","1","2","3"]}'),urllib.parse.quote('{"operator":"IN","value":["0","1"]}'))
            else:
                url = idUrl.format(secret["officeID"],secret["authenticationKey"],secret["authenticationToken"],dateEncoded)
        elif item == "Employee":
            url = idUrl.format(secret["officeID"],secret["authenticationKey"],secret["authenticationToken"],dateEncoded,urllib.parse.quote('{"operator":"IN","value":["0","1"]}'))    
        else:
            return
        
        response = requests.get(url,headers=headers,json=data)
        
        if response.status_code != 200:
            response = requests.get(url,headers=headers,json=data)
            response = response.json()
        else:
            response = response.json()
            
        results = response[endpoint['queryPlural']] 
        if len(results) > 0:
            logging.info('Result Length: '+str(len(results)))    
        finalList += results
        
        if len(finalList) < 1:
            logging.info('No {} records for office {}'.format(endpoint['urlEndpoint'],secret['officeID']))
            return finalList
        
        
        maxValue = finalList[-1]

    total = len(finalList)
    
    iterateIds(finalList,secret,endpoint,item)


def iterateIds(ids,secret,endpoint,item):
    # Logs the total of ID's to load for a specific class and branch
    length = len(ids)
    print((""+ str(length) + " total {}'s to dateUpdate in officeID: " + str(secret["officeID"]) +"").format(item))
    # Creates the connection to the SQL Server and queries for all ID's for that class to compare whether the ID's we are requesting from PR already exists in the database

    # While loop to iterate through the ID's by 1000 as it is the maximum amount you can bulk request from the api.
    while length > 1000:
        processedIDs = ids[0:999]
        ids = ids[999:]
        # Sends the next 1000 ID's to another function for the bulk load request.
        data = getBulkData(processedIDs,secret,endpoint)
        # After getting the bulk data for the 1000 ID's, if the class is payment, it gets sent to another function to specifically transform some of that data to be suitable for the database.
        # Otherwise it is inserted into the database as the raw data.
        status = insertData(data,secret,endpoint,processedIDs)
        # Reset the total ID's to the total after subtracting the data that has just been loaded.
        length = len(ids)
    # Continutes the bulk loading of data after the total ID's left to request is less than 1000
    if length > 0:
        data = getBulkData(ids,secret,endpoint)
        status = insertData(data,secret,endpoint,ids)


def getBulkData(ids,secret,endpoint):
    # Creates the url for extracting the bulk data of the ID's sent from the previous function
    bulkUrl = endpoint["bulkUrl"]
    url = bulkUrl.format(secret["officeID"],secret["authenticationKey"],secret["authenticationToken"])
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
    schemaTable = endpoint['sqlTable'].split('.')
    schema = schemaTable[0]
    table = schemaTable[1]
    columnQuery = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{}' AND TABLE_SCHEMA = '{}'".format(table,schema)
    status = cursor.execute(columnQuery)
    for row in status:
        dbColumns.append(str(row[0]))
        
    nonDBColumns = []
    prColumns = list(set().union(*(d.keys() for d in data)))
    
    for column in prColumns:
        if column not in dbColumns:
            nonDBColumns.append(str(column))
            
            data = [{k: v for k, v in d.items() if k != column} for d in data]

    # Get the branch timezone for conversion
    branch_timezone = secret.get("timeZone", "America/Los_Angeles")

    for row in data:
        for key in row:
            if endpoint['sqlTable'] == 'Subscription':
                if key == 'cancellationNotes' and len(row[key]) > 0:
                    reason = row[key]
                    reason = reason[0]
                    reason = reason['cancellationReason']
                    row[key] = reason
            if "key" in ('batchClosed','batchOpened','checkIn','checkOut','created','date','dateAdded','dateApplied','dateCancelled','dateChanged','dateCompleted','dateCreated','dateSent','dateSigned','dateUpdated','deleted','dueDate','emailSent','invoiceDate','lastAttemptDate','lastLogin','lastUpdated','leadDateAdded','leadDateAssigned','leadUpdated','paymentHoldDate','responseTime','sentFailureDate','timeCreated','voiceSent'):
                if row[key] == None or row[key] == '0000-00-00' or row[key] == '0000-00-00 00:00:00' or row[key] == 'None':
                    continue

                try:
                    # Convert datetime from Pacific to branch timezone
                    original_datetime = row[key]
                    converted_datetime = convert_pacific_to_branch_timezone(original_datetime, branch_timezone)
                    row[key] = converted_datetime
                    
                    # Validate the converted datetime
                    if len(converted_datetime) == 10:
                        checkDate = datetime.strptime(converted_datetime, "%Y-%m-%d")
                    elif len(converted_datetime) == 19:
                        checkDate = datetime.strptime(converted_datetime, "%Y-%m-%d %H:%M:%S")
                    
                except Exception as e:
                    logging.info(f"Error processing datetime field {key}: {e}")
                    logging.info(f"Original value: {row[key]}")
                    logging.info("Setting to default date")
                    row[key] = "2000-01-01 00:00:00"

    data = list({i[endpoint['sqlPK']]:i for i in reversed(data)}.values())

    nestedList = [[key for key in data[0].keys()], *[list(idx.values()) for idx in data ]]
    status = mergeData(nestedList,endpoint,secret)

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
    url = bulkUrl.format(secret["officeID"],secret["authenticationKey"],secret["authenticationToken"])
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
    
def mergeData(data,endpoint,secret):
    # Create sql connection
    sqlConnection = pyodbc.connect(secret["odbcString"])
    cursor = sqlConnection.cursor()
    cursor.execute('SET QUOTED_IDENTIFIER OFF')
    nestedList = data
    columns = nestedList.pop(0)
    newNestedList = []
    
    for listdata in nestedList:
        newList = []
        
        for element in listdata:
            if type(element) == str and len(element) > 0:
                element = str(element)
                if endpoint["sqlTable"] in ('PR.Appointment','PR.Subscription','PR.Lead','PR.Note','PR.Task','PR.Changelog','PR.Review','PR.Route','PR.Customer','PR.Insect','PR.Diagram'):
                    element = element.replace("'","''")
                element = element.replace("\\","")
                element = element.replace('\r','')
                element = element.replace('\n','')
            elif element != None and (str(element) == "0000-00-00 00:00:00" or len(str(element)) < 1 or element == "0000-00-00" or element == "00:00:00"):
                element = None
            elif type(element) == list:
                if len(element) < 1:
                    element = None
                element = str(element)
                if endpoint["sqlTable"] in ('PR.Appointment','PR.Subscription','PR.Lead','PR.Note','PR.Task','PR.Changelog','PR.Review','PR.Route','PR.Customer','PR.Insect','PR.Diagram'):
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
                if endpoint["sqlTable"] in ('PR.Appointment','PR.Subscription','PR.Lead','PR.Note','PR.Task','PR.Changelog','PR.Review','PR.Route','PR.Customer','PR.Insect','PR.Diagram'):
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
                if endpoint["sqlTable"] in ('PR.Appointment','PR.Subscription','PR.Lead','PR.Note','PR.Task','PR.Changelog','PR.Review','PR.Route','PR.Customer','PR.Insect','PR.Diagram'):
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
    sample = '''MERGE INTO [{a}] AS tgt
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
    success = False
    count = 0
    
    while success != True:
        try:
            # print("Executing SQL Statement")
            status = cursor.execute(('''MERGE INTO {a} AS tgt
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
    finalList = []
    results = [0]   
    maxValue = 0
    while len(results) > 0:         
        data = {    
            '{}'.format(primaryKey): '{"operator":">","value": ' + str(maxValue) + '}'
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