import json
import logging
import os
import urllib.parse
from datetime import datetime, timedelta
from multiprocessing import Process

import azure.functions as func
import pandas
import pyodbc
import requests
from dateutil.relativedelta import relativedelta
from pytz import timezone

import loadPestroutesData as loadData
import updatedData as updatedData

from variables import table


def main():
    print("Running ETL Process")
    date = datetime.strftime(datetime.now(),'%Y-%m-%d 00:00:00')
    date2 = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S')
    date = datetime.strptime(str(date),'%Y-%m-%d %H:%M:%S') - relativedelta(days=2)
    date2 = datetime.strptime(str(date2),'%Y-%m-%d %H:%M:%S') 
    
    endpoints = ["Contract","Appointment","AppointmentReminder","Document","Customer","Employee","Office","Payment","ServiceType","Subscription","Lead","Ticket","Form","Note","GenericFlag","GenericFlagAssignment","PaymentProfile","Product","Region","Review","Route","Task","PaymentApplication","Team","Knock","Chemical","ChemicalUse","Diagram","Disbursement","DisbursementItem","Group","Insect","Spot","Changelog"]

    kvBranches = "API KEYS"
    curDate = datetime.strftime(date2,'%Y-%m-%d')
    difference = ( pandas.to_datetime('2024-01-01') - pandas.to_datetime(curDate) )
    diffSec = difference.total_seconds()//3600
    while diffSec < 0:
        dtobj = datetime.strptime(str(date), '%Y-%m-%d %H:%M:%S')
        dtobj2 = datetime.strptime(str(date2), '%Y-%m-%d %H:%M:%S')
        dateFormat = datetime.strftime(dtobj,'%Y-%m-%d %H:%M:%S')
        dateFormat2 = datetime.strftime(dtobj2,'%Y-%m-%d %H:%M:%S')
        dateEncoded = urllib.parse.quote('{"operator":"BETWEEN","value":["'+dateFormat+'","'+dateFormat2+'"]}')
        print(' '+str(dateFormat)+' '+str(dateFormat2)+'  '+str(diffSec)+'   ')
        # print(dateEncoded)
        for item in endpoints:
            endpointData = table.tables[item]
            
            # with open('table.txt', 'w') as f:
            #     f.write(str(item))

            for branch in kvBranches:
                loadData.getIds(kvBranches[branch],endpointData,dateEncoded,item)
                updatedData.getIds(kvBranches[branch],endpointData,dateEncoded,item)
                # changelogUpdate.getIds(kvBranches[branch],dateEncoded)


        date = datetime.strptime(str(date),'%Y-%m-%d %H:%M:%S') - relativedelta(days=2)
        date2 = datetime.strptime(str(date2),'%Y-%m-%d %H:%M:%S') - relativedelta(days=2)
        with open('date1.txt', 'w') as f:
            f.write(str(date))
        with open('date2.txt', 'w') as f:
            f.write(str(date2))
        difference = ( pandas.to_datetime('2024-01-01') - pandas.to_datetime(date2) )
        diffSec = difference.total_seconds()//3600
        
main()
