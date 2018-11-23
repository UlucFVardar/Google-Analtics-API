#! /usr/bin/env python
# -*- coding: UTF-8 -*-
# @author =__Uluç Furkan Vardar__


'''
###############################################################################################
\tGet Yesterday Publish News Pageviews Insights From Analytics. For only Hurriyet

 * simple Usage *
 python publishdate_Analysis.py               ->Runs for yesterday
 python publishdate_Analysis.py  2018-09-12   ->Runs for given day

                                                                                          -uluc
###############################################################################################
'''

# Needed Libs
from googleapiclient.discovery import build  ## sudo pip install --upgrade google-api-python-client
from oauth2client.service_account import ServiceAccountCredentials # sudo pip install --upgrade oauth2client
from datetime import timedelta
from datetime import datetime
from datetime import date
import ConfigParser
import argparse
import httplib2
import requests
import pymssql # for db connections
import time
import yaml
import copy
import json
import sys
import os

#  BRAND CHOOSE ------------------------------------------------------------------------------------

global local_DB_table 
local_DB_table = '[GoogleAnalytics].[dbo].[.....]'

# 0 means Posta
# 1 means Fanatik
# 2 means Hurriyet 

# ---- DO NOT CHANGE THESE VALUES ------------------------------------------------------------------
def configure_env_values():
    which_Brand = 2 # 1 # 2  
    VIEW_IDs = ['*******',  #Posta
                '*******',   #Fanatik
                '*******']   #Hurriyet

    FB_PAGE_NAMEs = ['posta.com.tr',    #Posta
                     'Fanatik.com.tr',  #Fanatik
                     'hurriyet.com.tr'] #Hurriyet

    Brands = ['Posta',    #Posta 
              'Fanatik',  #Fanatik
              'Hurriyet'] #Hurriyet

    hcat2_dimensions = ['ga:dimension8',  #Posta
                        'ga:dimension6',  #Fanatik
                        'ga:dimension35'] #Hurriyet

    id_dimensions = ['ga:dimension13',    #Posta
                     'ga:dimension11',    #Fanatik
                     'ga:dimension40']    #Hurriyet
    news_types = ['',
                 '',
                 'ga:dimension15']          
                
    publish_dates = ['',
                    '',
                    'ga:dimension42']               

    detail_dimensions = ['ga:dimension18',  #Posta
                         'ga:dimension16',  #Fanatik
                         'ga:dimension50']  #Hurriyet
    VIEW_ID = VIEW_IDs[which_Brand]
    FB_PAGE_NAME = FB_PAGE_NAMEs[which_Brand]
    Brand = Brands[which_Brand]
    hcat2_dimension = hcat2_dimensions[which_Brand]
    id_dimension = id_dimensions[which_Brand]
    detail_dimension = detail_dimensions[which_Brand]
    news_type = news_types[which_Brand]
    publish_date_dimension = publish_dates[which_Brand]
    return VIEW_ID,FB_PAGE_NAME,Brand,hcat2_dimension,id_dimension,detail_dimension,news_type,publish_date_dimension
# --------------------------------------------------------------------------------------------------


# Initializes an analyticsreporting service object. ------------------------------------------------
def initialize_analyticsreporting():
    """Initializes an analyticsreporting service object.

    Returns:
    analytics an authorized analyticsreporting service object.
    """
    SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
    DISCOVERY_URI = ('https://analytics*************************overy/rest')
    KEY_FILE_LOCATION = "---YOUR KEY---"#filepath
    SERVICE_ACCOUNT_EMAIL = '---YOUR MAIL---'


    credentials = ServiceAccountCredentials.from_p12_keyfile( SERVICE_ACCOUNT_EMAIL, KEY_FILE_LOCATION, scopes=SCOPES )
    http = credentials.authorize(httplib2.Http())
    # Build the service object.
    analytics = build('analytics', 'v4', http=http, discoveryServiceUrl=DISCOVERY_URI)
    return analytics
# --------------------------------------------------------------------------------------------------

# Use the Analytics Service Object to query the Analytics Reporting API V4. ------------------------
def get_report(analytics,body_):
    return analytics.reports().batchGet(body=body_).execute()
# --------------------------------------------------------------------------------------------------


# --------------------------------------------------------------------------------------------------
def generate_start_end_dates():
    if len(sys.argv) == 1 : # mean no argument  
        today_time = datetime.today()
        a = str((today_time-timedelta(days=1)).strftime("%Y-%m-%d")),str((today_time-timedelta(days=1)).strftime("%Y%m%d")),str((today_time-timedelta(days=1)).weekday()+1)
    else:
        workingdate = sys.argv[1]
        datetime_object = datetime.strptime(workingdate, '%Y-%m-%d')
        a =str(sys.argv[1]), str((datetime_object).strftime("%Y%m%d")),str(datetime_object.weekday()+1)
    # 2018-11-22.  20181122. 4
    return a
def create_custom_request(VIEW_ID,start_end_date,date_for_publish,news_type,id_dimension,publish_date_dimension):
    body={
      "reportRequests": [
        {
          "pageSize" : 30000,
          "viewId": VIEW_ID ,
          "filtersExpression": publish_date_dimension +'=='+date_for_publish+';'+news_type+'=~gazete,'+news_type+'==haber',
          "dateRanges": [
            {
              "startDate": start_end_date,
              "endDate": start_end_date
            }
          ],
         "metrics": [
           {
             "expression": "ga:pageviews"
           }
         ],
         'orderBys': {
           'fieldName': 'ga:pageviews',
           'orderType': 'VALUE',
           'sortOrder': 'DESCENDING',
         },
         "dimensions": [
           {
             "name": id_dimension
           }
         ]
       }
     ]
    }
    # metrics and dimensions mapping 
    map_ = { id_dimension: 'Article_id' ,
             'ga:pageviews' : 'PageView' }

    return body,map_
# --------------------------------------------------------------------------------------------------


# ---Cleaning --------------------------------------------------------------------------------
class reportObject():
    def __init__(self,report,dimensions,metrics):
        a = {}
        for value,metric in zip (report['metrics'][0]['values'], metrics ):
            a[metric] = value
        for value,dimension in zip (report['dimensions'], dimensions ):
            a[dimension] = value
        self.info = a
    def get_json_info(self):
        return json.dumps( self.info ,indent = 4)

def clean_response(response,map_):
    temp_dimensions = response['reports'][0]['columnHeader']['dimensions']
    temp_metric = response['reports'][0]['columnHeader']['metricHeader']['metricHeaderEntries']
    metrics = []
    for m in temp_metric:
        try :
            metrics.append(map_[m['name']])
        except Exception as e:
            metrics.append(m['name'])
    dimensions = []
    for d in temp_dimensions:
        try :
            dimensions.append(map_[d])
        except Exception as e:
            dimensions.append(d)            
    # --- 
    rows = response['reports'][0]['data']['rows']
    results = []
    for data in rows:
        try:
            r_o =reportObject(data,dimensions,metrics)
            results.append(r_o)
            #print r_o.get_json_info()
            #break
        except Exception as e:
            #print e
            pass
    return results

# --------------------------------------------------------------------------------------------------

# --- Inserting to DB ------------------------------------------------------------------------------
class local_Dbconnection():
    def __init__(self,interested_DB_table):
        self.interested_DB_table = interested_DB_table
        self.cnxn = pymssql.connect( 
                server="*************", 
                user="***********", 
                password="*********", 
                database="*************", 
                host="***************",
                )
        self.cursor = self.cnxn.cursor()
    def get_select(self,line,Publish_date,day_of_the_week):
        select_sql = ('SELECT * FROM {} '
                      "WHERE Publish_date = '{}' AND "
                            "day_of_the_week = '{}' AND "
                            "Article_id = {} AND "
                            "PageView = {} AND ".format(self.interested_DB_table,
                                                            Publish_date,
                                                            day_of_the_week,
                                                            line.info['Article_id'],
                                                            line.info['PageView'] ))
        return select_sql
    def get_insert_sql(self,line,Publish_date,day_of_the_week):
        insert_sql = (  'INSERT INTO {} '
                        '(Publish_date, day_of_the_week, Article_id, PageView,InsertTime) '
                        "VALUES('{}', '{}' , '{}', {}, GETDATE())".format( self.interested_DB_table,
                                                                                        Publish_date,
                                                                                        day_of_the_week,
                                                                                        line.info['Article_id'],
                                                                                        line.info['PageView'] ))
        return insert_sql
    
    def check_is_in_db(self,select_sql):
        try:
            self.cursor.execute(select_sql)
            dbValue = self.cursor.fetchone()
            if dbValue == None:
                return False # false mean insert to  DB
            print 'in DB'
            return True
        except Exception as e:
            print e,'---Error when checking DB'
    def insert_2_db(self,insert_sql):
        try:
            self.cursor.execute(insert_sql)
            self.cnxn.commit()
        except Exception as e:
            print e,'---Error when insertting to DB'
def to_DB(data,Publish_date, day_of_the_week):
    global local_DB_table 
    DB_Conn = local_Dbconnection(local_DB_table)
    for d in data:
        try:
            temp_select_sql = DB_Conn.get_insert_sql(d,Publish_date,day_of_the_week)
            print temp_select_sql
            
            flag = DB_Conn.check_is_in_db(temp_select_sql)
            if flag == False:
                temp_insert_sql = DB_Conn.get_insert_sql(d,Publish_date,day_of_the_week)
                flag2 = DB_Conn.insert_2_db(temp_insert_sql)
            
        except Exception as e:
            #print e
            pass

# --------------------------------------------------------------------------------------------------


# --- Reportting  ------------------------------------------------------------------------------
def sendTelegramMess(header):
    api ='************************' ## a custom api for send Telegram messaje to my chats when there some errors. (Telegram API is used in my custom API)
    r = requests.post( api, data = json.dumps( header))
    return r.json()

def send_r_2(e,response,start_end_date,Brand):
    text = str('\n---Error returned data is diffrent\n\nException:\n')+str(e).replace('<','_').replace('\">','_')+'\n\n'+json.dumps(response,indent = 2)[:400]
    sub = str("From: ")+str(Brand)+str(" Daily Paper Publish News ")+str(start_end_date)
    a =  sendTelegramMess(header  = {
                          "mailto": "---your_chat_id----",
                          "sub": sub,
                          "text": text })
    print text   
def send_r_1(e,response,start_end_date,Brand):
    text = str('\n----Error when try to get report from Google Analystics\n\nException:\n')+str(e).replace('<','_').replace('\">','_')
    sub = str("From: ")+str(Brand)+str(" Daily Paper Publish News ")+str(start_end_date)
    a =  sendTelegramMess(header  = {
                          "mailto": "---your_chat_id----",
                          "sub": sub,
                          "text": text })
    print text       
# --------------------------------------------------------------------------------------------------


def main():
    VIEW_ID,FB_PAGE_NAME,Brand,hcat2_dimension,id_dimension,detail_dimension,news_type,publish_date_dimension = configure_env_values()
    analytics = initialize_analyticsreporting()
    start_end_date, date_for_publish, day_of_the_week = generate_start_end_dates()
    body_,map_ = create_custom_request(VIEW_ID,start_end_date,date_for_publish,news_type,id_dimension,publish_date_dimension)
    print start_end_date, date_for_publish
    try : 
        response = get_report(analytics,body_)
    except Exception as e:
        send_r_1(e,response,start_end_date,Brand)
        exit()      
    try: 
        response_data = response['reports'][0]['data']['rows']
    except Exception as e:
        send_r_2(e,response,start_end_date,Brand)
        exit()
    #print json.dumps( response , indent = 4)
    print 'veri alindi'

    data = clean_response(response,map_)
    to_DB(data,start_end_date, day_of_the_week)
    print 'veri Dbye yerlestirildi'

    
if __name__ == "__main__":
    print __doc__
    main()

