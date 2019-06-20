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
    VIEW_IDs = ['129283523',  #Posta
                '70760567',   #Fanatik
                '59609062']   #Hurriyet

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
    DISCOVERY_URI = ('https://analyticsreporting.googleapis.com/$discovery/rest')
    KEY_FILE_LOCATION = "./..............428f2..........p12" ##in uluc local
    #KEY_FILE_LOCATION = "D:\Galactica\Scripts\Python\h..............428f2...........p12" ##in server
    SERVICE_ACCOUNT_EMAIL = 'anal.........@................count.com'


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
    #......

def clean_response(response,map_):
    #......
# --------------------------------------------------------------------------------------------------

# --- Inserting to DB ------------------------------------------------------------------------------
class local_Dbconnection():
    #........ 
def to_DB(data,Publish_date, day_of_the_week):
    #........

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
        exit(1)      
    try: 
        response_data = response['reports'][0]['data']['rows']
    except Exception as e:
        send_r_2(e,response,start_end_date,Brand)
        exit(1)
    #print json.dumps( response , indent = 4)
    print 'veri alindi'

    data = clean_response(response,map_)
    to_DB(data,start_end_date, day_of_the_week)
    print 'veri Dbye yerlestirildi'

    
if __name__ == "__main__":
    print __doc__
    main()

