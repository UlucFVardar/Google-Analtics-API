#! /usr/bin/env python
# -*- coding: UTF-8 -*-
# @author =__Uluç Furkan Vardar__


'''
###############################################################################################
\tGet Weekly Insights From Analytics.

 * simple Usage *
 python device_analysis.py                          ->Runs for last finished week
 python device_analysis.py  2018-11-19 2018-11-25   ->Runs for given week

                                                                                          -uluc
###############################################################################################
'''

# Needed Libs
from googleapiclient.discovery import build  ## sudo pip install --upgrade google-api-python-client
from oauth2client.service_account import ServiceAccountCredentials # sudo pip install --upgrade oauth2client
from collections import OrderedDict
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
global platform 
global period 

local_DB_table = '[GoogleAnalytics].[dbo].[.......]'
platform = 'Web'
period = 'Weekly'
# 0 means Posta
# 1 means Fanatik
# 2 means Hurriyet 

# ---- DO NOT CHANGE THESE VALUES ------------------------------------------------------------------
def configure_env_values():
    which_Brand = 2 # 1 # 2  
    VIEW_IDs = ['********',  #Posta
                '********',   #Fanatik
                '********']   #Hurriyet

    FB_PAGE_NAMEs = ['posta.com.tr',    #Posta
                     'Fanatik.com.tr',  #Fanatik
                     'hurriyet.com.tr'] #Hurriyet

    Brands = ['Posta',    #Posta 
              'Fanatik',  #Fanatik
              'Hurriyet'] #Hurriyet

    cat2_dimensions = ['ga:dimension8',  #Posta
                        'ga:dimension6',  #Fanatik
                        'ga:dimension35'] #Hurriyet

    id_dimensions = ['ga:dimension13',    #Posta
                     'ga:dimension11',    #Fanatik
                     'ga:dimension40']    #Hurriyet
    news_types = ['',
                  'ga:dimension4',
                 'ga:dimension15']          
                
    publish_dates = ['',
                    '',
                    'ga:dimension42']               

    detail_dimensions = ['ga:dimension18',  #Posta
                         'ga:dimension16',  #Fanatik
                         'ga:dimension50']  #Hurriyet

    cat3_dimensions = ['',
                       'ga:dimension8',
                       'ga:dimension46'] #                          
    VIEW_ID = VIEW_IDs[which_Brand]
    FB_PAGE_NAME = FB_PAGE_NAMEs[which_Brand]
    Brand = Brands[which_Brand]
    hcat2_dimension = cat2_dimensions[which_Brand]
    id_dimension = id_dimensions[which_Brand]
    detail_dimension = detail_dimensions[which_Brand]
    news_type = news_types[which_Brand]
    publish_date_dimension = publish_dates[which_Brand]
    cat3_dimension = cat3_dimensions[which_Brand]
    return VIEW_ID,FB_PAGE_NAME,Brand,hcat2_dimension,id_dimension,detail_dimension,news_type,publish_date_dimension,cat3_dimension
# --------------------------------------------------------------------------------------------------


# Initializes an analyticsreporting service object. ------------------------------------------------
def initialize_analyticsreporting():
    """Initializes an analyticsreporting service object.

    Returns:
    analytics an authorized analyticsreporting service object.
    """
    SCOPES = ['................................']
    DISCOVERY_URI = ('..........................')
    #KEY_FILE_LOCATION = "./......................p12" ##in uluc local
    SERVICE_ACCOUNT_EMAIL = '.........................count.com'


    credentials = ServiceAccountCredentials.from_p12_keyfile( SERVICE_ACCOUNT_EMAIL, KEY_FILE_LOCATION, scopes=SCOPES )
    http = credentials.authorize(httplib2.Http())
    # Build the service object.
    analytics = build('analytics', 'v4', http=http, discoveryServiceUrl=DISCOVERY_URI)
    return analytics
# --------------------------------------------------------------------------------------------------

# Use the Analytics Service Object to query the Analytics Reporting API V4. ------------------------
def get_report(analytics,body_):
    try:
        return analytics.reports().batchGet(body=body_).execute()
    except Exception as e:
        print e
# --------------------------------------------------------------------------------------------------


# -- Custom Request --------------------------------------------------------------------------------
def generate_start_end_dates():
    if len(sys.argv) == 1 : # mean no argument  
        today_time = datetime.today()
        first_day_of_the_week = (today_time-timedelta(days=today_time.weekday()))
        start_date = str((first_day_of_the_week - timedelta(days = 7)).strftime("%Y-%m-%d"))
        end_date = str((first_day_of_the_week - timedelta(days = 1)).strftime("%Y-%m-%d"))
    else:
        start_date = str(sys.argv[1])
        end_date = str(sys.argv[2])
    # 2018-11-19 2018-11-25 
    return start_date,end_date
def give_static_part_of_requests(VIEW_ID,BeginDate,EndDate,news_type,hcat2_dimension):
    static_part = {
                "reportRequests": [{
                     "pageSize" : 30000,
                     "viewId": VIEW_ID ,
                     "dateRanges": [{"startDate": BeginDate, "endDate": EndDate}],
                     "dimensions": [],
                     "metrics": [{'expression': 'ga:pageviews'}, 
                                {'expression': 'ga:users'},
                                {'expression': 'ga:sessions'},
                                {'expression': 'ga:avgSessionDuration'}
                                ],
                     'orderBys': {
                     'fieldName': 'ga:pageviews',
                     'orderType': 'VALUE',
                     'sortOrder': 'DESCENDING',
                   },
                   
                 }
               ]
    } 
    # static metrics and dimensions mapping 
    global platform 
    global period 
    map_ = { 
              'ga:pageviews'          : 'Page_View',
              'ga:users'              : 'Unique_User',
              'ga:sessions'           : 'Session_Count',
              'ga:avgSessionDuration' : 'Average_Time_On_Page',
               news_type              : {'Category': 'Total' }, 
               hcat2_dimension        : {'NewsCategory': 'Total' },
              'End_Of_Article'        : {'End_Of_Article': '0'},
              'End_Of_Page'           : {'End_Of_Page': '0'},
              'Scroll'                : {'Scroll': '0'},
              'Article_Count'         : {'Article_Count': '0'},
              'BeginDate'             : {'BeginDate': BeginDate },
              'EndDate'               : {'EndDate': EndDate },
              'Period'                : {'Period': period },
              'Platform'              : {'Platform': platform },
              'ga:deviceCategory'     : {'Device': 'Total'},              

 }        
    return static_part, map_
def create_custom_requests(VIEW_ID,BeginDate,EndDate,news_type,cat3_dimension,hcat2_dimension):
    query_params = list()
    map_s = list()
    static_part, map_ = give_static_part_of_requests(VIEW_ID,BeginDate,EndDate,news_type,hcat2_dimension)
    #----------0
    temp = copy.deepcopy(static_part)
    temp_map = copy.deepcopy(map_)
    query_params.append(temp)   
    map_s.append(temp_map)

    #----------1     
    temp = copy.deepcopy(static_part)
    temp_map = copy.deepcopy(map_)
    temp['reportRequests'][0]['dimensions'] = [
           {"name": "ga:deviceCategory" }
         ]        
    temp_map['ga:deviceCategory'] = 'Device'                           
    query_params.append(temp)
    map_s.append(temp_map)
    #----------2
    temp = copy.deepcopy(static_part)
    temp_map = copy.deepcopy(map_)
    temp['reportRequests'][0]['dimensions'] = [
           {"name": "ga:deviceCategory" },
           {'name': hcat2_dimension}
         ]
    temp_map['ga:deviceCategory'] = 'Device'                           
    temp_map[hcat2_dimension] = 'NewsCategory'

    query_params.append(temp)
    map_s.append(temp_map)
    #----------3
    temp = copy.deepcopy(static_part)
    temp_map = copy.deepcopy(map_)   
    temp['reportRequests'][0]['dimensions'] = [
           {"name": "ga:deviceCategory" }
         ]
    temp_map['ga:deviceCategory'] = 'Device'                           
    temp['reportRequests'][0]['filtersExpression'] = news_type+"==newsgaleri,"+news_type+"==galeri"
    temp_map[news_type] = {'Category': 'Gallery'} 
    query_params.append(temp)
    map_s.append(temp_map)     
    #----------4
    temp = copy.deepcopy(static_part)
    temp_map = copy.deepcopy(map_)    
    temp['reportRequests'][0]['dimensions'] = [
           {"name": "ga:deviceCategory" },
           {"name": hcat2_dimension }
         ]
    temp_map['ga:deviceCategory'] = 'Device'                           
    temp_map[hcat2_dimension] = 'NewsCategory'                  
    temp['reportRequests'][0]['filtersExpression'] = news_type+"==newsgaleri,"+news_type+"==galeri"
    temp_map[news_type] = {'Category': 'Gallery'} 
    query_params.append(temp)
    map_s.append(temp_map)
    #----------5
    temp = copy.deepcopy(static_part)
    temp_map = copy.deepcopy(map_)    
    temp['reportRequests'][0]['dimensions'] = [
           {"name": "ga:deviceCategory" }
         ]
    temp_map['ga:deviceCategory'] = 'Device'                           
    temp['reportRequests'][0]['filtersExpression'] = news_type+"==haber,"+news_type+"==seo,"+news_type+"==seo-content-haber"
    temp_map[news_type] = {'Category': 'Article'} 
    query_params.append(temp)
    map_s.append(temp_map)
    #----------6
    temp = copy.deepcopy(static_part)
    temp_map = copy.deepcopy(map_)
    temp['reportRequests'][0]['dimensions'] = [
           {"name": "ga:deviceCategory" },
           {"name": hcat2_dimension }
         ]
    temp_map['ga:deviceCategory'] = 'Device'                  
    temp_map[hcat2_dimension] = 'NewsCategory'         
    temp['reportRequests'][0]['filtersExpression'] = news_type+"==haber,"+news_type+"==seo,"+news_type+"==seo-content-haber"
    temp_map[news_type] = {'Category': 'Article'} 
    query_params.append(temp)
    map_s.append(temp_map)
    #----------7  
    temp = copy.deepcopy(static_part)
    temp_map = copy.deepcopy(map_)
    temp['reportRequests'][0]['dimensions'] = [
            {"name": hcat2_dimension }
         ]    
    temp_map[hcat2_dimension] = 'NewsCategory'                      
    query_params.append(temp)
    map_s.append(temp_map)
    
    #----------8
    temp = copy.deepcopy(static_part)
    temp_map = copy.deepcopy(map_)
    temp['reportRequests'][0]['dimensions'] = [
            {"name": news_type }
         ]    
    temp_map[news_type] = 'Category'                      
    query_params.append(temp)
    map_s.append(temp_map)   

    return query_params,map_s
# --------------------------------------------------------------------------------------------------


# ---Cleaning --------------------------------------------------------------------------------
class reportObject():
    def __init__(self,report,dimensions,metrics,map_):
        a = OrderedDict()
        for key in map_:
            try:
                staticv = map_[key].keys()
                a[staticv[0]] =  map_[key][staticv[0]]
            except Exception as e:
                pass
        try:
            for value,metric in zip (report['metrics'][0]['values'], metrics ):
                a[metric] = value
        except Exception as e:
            pass
        try:            
            for value,dimension in zip (report['dimensions'], dimensions ):
                a[dimension] = value
        except Exception as e:
            pass
        try:
            metric = a['Average_Time_On_Page']
            metric = int(metric.split('.', 1)[0])
            a['Average_Time_On_Page'] = str(timedelta(seconds=metric))
        except Exception as e:
            pass

        self.info = a
    def get_json_info(self):
        return json.dumps( self.info ,indent = 4)

def clean_response(response,map_):
    dimensions = []
    metrics = []
    try:
        temp_metric = response['reports'][0]['columnHeader']['metricHeader']['metricHeaderEntries']
        for m in temp_metric:
            try :
                metrics.append(map_[m['name']])
            except Exception as e:
                metrics.append(m['name'])
    except Exception as e:
        pass
    try:
        temp_dimensions = response['reports'][0]['columnHeader']['dimensions']
        for d in temp_dimensions:
            try :
                dimensions.append(map_[d])
            except Exception as e:
                dimensions.append(d)      
    except Exception as e:
        pass
    # --- 
    rows = response['reports'][0]['data']['rows']
    results = []
    for data in rows:
        try:
            r_o =reportObject(data,dimensions,metrics,map_)
            results.append(r_o)
            #print r_o.get_json_info()
            #break
        except Exception as e:
            #print e
            pass
    return results
# --------------------------------------------------------------------------------------------------
# --- Reportting  ------------------------------------------------------------------------------
def sendTelegramMess(header):
    api ='https://7oic6nrbvb.execute-api.us-east-2.amazonaws.com/sapsik/'
    r = requests.post( api, data = json.dumps( header))
    return r.json()

def send_r_2(e,response,start_end_date,Brand,i):
    text = str('\n---Error returned data is diffrent for request '+str(i)+'\n\nException:\n')+str(e).replace('<','_').replace('\">','_')+'\n\n'+json.dumps(response,indent = 2)[:400]
    sub = str("From: ")+str(Brand)+str(" Weekly Device ")+str(start_end_date)
    a =  sendTelegramMess(header  = {
                          "mailto": "-292627378",
                          "sub": sub,
                          "text": text })
    print text   
def send_r_1(e,response,start_end_date,Brand,i):
    text = str('\n----Error when try to get report from Google Analystics for request '+str(i)+'\n\nException:\n')+str(e).replace('<','_').replace('\">','_')
    sub = str("From: ")+str(Brand)+str(" Weekly Device ")+str(start_end_date)
    a =  sendTelegramMess(header  = {
                          "mailto": "-292627378",
                          "sub": sub,
                          "text": text })
    print text       
# --------------------------------------------------------------------------------------------------

# --- Inserting to DB ------------------------------------------------------------------------------
class local_Dbconnection():
    def __init__(self,interested_DB_table):
        self.interested_DB_table = interested_DB_table
        self.cnxn = pymssql.connect( 
                server="...........", 
                user=".............", 
                password="...........", 
                database="GoogleAnalytics", 
                host="...............",
                )
        self.cursor = self.cnxn.cursor()
    def get_select(self,d):
        sqlDict = d.info
        select_sql = ('SELECT * FROM '+self.interested_DB_table+' '
                      'WHERE [BeginDate] =\''+sqlDict['BeginDate']+'\' AND '
                            '[EndDate] =\''+sqlDict['EndDate']+'\' AND '
                            '[Period] =\''+sqlDict['Period']+'\' AND '
                            '[Device] =\''+sqlDict['Device']+'\' AND '
                            '[Category] =\''+sqlDict['Category']+'\' AND '
                            '[Platform] =\''+sqlDict['Platform']+'\' AND '
                            '[NewsCategory] =\''+sqlDict['NewsCategory']+'\'')

        return select_sql
    def get_insert_sql(self,sqlDict):
        sqlKeys   =  '['+'],['.join(sqlDict.info.keys())+']'
        sqlValues =  '\''+'\',\''.join(sqlDict.info.values())+'\''
        insert_sql = 'INSERT INTO '+self.interested_DB_table+' ('+sqlKeys+', [InsertTime]) VALUES ('+sqlValues+', GETDATE())'
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
def to_DB(data):
    global local_DB_table 
    DB_Conn = local_Dbconnection(local_DB_table)
    for d in data:
        try:
            temp_select_sql = DB_Conn.get_select(d)
            flag = DB_Conn.check_is_in_db(temp_select_sql)
            if flag == False:
                temp_insert_sql = DB_Conn.get_insert_sql(d)
                flag2 = DB_Conn.insert_2_db(temp_insert_sql) 
        except Exception as e:
            print e
            pass

# --------------------------------------------------------------------------------------------------


def main():
    VIEW_ID,FB_PAGE_NAME,Brand,hcat2_dimension,id_dimension,detail_dimension,news_type,publish_date_dimension,cat3_dimension = configure_env_values()
    analytics = initialize_analyticsreporting()
    BeginDate,EndDate = generate_start_end_dates()
    bodys,maps_ = create_custom_requests(VIEW_ID,BeginDate,EndDate,news_type,cat3_dimension,hcat2_dimension)
    print BeginDate,EndDate
    clean_report = []
    for i,(body_,map_) in enumerate(zip(bodys,maps_)) :
        try :
            response = get_report(analytics,body_)
        except Exception as e:
            send_r_1(e,response,(BeginDate+'-'+EndDate),Brand,i)
            exit(1)  
        try: 
            response_data = response['reports'][0]['data']['rows']
        except Exception as e:
            send_r_2(e,response,(BeginDate+'-'+EndDate),Brand,i)
            exit(1)

        data = clean_response(response,map_)
        clean_report.extend(data)
    '''
    for d in clean_report:
        #if d.info['NewsCategory'] == 'Total' and d.info['Category'] != 'Total' and d.info['Device'] == 'Total':
        if d.info['Category'] == 'Total' and d.info['NewsCategory'] != 'Total' and d.info['Device'] == 'Total':
            print d.get_json_info()
    '''
    to_DB(clean_report)
    
            



if __name__ == "__main__":
    print __doc__
    main()

