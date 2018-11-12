#! /usr/bin/env python
# -*- coding: UTF-8 -*-
# @author =__Uluç Furkan Vardar__


'''
"""Get ....... Insights From Analytics."""
Tutorial for Analytics
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
import pymssql  # for db connections
import pymysql  # for db connections
import time
import yaml
import copy
import json
import sys
import os


# Initializes an analyticsreporting service object. ------------------------------------------------
def initialize_analyticsreporting():
	"""Initializes an analyticsreporting service object.

	Returns:
	analytics an authorized analyticsreporting service object.
	"""
	SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
	DISCOVERY_URI = ('https://analyticsreporting.googleapis.com/$discovery/rest')
	KEY_FILE_LOCATION = "---YOUR KEY---"
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
def create_custom_request():
	# a custom request
	body = {  
			   "reportRequests":[  
			      {  
			         "pageSize": 20,
			         "viewId":  '129283523' , #VIEW_ID,
			         "filtersExpression":"ga:hour=="+'05'+";ga:dimension18==detail",
			         "dateRanges":[  
			            {  
			               "startDate":"today",
			               "endDate":"today"
			            }
			         ],
			         "metrics":[  
			            {  
			               "expression":"ga:pageviews"
			            },
			            {  
			               "expression":"ga:users"
			            }
			         ],
			         'orderBys':{  
			            'fieldName':'ga:pageviews',
			            'orderType':'VALUE',
			            'sortOrder':'DESCENDING',

			         },
			         "dimensions":[  
			            {  
			               "name":"ga:screenName"
			            },
			            {  
			               "name":'ga:dimension8' #coresponding_dimensions[0]
			            },
			            {  
			               "name":"ga:hour"
			            },
			            {  
			               "name": 'ga:dimension13' #id_dimension[0]
			            }
			         ]
			      }
			   ]
			}

	return body
# --------------------------------------------------------------------------------------------------


# --------------------------------------------------------------------------------------------------
def clean_response(response):
	#Cleaning 
	return json.dumps( response, ensure_ascii=True, encoding='utf8' ,indent = 4)
# --------------------------------------------------------------------------------------------------


def insert_2_db(data):
	for d in data:
		temp_select_sql = get_select(d)
		flag = check_is_in_db(temp_select_sql)
		if flag == True: 
			flag2 = insert_2_db(get_insert_sql())
		else: 
			flag3 = update_2_db(get_update_sql())


### This Code doens't work but you can get understand how it must work and flow
def main():
	analytics = initialize_analyticsreporting()
	body_ = create_custom_request()
	response = get_report(analytics,body_)
	data =  clean_response(response )
	flag = insert_2_db(data):


if __name__ == "__main__":
	main()











