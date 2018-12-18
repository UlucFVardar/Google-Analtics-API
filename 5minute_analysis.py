#! /usr/bin/env python
# -*- coding: UTF-8 -*-
# @author =__Uluç Furkan Vardar__
"""
The main runnable part of this code is taken from 
  (https://github.com/googleapis/google-api-python-client/blob/master/samples/analytics/hello_analytics_api_v3.py)

We try to take last five minutes pageview of every page.

"""
from __future__ import print_function
import json
import argparse
import sys
from googleapiclient.errors import HttpError
from googleapiclient import sample_tools
from oauth2client.client import AccessTokenRefreshError
import redis
import requests
import datetime

#  BRAND CHOOSE ------------------------------------------------------------------------------------
global which_Brand
which_Brand = 3 # 1 # 2


# 0 means Posta
# 1 means Fanatik
# 2 means Milliyet
# 4 means CNN Turk

# ---- DO NOT CHANGE THESE VALUES ------------------------------------------------------------------
def configure_env_values():
    r = redis.Redis(host='localhost', port=6379, db=0)

    global which_Brand  
    VIEW_IDs = ['........',  #Posta
                '........',   #Fanatik
                '........',
                '........']   #Milliyet

    Brands = ['Posta',    #Posta
              'Fanatik',  #Fanatik
              'Milliyet',
              'CNNTURK'] #Milliyet
    return VIEW_IDs[which_Brand],r
# --------------------------------------------------------------------------------------------------


# Initializes an analyticsreporting service object. ------------------------------------------------
def initialize_analyticsreporting():
  # Authenticate and construct service.
  service, flags = sample_tools.init( '',
       'analytics', 'v3', __doc__, __file__,
      scope='https://www.googleapis.com/auth/analytics.readonly')
  return service
# --------------------------------------------------------------------------------------------------


# Use the Analytics Service Object to query the Analytics Real Time Reporting API V3. and create custom request ---
def get_report2(service,id_):
  return service.data().realtime().get(
      ids='ga:'+id_,
      metrics='rt:pageviews',
      dimensions='rt:pagePath,rt:source,rt:medium',
      sort='-rt:pageviews',
      filters='rt:minutesAgo==05',
      max_results='30000'
      ).execute()
def get_report(service,id_):
  return service.data().realtime().get(
      ids='ga:'+id_,
      metrics='rt:pageviews',
      dimensions='rt:pagePath',
      sort='-rt:pageviews',
      filters='rt:minutesAgo==05',
      max_results='30000'
      ).execute()

# ----------------------------------------------------------------------------------------------------------------

# ---Cleaning --------------------------------------------------------------------------------
def clean_response2(results):
  def clean_urls(url):
    r = url
    if '?' in r:
      r = str(r.split('?')[0])
    if '/amp/' == str(r)[:5]:
      r = str(r[4:])
    if '/sm/' == str(r)[:4]:
      r = str(r[3:])
    return r
  def isReferral(sourceMedium):
    source,medium = sourceMedium.split('/')
    if medium == 'referral':
      if 'instagram' not in source and \
            'facebook' not in source and \
            'medyanet' not in source and \
            'twitter' not in source and \
            'google' is not source and \
            't.co' is not source and \
            'email' is not source:
        return True
    return False
  def sort_json_2_list(clean_json):
    temp=[]
    for k in clean_json.keys():
      if len(k)<3:
        continue
      a = clean_json[k]
      a['url'] = k
      temp.append(a)
    clean_list = sorted(temp, key=lambda k: k['total_pv'], reverse=True)
    return clean_list
  def isSocial(sourceMedium):
    source,medium = sourceMedium.split('/')
    if 'instagram'  in source or \
            'facebook'  in source or \
            'medyanet'  in source or \
            'twitter'  in source or \
            't.co' is source or medium == 'social':
      return True
    return False    
  def isDirect(sourceMedium):
    if 'direct' in sourceMedium:
      return True
    return False
  def isOrganic(sourceMedium):
    if 'organic' in sourceMedium:
      return True
    return False

  clean_json = {}
  rows = results['rows']

  #print (json.dumps(rows,indent=4))

  for i,r in enumerate(rows):
    clean_path = clean_urls(r[0])
    sourceMedium = (str(r[1])+'/'+str(r[2])).lower()
    try:
      clean_json[clean_path]['total_pv'] += int(r[3])
      if isReferral(sourceMedium):
        clean_json[clean_path]['REFERRAL'] += int(r[3])
      elif isSocial(sourceMedium):
        clean_json[clean_path]['SOCIAL'] += int(r[3])         
      elif isDirect(sourceMedium):
        clean_json[clean_path]['DIRECT'] += int(r[3])         
      elif isOrganic(sourceMedium):
        clean_json[clean_path]['ORGANIC'] += int(r[3])                 
    except Exception as e:
      clean_json[clean_path] = {}
      clean_json[clean_path]['REFERRAL'] = 0
      clean_json[clean_path]['SOCIAL'] = 0 
      clean_json[clean_path]['DIRECT'] = 0 
      clean_json[clean_path]['ORGANIC'] = 0 
      clean_json[clean_path]['total_pv'] = int(r[3])

      if isReferral(sourceMedium):
        clean_json[clean_path]['REFERRAL'] += int(r[3]) 
      elif isSocial(sourceMedium):
        clean_json[clean_path]['SOCIAL'] += int(r[3]) 
      elif isDirect(sourceMedium):
        clean_json[clean_path]['DIRECT'] += int(r[3]) 
      elif isOrganic(sourceMedium):
        clean_json[clean_path]['ORGANIC'] += int(r[3])                         

    try:
      #print (sourceMedium, isReferral(sourceMedium))
      if (isReferral(sourceMedium) !=True) and \
            (isSocial(sourceMedium) !=True) and \
              (isOrganic(sourceMedium) !=True) and \
                (isDirect(sourceMedium) != True):
        clean_json[clean_path][ sourceMedium] +=  int(r[3])
    except Exception as e:
      clean_json[clean_path][ sourceMedium] =  int(r[3])
    clean_json[clean_path]['Last_updated_time'] = str(datetime.datetime.now())


  clean_json = sort_json_2_list(clean_json)

      

  
  #print('-'*40)
  #print (json.dumps(a,indent=4))
  return clean_json

def clean_response(results):
  a = {}
  rows = results['rows']
  #print (json.dumps(rows,indent=4))        
  for i,r in enumerate(rows):
    if '?' in r[0]:
      rows[i][0] = str(r[0].split('?')[0])
    if '/amp/' == str(r[0])[:5]:
      rows[i][0] = str(r[0][4:])
  for r in rows:
    try:      
      a[r[0]] += int(r[1])
    except Exception as e:
      a[r[0]] = int(r[1])
  
  #print('-'*40)
  #print (json.dumps(a,indent=4))
  return a  
# --------------------------------------------------------------------------------------------

# --- WILL IMPLEMENT -------------------------------------------------------------------------
def to_Redis(REDIS,results):
  print ('URL adedi ',len(results.keys()))
  for key in results.keys():
    r = key, results[key]
    flag = REDIS.set(r[0], r[1])
    if flag == False:
      print ('----HATA',r[0],r[1],'\n--------------------------------')
      pass
    else:
      print (r[0],r[1])
      pass

def to_Redis2(REDIS,results):
  print ('URL adedi ',len(results))
  for info in results:
    url = info['url']
    del info['url']
    value = json.dumps(info)
    flag = REDIS.set(url, value)
    if flag == False:
      print ('----HATA',url, value,'\n--------------------------------')
      pass
    else:
      flag = REDIS.set(url, value)
      pass          
# --------------------------------------------------------------------------------------------

# --- Reportting  ------------------------------------------------------------------------------
def sendTelegramMess(header):
    api ='https://7oic6nrbvb.execute-api.us-east-2.amazonaws.com/sapsik/'
    r = requests.post( api, data = json.dumps( header))
    return r.json()

def send_r(type,e):
    text = str('\n---'+type+'\n\nException:\n')+str(e).replace('<','_').replace('\">','_')
    sub = str("From: 5 Dakikalık - CNNTurk")
    a =  sendTelegramMess(header  = {
                          "mailto": "-376756543",
                          "sub": sub,
                          "text": text })

def main():
  VIEW_ID, REDIS = configure_env_values()
  # Authenticate and construct service.
  try:
    service = initialize_analyticsreporting()
  except Exception as e:
    print ('Service hatası',e)
    send_r('Service hatası',e)
    exit(-1)
  
  try:
    results = get_report(service,VIEW_ID)
  except Exception as e:
    print ( 'API hatası',e )
    send_r('API hatası',e)
    exit(-1)

  try: 
    clean_results = clean_response(results)
  except Exception as e:
    print( 'Data hatası', e )
    send_r('Data hatası',e)
    exit(-1)    
  try:
    to_Redis(REDIS,clean_results)
  except Exception as e:
    print( 'Redis hatası', e )
    send_r('Redis hatası',e)
    exit(-1)     

def main2():
  VIEW_ID, REDIS = configure_env_values()
  # Authenticate and construct service.
  try:
    service = initialize_analyticsreporting()
  except Exception as e:
    print ('Service hatası',e)
    send_r('Service hatası',e)
    exit(-1)
  
  try:
    results = get_report2(service,VIEW_ID)
  except Exception as e:
    print ( 'API hatası',e )
    send_r('API hatası',e)
    exit(-1)
  #print(json.dumps(results['rows'],indent=4))  
  try: 
    clean_results = clean_response2(results)
  except Exception as e:
    print( 'Data hatası', e )
    send_r('Data hatası',e)
    exit(-1)  
  
  print(json.dumps(clean_results,indent=4))  

  try:
    to_Redis2(REDIS,clean_results)
  except Exception as e:
    print( 'Redis hatası', e )
    send_r('Redis hatası',e)
    exit(-1)          



if __name__ == '__main__':
  main2()
  #main()



