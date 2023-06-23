#----------------------------------------------------------------------------
# Created By  :
# Created Date:
# version ='1.0'
# Change Logs : 
# Change Number :
# ---------------------------------------------------------------------------
""" Django views that takes a web request and returns a web response"""
# ---------------------------------------------------------------------------
import copy
import json
import jmespath
import psycopg2
import requests
import logging
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from .pull_threading import ThreadOperation
import threading
import time 
import socket
import random  
import datetime
import os
import traceback
import sys
from pathlib import Path
from .mdepull_interfaces import IMDEPull
from django.db import connection
script_dir = os.path.dirname( __file__ )
mymodule_dir = os.path.join( script_dir, '../../../mde-common-programs/common_utils')
sys.path.append(mymodule_dir)
service_key = os.path.join(mymodule_dir, 'service_urls.json')
with open(service_key) as f:
     service_data = json.load(f)

from cred import atlassecrets
atlas_uname, atlas_pswd = atlassecrets()
from django.conf import settings
log = settings.LOG
time_out = settings.TIMEOUT_FLAG
from Redis_instance import redismethods
from .models import * 
from django.db import *

envmodule_dir = os.path.join( script_dir, '../../../mde-common-programs/mde_environment')
sys.path.append(envmodule_dir)
key = os.path.join(envmodule_dir, 'env_setup.json')
with open(key) as f:
    config_details = json.load(f)
    env_config = config_details[os.environ.get('SERVER_ENV')]

env_limit = env_config["limit"]
env_offset = env_config["offset"]
max_number_of_retry = env_config["max_number_of_retry"]
pull_dir = env_config["pull_directory"]
proxies = {
                 "http": None,
                 "https": None,
               }
num_of_ops_per_set = env_config["num_of_ops_per_set"]
num_parallel_jobs = env_config["num_parallel_jobs"]

log_args = {"api_url": "NA", "url_type": "NA", "http_verb":"NA", "log_status_code": "NA", "transaction_id": ""}
MDE_failed_message = "MDE-PULL failed for Transaction ID-{}"

class AtlasPull(IMDEPull):
    """ MDE-Pull Views Class to Initiate Pull Service """
    def __init__(self):
       """ init method to initiat ethe message count """
       self._msg_count = 0
       self._lock = threading.Lock()

    def __addCounter(self):
        """ incremanetin the message count in get_final_data method """
        with self._lock:
            self._msg_count += 1
            return  self._msg_count

    def views_code(self, source, target, transactionId, refresh, error_code_data, jobstart_time, env, jobid, message2):
        """Initialized Thread Object"""
        thread_obj = ThreadOperation()
        """Fetching Redis data for pull service"""
        source_data_final, published_fileds, job_control_data = self.get_rediscache_data(transactionId, message2)
        """Establishing connection to Atlas"""
        atlas_credentials = (atlas_uname, atlas_pswd)#self.atlas_connectionstring() 
        last_rundate  = job_control_data["LastRunDate"]
        current_date = job_control_data["CurrentRunDate_Pull"]
        """Looping for API call sequence """
        log_args.update({"transaction_id": transactionId})
        atlas_valid_table_count = 0
        error_table_count = 0
        processed_table_count = 0

        mail_subject = "MDE_" + os.environ.get('SERVER_ENV') + "_" + refresh + "_" + "{}" + "_" + transactionId
        log.info("source_data_finasource_data_finalsource_data_finall",source_data_final)

        for api_values in source_data_final:
            log.info("api_values",api_values)
            for api_keys, values in api_values.items():

                """Constructing API Urls based on refresh Type"""
                system, system_url, fieldinput, fieldoutput = self.get_single_source_data(api_keys, values, last_rundate, refresh, transactionId, log_args, current_date,error_code_data)

                if api_keys in ["ApiCall1"]:
                    """ In API Call1 to fetch the Category Policy terms """
                    keys_data = self.normalize_outdata(values)
                    log.info("Fetching wkscategory_guids from Atlas", log_args)
                    self.wkscategory_guids = self.get_wkscategory_guids(system_url, atlas_credentials, fieldoutput, keys_data, api_keys, error_code_data, transactionId)
                    try:
                       redismethods("set", "mde_wkscategory_guids_"+transactionId,  self.wkscategory_guids)
                       log.info("set WKC category guids to Redis", log_args)
                    except Exception as e: # work on python 3.x
                       self.jobupdate(transactionId)
                       log.error("exception while fetching wkscategory_guids from Redis "+ str(e), log_args)
                       mail_msg = {
                           "SUBJECT": mail_subject.format("FAIL"),
                           "ApplicationName": "MDE",
                           "Environment": os.environ.get('SERVER_ENV'),
                           "process_start_timestamp": jobstart_time,
                           "Additional Attributes": {"sub_service": "PullService"},
                           "JobType": refresh,
                           "EventType": "FAILED",
                           "ErrorCode": "1031",
                           "ErrorDetails": e.args[1]
                       }
                elif api_keys in ["ApiCall2"]:
                    """ API crted here to fetch Business terms for the category policy terms"""
                    entity, relationshipAttributes, terms, guid =  self.apicall2_output_data(values, api_keys)
                    input_ = [values, api_keys, entity, relationshipAttributes, terms, guid]
                    try:
                        wkc_guids = redismethods("get", "mde_wkscategory_guids_"+transactionId)
                        log.info(f'checking-test wkc_guids is {wkc_guids}')
                        if not wkc_guids:
                           self.jobupdate(transactionId)
                           log.info("138  jobupdate")
                           mail_msg = {
                               "SUBJECT": mail_subject.format("FAIL"),
                               "ApplicationName": "MDE",
                               "Environment": os.environ.get('SERVER_ENV'),
                               "process_start_timestamp": jobstart_time,
                               "Additional Attributes": {"sub_service": "PullService"},
                               "JobType": refresh,
                               "EventType": "FAILED",
                               "ErrorCode": "1031",
                               "ErrorDetails": "Unable Fetching WKC Catagory Guids"
                           }
                           return MDE_failed_message.format(transactionId)
                        log.info("Fetching term_guids  from Atlas", log_args)

                        self.term_guids = self.get_term_guids(system_url, atlas_credentials,  input_, self.wkscategory_guids, error_code_data, transactionId )
                        try:
                           redismethods("set", "mde_term_guids_"+transactionId, self.term_guids)
                        except Exception as err:
                           log.warning("No Wkc category guids found to fetch Terms.{}".format(err),  log_args)
                    except Exception as err:
                        log.warning("No Wkc category guids found to fetch Terms.{}".format(err), log_args)

                elif api_keys in ["ApiCall3"]:
                    """ API call sequence3 started here to fetch the Polciy terms"""
                    log.info("I'm in APIcall3")
                    try:
                        term_guids = redismethods("get", "mde_term_guids_"+transactionId)
                        if not term_guids:
                            log.info("I'm in jobupdate APIcall3")
                            self.jobupdate(transactionId)
                            log.critical("No Terms found "+MDE_failed_message.format(transactionId), log_args)
                            mail_msg = {
                                "SUBJECT": mail_subject.format("FAIL"),
                                "ApplicationName": "MDE",
                                "Environment": os.environ.get('SERVER_ENV'),
                                "process_start_timestamp": jobstart_time,
                                "Additional Attributes": {"sub_service": "PullService"},
                                "JobType": refresh,
                                "EventType": "FAILED",
                                "ErrorCode": "1033",
                                "ErrorDetails": "No Terms found"
                            }
                            return MDE_failed_message.format(transactionId)
                        self.policy_termids = self.get_policy_termids(system_url, atlas_credentials, fieldoutput, transactionId, term_guids, error_code_data)
                        try:
                           redismethods("set", "mde_policy_termids_"+transactionId, self.policy_termids)
                        except Exception as err:
                           log.warning("error while seeting redis data in APICall3.{}".format(err), log_args)
                    except Exception as err:
                        log.warning("No Wkc Term guids found to fetch Policy Terms.{}".format(err), log_args)

                elif api_keys in ["ApiCall4"]:
                    """ API call sequence4 started here to fetch  dataprotectin_rules from policy terms"""
                    log.info("I'm in APIcall4")
                    try:
                        policy_terms = redismethods("get","mde_policy_termids_"+transactionId)
                        if not policy_terms:
                            log_args.update({"transaction_id": transactionId})
                            log.critical("No Policy Terms found "+MDE_failed_message.format(transactionId), log_args)
                            log.info("I'm in jobupdate No Policy Terms found")
                            self.jobupdate(transactionId)
                            mail_msg = {
                                "SUBJECT": mail_subject.format("FAIL"),
                                "ApplicationName": "MDE",
                                "Environment": os.environ.get('SERVER_ENV'),
                                "process_start_timestamp": jobstart_time,
                                "Additional Attributes": {"sub_service": "PullService"},
                                "JobType": refresh,
                                "EventType": "FAILED",
                                "ErrorCode": "1033",
                                "ErrorDetails": "No Policy Terms found"
                            }
                            return MDE_failed_message.format(transactionId)
                        self.data_protection_rules = self.get_dataprotectin_rules(system_url, atlas_credentials, fieldoutput, transactionId, policy_terms)
                        with open(pull_dir+"/"+"final_policy_termids.json","w") as f:
                            f.write(json.dumps(self.data_protection_rules, indent=4))
                        try:
                           redismethods("set", "mde_final_policy_termids_"+transactionId, self.data_protection_rules)
                        except Exception as ee:
                           log.warning("Error while setting final  Terms to Redis-{}".format(ee), log_args)
                    except Exception as err:
                        log.warning("Error while fetching Terms from Redis ###"+ str(err), log_args)

                elif api_keys in ["ApiCall5"]:
                    log.info("I'm in APIcall5")
                    connection_guids_list=self.get_connection_guids_list(system_url, atlas_credentials, fieldoutput, keys_data, api_keys, error_code_data, transactionId, env_offset, env_limit, refresh)
                    log.info("connection_guids_list",connection_guids_list)
                    if len(connection_guids_list) == 0:
                        log.info("227  jobupdate")
                        self.jobupdate(transactionId)
                        log.critical("No Coneections Detail Found" + MDE_failed_message.format(transactionId), log_args)
                        return MDE_failed_message.format(transactionId)
                    redismethods("set", "mde_connections_guids_"+transactionId, connection_guids_list)

                elif api_keys in ["ApiCall6"]:

                    """ API call sequence5 started here to Fetch Table guids and Table names"""
                    log.info("I'm in APIcall6")
                    log.info("system_url",system_url)
                    self.table_guids = []
                    for api in system_url:
                        log.info("api",api)
                        keys_data = self.normalize_outdata( values)
                        log.info("Fetching Table Guids from Atlas", log_args)
                        log.info(system_url)
                        if refresh == "FR":
                            connection_guids = redismethods("get",  "mde_connections_guids_"+transactionId)
                            log.info("connection_guidsconnection_guids",connection_guids)
                            for conn_guid in connection_guids:
                                log.info("Fetching Table Guids from Atlas for Connection  {}".format(conn_guid), log_args)
                                api_1 = api.replace("aaaaa",conn_guid)
                                log.info("URL for get table guids", api_1)
                                table_guids_temp = self.get_table_guids(api_1, atlas_credentials, fieldoutput, keys_data,
                                                                        api_keys, error_code_data, transactionId,
                                                                        env_offset, env_limit, refresh)

                                self.table_guids.extend(table_guids_temp)
                            atlas_valid_table_count = len(self.table_guids)
                            try:
                                error_tables, error_job_control_data = self.error_logprocessing()
                                error_table_count = self.error_table_count(copy.copy(error_tables))
                                if error_tables:

                                    self.table_guids.extend(error_tables)
                                    try:
                                        self.error_statusupd(error_job_control_data)
                                    except Exception as err:
                                        log.error("failed update error_log table status as P", log_args)
                                else:
                                    log.info("No error log tables", log_args)
                            except Exception as err:
                                log.error(str(err), log_args)
                        else:
                            table_guids_temp = self.get_table_guids(api, atlas_credentials, fieldoutput, keys_data, api_keys, error_code_data, transactionId, env_offset, env_limit, refresh)
                            self.table_guids.extend(table_guids_temp)
                            atlas_valid_table_count = len(self.table_guids)
                            try:
                               error_tables, error_job_control_data = self.error_logprocessing()
                               error_table_count = self.error_table_count(copy.copy(error_tables))
                               if error_tables:

                                  self.table_guids.extend(error_tables)
                                  try:
                                     self.error_statusupd(error_job_control_data)
                                  except Exception as err:
                                     log.error("failed update error_log table status as P", log_args)
                               else:
                                  log.info("No error log tables", log_args)
                            except Exception as err:
                               log.error(str(err), log_args)
                    log.info("self.table_guids",self.table_guids)
                    if self.table_guids:
                       self.table_guids = self.__get_unique_guids(self.table_guids)
                       try:
                          with open(pull_dir+"/"+"tables_data.json","w") as f:
                              f.write(json.dumps(self.table_guids, indent=4))
                          redismethods("set", "mde_table_guids_"+transactionId, self.table_guids)
                       except Exception as err:
                          log.error("Error while loading table guids to Redis-{}".format(err), log_args)
                    elif len(self.table_guids) == 0 and refresh == "IR":
                       self.jobupdate_comp(transactionId)
                       log.critical("No Tables found MDE sucessfully Completed {}".format(transactionId), log_args)
                       return MDE_failed_message.format(transactionId)
                    elif len(self.table_guids) == 0 and refresh in ["FR", "AH"]:
                       log.info("no table found jobupdate")
                       self.jobupdate(transactionId)
                       log.critical("No Tables found "+MDE_failed_message.format(transactionId), log_args)
                       return MDE_failed_message.format(transactionId)


                elif api_keys in ["ApiCall7"]:
                    """ API call sequence7 started here to fetch Tables columns by using API call5 output """
                    log.info("I'm in APIcall7")
                    table_guids = redismethods("get", "mde_table_guids_"+transactionId)
                    if not table_guids:
                       log.warning("No Tables present in the Atlas", log_args)
                       self.jobupdate(transactionId)
                       mail_msg = {
                           "SUBJECT": mail_subject.format("FAIL"),
                           "ApplicationName": "MDE",
                           "Environment": os.environ.get('SERVER_ENV'),
                           "process_start_timestamp": jobstart_time,
                           "Additional Attributes": {"sub_service": "PullService"},
                           "JobType": refresh,
                           "EventType": "FAILED",
                           "ErrorCode": "1028",
                           "ErrorDetails": "No data found for Table guid"
                       }
                       return MDE_failed_message.format(transactionId)
                    else: 
                        log.info("Fetching Columns data from Atlas", log_args)
                        try:
                           thread_obj.thread_columns(system_url, atlas_credentials, values, api_keys, table_guids, transactionId,  error_code_data, log_args)
                        except Exception as err:
                           log.error(str(err), log_args)

                elif api_keys in ["ApiCall8"]:
                    """ API call sequence8 started here to fetch columnsTerms by using column guids from the API call6"""
                    log.info("I'm in APIcall8")
                    # log_args = {"transaction_id": transactionId}
                    try:
                        columns_list_data = redismethods("get", "mde_columns_list_data_"+transactionId)
                        total_count = len(columns_list_data)
                        if columns_list_data:
                            log.info(f"******** Total Table Column List Pulled From ATLAS {len(columns_list_data)}", log_args)
                            log.info("Fetching Column Terms  from Atlas", log_args)
                            with open(pull_dir+"/"+"columns_data.json","w") as f:
                                f.write(json.dumps(columns_list_data, indent=4))
                            try: 
                                policy_termids = redismethods("get", "mde_final_policy_termids_"+transactionId)
                                final_policy_data = [system, published_fileds, target, transactionId, error_code_data, jobstart_time, refresh, env, jobid, policy_termids, total_count]
                                """ Sending Policy Terms ids along with columns data to fetch the Column terms and compare with Polciyterms """
                                processed_table_count = thread_obj.thread_columns_terms(system_url, atlas_credentials, values, api_keys, transactionId, columns_list_data, error_code_data,log_args, total_count, final_policy_data)
                                columns_terms_data = redismethods("get", "mde_columns_terms_data_"+transactionId)
                                """ Calling Method to compare the Columns data and Columns Terms data to get the Missed table guids for fixing data loss """
                                differentialGuid = self.__getDifferentialGuid(columns_list_data,columns_terms_data)
                          
                                number_of_retry=1
                                while len(differentialGuid) != 0 and number_of_retry <= max_number_of_retry:
                                    log.debug("******** Incremental Procesing " + str(number_of_retry)+ " for differential length "+str(len(differentialGuid)), log_args)
                                    column_data = self.__getColumnData(columns_list_data,differentialGuid)
                                    if column_data:
                                        thread_obj.thread_columns_terms(system_url, atlas_credentials, values, api_keys, transactionId, column_data, error_code_data, log_args)
                                        columns_terms = redismethods("get", "mde_columns_terms_data_"+transactionId)
                                        log.debug("******** Column Terms Data Length Before Extend is" +str(len(columns_terms)),log_args)
                                        columns_terms_data.extend(columns_terms)
                                        log.debug("******** Column Terms Data Length after retry " +str(number_of_retry)+ " is " +str(len(columns_terms_data)),log_args)
                                        differentialGuid = self.__getDifferentialGuid(columns_list_data,columns_terms_data)
                                        log.debug("After While Loop",log_args)
                                    number_of_retry+=1

                                if(number_of_retry >= max_number_of_retry and len(differentialGuid) != 0 ):
                                    log.info("Before calling saveMissingTable ", log_args)
                                    self.__saveMissingTable(columns_list_data,differentialGuid,transactionId)
                                    
                                log.debug("******** Before setting redis value for columns_terms_data ",log_args)
                                redismethods("set", "mde_columns_terms_data_"+transactionId,columns_terms_data)
                                log.debug("******** After setting redis value for columns_terms_data ",log_args)
                            except Exception as err:
                                log.error(err, log_args)
                        else:
                            log.error("No columns found {} ".format(columns_list_data),log_args)
                            self.jobupdate_comp(transactionId)
                    except Exception as err:
                        log.error("exception while fetching columns-{}".format(err),log_args)
                        self.jobupdate(transactionId)
        log.info("********Total Table Count From Atlas ----- {}".format(str(atlas_valid_table_count)), log_args)
        log.info("******** Valid Table Count From ErrorLog Table ----- {}".format(str(error_table_count)), log_args)
        log.info("******** Count of Sent to Sync Service ----- {}".format(str(processed_table_count)), log_args)

        return "MDE sucessfully Completed" 


    def error_table_count(self, error_tables):
        #error_table_count = list(set(error_tables))
        error_table_count = [dict(t) for t in {tuple(d.items()) for d in error_tables}]
        return len(error_table_count)

    def __getDifferentialGuid(self,columns_list_data,columns_terms_data):
        """ Get list column list not processed """
        log.info("******** WITHIN getDifferentialGuid", log_args)
        columns_list_guid = [k.get("guid") for k in columns_list_data]
        columns_terms_guid = [m.get("table_guid") for m in [l.get("project") for l in columns_terms_data]]
        differentialGuid = set(columns_list_guid).difference(columns_terms_guid)
        log.info("******** ENDING getDifferentialGuid", log_args)
        log.info("The number of tables missed "+str(len(differentialGuid)), log_args)
        return differentialGuid

    def __getColumnData(self,columns_list_data,differentialGuid):
          newset = []
          log.info("******** WITHIN __getColumnData", log_args)
          if len(differentialGuid) != 0:
             for id,val in enumerate(differentialGuid):
                 log.debug("VAL IN __getColumnData "+ val, log_args)
                 for item in columns_list_data :
                     if item["guid"] == val :
                        log.debug("MATCHED GUID", log_args)
                        newset.append(item)
             log.info("******** ENDING __getColumnData", log_args)
          return (newset)

    def __saveMissingTable(self,columns_list_data,differentialGuid,transactionId):
        """ Writing tables to error log table when they are not prcoesed """
        log.info("******** WITHIN __saveMissingTable", log_args)
        for id,val in enumerate(differentialGuid):
            log.info("VAL IN __saveMissingTable "+ val, log_args)
            for item in columns_list_data :
                log.info("COLUMN LIST DATE IN __saveMissingTable "+ item["guid"], log_args)
                if item["guid"] == val :
                    log.info("MATCHED GUID in __saveMissingTable", log_args)
                    #log_arg = {"log_status_code": 1044, "transaction_id": transactionId}
                    #log.error("*****ERROR!!! Table {} with GUID {} dropped out from MDE processing due to thread timeout*****".format(item.get["tableName"],item.get["guid"]),log_arg)
                    table_list = {"displayText": item.get("tableName"), "guid": item.get("guid")}
                    log.info("******** Table List "+ str(table_list), log_args)
                    time = datetime.datetime.now()
                    errdate = time.strftime("%Y-%m-%d")
                    self.errorlog((table_list),1044,'E', transactionId,errdate)

        log.info("******** ENDING __saveMissingTable", log_args)

    def get_single_source_data(self, api_keys, source_data, last_rundate, refresh, transactionId, log_args, current_date,error_code_data):
        """Constructing API's w.r.to refresh type"""
        source_domain = source_data['SystemConnection']+source_data['ObjectParameter']
        log_args.update({"api_url": source_domain, "transaction_id": transactionId})
        # connect API Structure for FR
        if api_keys == "ApiCall5":# and refresh == "FR"):
            if refresh == "FR":
                try:
                    proj_prefix = source_data['Prefix']
                    system_url = []
                    log.info("source_domainsource_domainsource_domain",source_domain)
                    log.info("proj_prefixproj_prefix11223",proj_prefix)
                    for projc_conn in proj_prefix:
                        log.info("projc_conn",projc_conn)
                        api_url = source_domain.format(typeName=source_data['ObjectName'], conection_str=projc_conn+".*")  # , wkcCreateTime=wkcCreateTime)
                        log.info("api_urlapi_urlapi_url",api_url)
                        system_url.append(api_url)
                    log_args.update({"api_url": api_url, "transaction_id": transactionId})
                    log.debug("constructed FR Table Url", log_args)
                    return source_data['SystemName'], system_url, source_data['FieldNameInput'], source_data[
                        'FieldNameOutput']

                except Exception as e:
                    log.error("unable to construct Connection URL url for FR "+str(e), log_args)

        elif api_keys == "ApiCall6":# and refresh == "FR"):
            if refresh == "FR":  
                try:
                    api_url= source_domain.format(typeName=source_data['ObjectName'], catalog_name=source_data['CatalogId'],connectionname="aaaaa")#, wkcCreateTime=wkcCreateTime)
                    system_url =[api_url]
                    log_args.update({"api_url": api_url, "transaction_id": transactionId})
                    log.debug("constructed FR Table Url", log_args)
                    return source_data['SystemName'], system_url, source_data['FieldNameInput'], source_data['FieldNameOutput']
                except Exception as e:
                    log.error("unable to construct Table url for FR "+e.args[0], log_args)
            if refresh == "IR":
                try:
                    system_url = []
                    for catalogid,last_timestamp in last_rundate.items():
                        new_last_rundate = last_timestamp.split("Z")[0]+".0Z"
                        new_current_date = current_date.split("Z")[0]+".0Z"
                        url_api = source_domain.format(typeName=source_data['ObjectName'], modificationTimestamp=new_last_rundate,
                                             catalog_name=catalogid, modificationCurrenttime=new_current_date,
                                             currentTimestamp=new_current_date, timestamp=new_last_rundate)

                        system_url.append(url_api)
                        log_args.update({"api_url": url_api, "transaction_id": transactionId})
                        log.debug("constructed IR Table Url", log_args)

                    return source_data['SystemName'], system_url, source_data['FieldNameInput'], source_data['FieldNameOutput']
                except Exception as e:
                    log.error("unable to construct Table url for IR "+str(e) , log_args)
            if refresh == "AH":
                try:
                    system_url = [source_domain.format(typeName=source_data['ObjectName'])]#, catalog_name=source_data['CatalogId'])
                    log_args.update({"api_url": system_url, "transaction_id": transactionId})
                    log.debug("constructed AH Table Url", log_args)
                    return source_data['SystemName'], system_url, source_data['FieldNameInput'], source_data['FieldNameOutput']
                except Exception as err:
                    error_code = error_code_data.get("source fields not found")
                    _data, category, log_args = self.errorcodes(error_code, transactionId)
                    if category == "Error":
                        log.error(_data, log_args)
                    elif category == "Critical":
                        log.critical(_data, log_args)
                    elif category == "Warning":
                        log.warning(_data, log_args)
                    else:
                        log.info(_data, log_args)
        elif api_keys == "ApiCall1":
            try:
                system_url = (source_domain.format(typeName=source_data['ObjectName']))+"and name='Policy Terms'"
                log_args.update({"api_url": system_url, "transaction_id": transactionId})
                log.debug("constructed Apicall1 url", log_args)
            
                return source_data['SystemName'], system_url, source_data['FieldNameInput'], source_data['FieldNameOutput']
            except Exception as err:
                log.error("unable to construct url for Apicall1 "+str(err), log_args)
                
        elif api_keys in ["ApiCall2", "ApiCall3", "ApiCall4", "ApiCall7", "ApiCall8"]:
            try:
                system_url = source_domain
                log_args.update({"api_url": system_url, "transaction_id": transactionId})
                log.debug("constructed Apicall2,3,4,6,7 urls", log_args)

                return source_data['SystemName'], system_url, source_data['FieldNameInput'], source_data['FieldNameOutput']
            except Exception as err:
                log.error("unable to construct url for Apicall2,3,4,6,7 urls "+str(err), log_args)

    def normalize_outdata(self, values):      
        """Creating output fileds"""
        output_data = values["FieldNameOutput"]
        #output_keys = {"entities": []}
        output_values = []
        for key, outdata in values["FieldNameOutput"].items():
            output_final = outdata.split('.')
            if (len(output_final) == 2 and output_final[0] == "entities") :
                output_values.append(output_final[-1])
        output_keys = {"entities": output_values}
        return output_keys

    def get_request_url_data(self, url, atlas_credentials, transactionId):
        """calling Atlas API using requests module"""
        log_args = {"api_url": url ,"url_type":"Atlas","http_verb":"GET","log_status_code": "NA", "transaction_id": transactionId}
        try:
            log.info("atlas_credentials",atlas_credentials)
            log.info("****ATLAS API****{}****start_time***{}".format(url, str(datetime.datetime.now())), log_args)
            data = requests.get(url, auth=atlas_credentials, timeout=time_out)
            log.info("****ATLAS API****{}****end_time***{}".format(url, str(datetime.datetime.now())), log_args)
            log_args.update({"log_status_code":data.status_code})
            try:
                status_code = data.status_code
                log.info("status_code",status_code)
                msg="success"
                log.info(msg, log_args)
                json_data = data.json()
                return data
            except Exception as e: # work on python 3.x
                log.error('Failed to Atlas data: '+ data.reason, log_args)
                msg = "failed"
                log.error(msg, log_args)
        except Exception as er:
            log.info("In the Exception "+ traceback.format_exc())
            log.debug('Failed to Fetch Atlas data: '+ str(er), log_args)
            sys.exit(1)

    def get_table_request_url_data(self, url, atlas_credentials, transactionId, offset, limit):
        """calling Atlas API using requests module"""
        global total_table_data
        final_url = url+"&limit={limit}&offset={offset}"
        log_args = {"api_url": final_url ,"url_type":"Atlas","http_verb":"GET","log_status_code": "NA", "transaction_id": transactionId}
        try:
           log.debug("****ATLAS API****{}****start_time***{}".format(final_url.format(limit=limit, offset=offset), str(datetime.datetime.now())), log_args)
           log.info("get_table_request_url_data---table url ",final_url)
           data = requests.get(final_url.format(limit=limit, offset=offset), auth=atlas_credentials, timeout=time_out)
           log.info("get_table_request_url_data___table  ",data)
           log_args.update({"log_status_code":data.status_code, "api_url": final_url.format(limit=limit, offset=offset)})
           log.debug("****ATLAS API****{}****end_time***{}".format(final_url.format(limit=limit, offset=offset), str(datetime.datetime.now())), log_args)
           #log_args.update({"log_status_code":data.status_code, "api_url": final_url.format(limit=limit, offset=offset)})
           try:
                status_code = data.status_code
                msg="success"
                log.debug("Status Code of the Atlas API ##"+  str(status_code),log_args)
                log.debug(msg, log_args)
                json_data = data.json()
                final_data = json_data.get("entities") 
                total_table_data.extend(final_data)
                
                if (len(final_data) < limit) :
                     return total_table_data 
                else:
                     offset = offset+limit
                    #final_url = url+"&offset={offset}"
                    #final_url_ = final_url.format(limit=limit, offset=offset)
                    
                     self.get_table_request_url_data(url, atlas_credentials, transactionId, offset, limit)
           except Exception as e: # work on python 3.x
                log.error('Failed to Atlas data: '+ str(e), log_args)
                msg = "failed"
                log.error(msg, log_args)
        except Exception as er:
            log.info("In the Exception "+ traceback.format_exc())
            log.debug('Failed to Fetch Atlas data: '+ str(er), log_args)
            sys.exit(1)
        log.info("total_table_data----table  ",total_table_data)
        return total_table_data 

    def create_json_obj(self, fieldoutput):
        """creating json obj for table output fileds"""
        final_dict = {}
        for key, value in fieldoutput.items():
            json_key = value.split(".")
            final_dict.update({json_key[-1]:  ""})
        return key, final_dict

    def create_json_policy_obj(self, fieldoutput):
        """ Cerating Json object for policy terms"""
        final_dict = {}
        for key, value in fieldoutput.items():
            json_key = value.split(".")
            final_dict.update({json_key[-1]: ""})
        return final_dict

    def get_wkscategory_guids(self, system_url, atlas_credentials, fieldoutput, keys_data, api_keys, error_code_data, transactionId ):
        """ Fetching  wkscategory_guids from this method"""
        wkc_category_guids = []    
        data = self.get_request_url_data(system_url, atlas_credentials, transactionId)
        json_data = data.json()
        #log.info("Atlas API Status code.. ##"+ str(data.status_code) )
        log_args = {"api_url": system_url ,"url_type":"Atlas","http_verb":"GET","log_status_code": data.status_code, "transaction_id": transactionId}
        if data.status_code == 200:
            msg="success"
            log.debug("Getting WKC catagory Guid", log_args)

            final_key, final_data_obj = self.create_json_obj(fieldoutput)
            for key, value in fieldoutput.items():
                json_key = value.split(".")
                try:
                    #final_data_obj.update({json_key[-1]: jmespath.search(json_key[0]+"[*]."+json_key[-1], json_data)})
                    if jmespath.search(json_key[0]+"[*]."+json_key[-1], json_data):
                        final_data_obj.update({json_key[-1]: jmespath.search(json_key[0]+"[*]."+json_key[-1], json_data)})
                    else:
                        error_code = error_code_data.get("source fields not found")
                        _data, category, log_args = self.errorcodes(error_code, transactionId)
                        if category == "Error":
                            log.error(_data, log_args)
                        elif category == "Critical":
                            log.critical(_data, log_args)
                        elif category == "Warning":
                            log.warning(_data, log_args)
                        else:
                            log.info(_data, log_args)
                        log.error("path not found for wkc category  keys ###"+value, log_args)
                except Exception as e:
                    error_code = error_code_data.get("source fields not found")
                    _data, category, log_args = self.errorcodes(error_code, transactionId)
                    #log.error(data, log_args)
                    self.error_msg_category(category, _data, log_args)
                    log.debug("path not found for wkc category  keys ###"+ value, log_args)
                    log.debug("found unexpeted key found for WKC Category  $$$ "+ str(e), log_args)
            final_keys = []
            final_val = []
            for key, values in final_data_obj.items():
                final_keys.append(key)
                final_val.append(values)
            if len(final_val[0]) == len(final_val[1]):
                zipped_data = list(zip(final_val[0], final_val[1]))
                for i in zipped_data:
                    temp_dict =  {final_keys[0]: i[0], final_keys[1]: i[1]}
                    wkc_category_guids.append(temp_dict)

        else:
            msg="failed"
            log.error(msg, log_args)
            log.debug("Atlas Wkc Category Guids Status Code ++"+  str(data.status_code), log_args)
            log.debug("Atlas Api status code +++" +system_url, log_args)
            log.debug("Atlas Status Error Code "+  json_data["errorCode"], log_args)
            log.debug("Atlas Status errorMessage "+  json_data["errorMessage"], log_args)
            log.info("In the else ##"+ traceback.format_exc())
            error_code = error_code_data.get("No Category Guids found")
            _data, category, log_args = self.errorcodes(error_code, transactionId)
            self.error_msg_category(category, _data, log_args)
            #log.error(data, log_args)
            # sys.exit(1)
        return wkc_category_guids

    def get_connection_guids_list(self, api, atlas_credentials, fieldoutput, keys_data, api_keys,error_code_data, transactionId, offset, limit, refresh):
        connection_guids_list = []
        log.info("get_connection_guids_listapi",api)
        for system_url in api:
            log.info("system_urlsystem_urlsystem_urlsystem_url", system_url)
            istrue = True
            while istrue:
                final_url = system_url + "&limit={limit}&offset={offset}"
                log_args = {"api_url": final_url, "url_type": "Atlas", "http_verb": "GET", "log_status_code": "NA",
                            "transaction_id": transactionId}
                try:
                    log.debug("****ATLAS API****{}****start_time***{}".format(final_url.format(limit=limit, offset=offset),
                                                                              str(datetime.datetime.now())), log_args)
                    log.info("final_url.format(limit=limit, offset=offset)",final_url.format(limit=limit, offset=offset))
                    data = requests.get(final_url.format(limit=limit, offset=offset), auth=atlas_credentials, timeout=time_out)
                    log_args.update(
                        {"log_status_code": data.status_code, "api_url": final_url.format(limit=limit, offset=offset)})
                    log.debug("****ATLAS API****{}****end_time***{}".format(final_url.format(limit=limit, offset=offset),
                                                                            str(datetime.datetime.now())), log_args)
                    # log_args.update({"log_status_code":data.status_code, "api_url": final_url.format(limit=limit, offset=offset)})
                    try:
                        status_code = data.status_code
                        msg = "success"
                        log.debug("Status Code of the Atlas API ##" + str(status_code), log_args)
                        log.debug(msg, log_args)
                        json_data = data.json()
                        final_data = json_data.get("entities")
                        conn_guid = []
                        for final_dt in final_data:
                            conn_guid.append(final_dt["attributes"]["wkcGuid"])
                        connection_guids_list.extend(conn_guid)
                        log.info("len(final_data)",len(final_data))
                        if (len(final_data) < limit):
                            istrue=False

                            #return connection_guids_list
                        else:
                            offset = offset + limit

                    except Exception as e:  # work on python 3.x
                        log.error('Failed to Atlas data: ' + str(e), log_args)
                        msg = "failed"
                        istrue = False
                        log.error(msg, log_args)
                except Exception as er:
                    log.info("In the Exception " + traceback.format_exc())
                    log.debug('Failed to Fetch Atlas data: ' + str(er), log_args)
                    sys.exit(1)
        return connection_guids_list

    def get_table_guids(self, system_url, atlas_credentials, fieldoutput, keys_data, api_keys,error_code_data, transactionId, offset, limit, refresh):
        """ Fetching Table guids from this method """
        WKCRelationalTable = []    
        global total_table_data
        total_table_data = []
        #data = self.get_request_url_data(system_url, atlas_credentials, transactionId)
        #json_data = data.json()
        json_data = self.get_table_request_url_data(system_url, atlas_credentials, transactionId, offset, limit)
        log_args = {"api_url": system_url ,"url_type":"Atlas","http_verb":"GET","log_status_code": "NA", "transaction_id": transactionId}
        log.info("get_table_guidsget_table_guids", json_data)
        if json_data:
            for j_data in json_data:
                final_key, final_data_obj = self.create_json_obj(fieldoutput)
                for key, value in fieldoutput.items():
                    json_key = value.split(".")
                    try:
                        if jmespath.search(json_key[-1], j_data):
                            final_data_obj.update({json_key[-1]: jmespath.search(json_key[-1], j_data)})
                        else:
                            log.error("path not found for wkc RelationalTable  keys  ###" +value,log_args)
                            error_code = error_code_data.get("source fields not found")
                            _data, category, log_args = self.errorcodes(error_code, transactionId)
                            self.error_msg_category(category, _data, log_args)
                
                    except Exception as e:
                        error_code = error_code_data.get("source fields not found")
                        _data, category, log_args = self.errorcodes(error_code, transactionId)
                        self.error_msg_category(category, _data, log_args)
                        if refresh == "IR":
                            log.info('Exception occured while getting the tables from Atlas, marking IR as complete', log_args)
                            # self.jobupdate_comp(transactionId)
                        else:
                            log.info('Exception occured while getting the tables from Atlas, marking run as failed', log_args)
                            self.jobupdate(transactionId)

                       #  self.jobupdate(transactionId)
                WKCRelationalTable.append(final_data_obj)
        else:
            msg="failed"
            log.error(msg, log_args)
            error_code = error_code_data.get("No Relational Tables")
            _data, category, log_args = self.errorcodes(error_code, transactionId)
            self.error_msg_category(category, _data, log_args)
            #if refresh == "IR":
            #    log.info('No relational tables found from Atlas, marking IR as complete')
            #    self.jobupdate_comp(transactionId)
            #else:
            #    log.info('No relational tables found from Atlas, marking run as complete')
            #    self.jobupdate(transactionId)
        log.info("WKCRelationalTableWKCRelationalTableWKCRelationalTable",WKCRelationalTable)
        return WKCRelationalTable
        
    def get_policy_termids(self, system_url, atlas_credentials, fieldoutput, transactionId, plociy_terms_guids, error_code_data):
        """ Getting policy terms """
        global log_args
        policy_term_data = []
     
        for policy_terms in plociy_terms_guids:
            log.info("****ATLAS API****{}****start_time***{}".format(system_url.format(guid=policy_terms), str(datetime.datetime.now())), log_args)
            data = requests.get(system_url.format(guid=policy_terms), auth=atlas_credentials, timeout=time_out)
            log.info("****ATLAS API****{}****end_time***{}".format(system_url.format(guid=policy_terms), str(datetime.datetime.now())), log_args)
            status_code = data.status_code
             
            if status_code == 200:
                #fieldoutput.pop("CustomAttrName")
                final_data_obj = self.create_json_policy_obj(fieldoutput)
                json_data = data.json()
                policies = json_data["entity"]["attributes"]["customAttributes"]
                PolicyName = json_data["entity"]["attributes"]["name"]
                policy_term_id = []
                policy_term_dict = {}
                for cusattr in json.loads(policies):
                    if cusattr["name"].strip().lower() == "vast data element id":
                        if cusattr["values"]:
                            policy_term_dict["PolicytermId"] =  cusattr["values"][0]["value"]
                            policy_term_dict["guid"] = policy_terms
                            policy_term_dict["PolicyName"] = PolicyName
                            policy_term_dict["Classifications"] = "|".join([i.get("displayText") for i in json_data["entity"]["relationshipAttributes"]["classifications"] if i])
                            policy_term_dict["DataProtectionRule"] = [i.get("guid") for i in json_data["entity"]["relationshipAttributes"]["dataProtectionRules"] if i ]
                            policy_term_data.append(policy_term_dict)
                        #else:
                        #    log.debug("No data for vast data element id for Bussiness terms #### "+ system_url.format(guid=policy_terms))
                    '''
                    else:
                        error_code = error_code_data.get("No Vast Element Id Found")
                        _data, category, log_args = self.errorcodes(error_code, transactionId)
                        final_data = _data.format(PolicyTerm=policy_terms)
                        self.error_msg_category(category, final_data, log_args)
                        #log.info("No vast data element id for Bussiness terms ####"+ system_url.format(guid=policy_terms))
                    '''
            else:
                error_code = error_code_data.get("No Vast Element Id Found")
                _data, category, log_args = self.errorcodes(error_code, transactionId)
                p_data = _data.format(PolicyTerm=policy_terms)
                self.error_msg_category(category, p_data, log_args)
                log.error("Atlas Table Guids Status Code "+ str(data.status_code), log_args)
                log.debug("Atlas Status Error Code   "+  str(status_code), log_args)
                log.debug("Atlas Status errorMessage  "+  data.reason, log_args)
                log.info("In the else "+ traceback.format_exc())

        return policy_term_data

    def create_final_msg(self, columns, published_fileds, target, system):
        """ Creating final message here """
        pub_data = published_fileds["PublishedField"]
        source_pub_data = published_fileds["SourcePublishedFields"]
        final_msg = {}
        #final_msg.setdefault("columns", [])
        list_data = []
        for key, value in source_pub_data.items():
            #final_msg.setdefault("columns", []) 
            if pub_data.get(value) == "val" and "." not in value:
               final_msg.update({value : ""})
            if ((pub_data.get(value) == "val" and "." in value) and "columns." in value):
               #list_data = []
               keys = value.split('.')
               #final_msg.setdefault("columns", [])
               #if pub_data.get(keys[0]) ==  keys[0]:
               #log.info(value)
               if keys[0] in pub_data and "columns." in value:
                  #log.info(keys[-1])
                  #final_msg.setdefault(keys[0], [])
                  list_data.append({keys[-1]: keys[-1]})
        final_msg.update({"columns": list_data})
        return final_msg

    def get_term_guids(self, system_url, atlas_credentials, input_data, wkscategory_guids, error_code_data, transactionId):
        """ Fecthing Terms from WKC Category polcies """
        values, api_keys, entity, relationshipAttributes, terms, guid = input_data[0], input_data[1], input_data[2], input_data[3], input_data[4],input_data[5]
        relationshipAttributes_term_guids = []
        for guids in wkscategory_guids:
            catogory_guid = guids["guid"]
            url = system_url.format(guid=catogory_guid)
            data = self.get_request_url_data(url, atlas_credentials, transactionId)
            status_code = data.status_code

            if status_code == 200:     
                try:
                    json_data = data.json().get(entity).get(relationshipAttributes)
                    log.info(f"Json data {json_data}")
                    terms_list = json_data.get(terms)
                    if not terms_list:
                        continue
                    for guid_data in terms_list:
                        term_guid = guid_data.get(guid)
                        relationshipAttributes_term_guids.append(term_guid)
                except Exception as e:
                    log_args = {"api_url": "NA", "url_type": "NA", "http_verb": "NA", "log_status_code": "NA",
                                "transaction_id": transactionId}
                    log.error("Expected Terms Keys not found $$$"+ input_data,log_args)
            else:
                log.error("Atlas terms Guids Status Code ### "+ str(data.status_code), log_args)
                log.debug("Terms Atlas API Status Error Code $$  "+  json_data["errorCode"], log_args)
                log.debug("Terms Atlas  API Status errorMessage ###"+  json_data["errorMessage"], log_args)
                log.info("Terms exception ###"+ traceback.format_exc())
                error_code = error_code_data.get("No bussiness terms found")
                _data, category, log_args = self.errorcodes(error_code, transactionId)
                self.error_msg_category(category, _data, log_args)
        return relationshipAttributes_term_guids

    def apicall2_output_data(self, values, api_keys):
        """ Constructing output data fro API call2 """
        output_data = values["FieldNameOutput"]
        for key, out_data in output_data.items():
            data = out_data.split('.')
            entity = data[0]
            relationshipAttributes = data[1]
            terms = data[2]
            guid = data[3]
            return entity, relationshipAttributes, terms, guid

    def get_rediscache_data(self, transactionId, msg2):
        """ Fethcing Pull input data from the Redis """
        try:
            log.info("transactionId",transactionId)
            source_data = redismethods("get", "mde_source_"+transactionId)
            job_control_data = redismethods("get", "mde_jobdata_"+transactionId)
            source_data_final = source_data.get("Apicalls")
            log.info("source_datasource_datasource_datasource_data",source_data)
            log.info("source_data_finalsource_data_final____1",source_data_final)
            published_fileds = source_data.get("SourcePublishedField")
            if source_data and published_fileds:
                return source_data_final, published_fileds, job_control_data
            else:
                log.warning("Unable to fetch Source details from cache - ", log_args)
                log.info("Getting details from DBservice...", log_args)
                source_data_final, published_fileds, job_control_data = self.cache_restart(transactionId, msg2)
                return source_data_final, published_fileds, job_control_data
        except Exception as err:
            log.info("rediserrerr", err)
            log.error("No redis data found "+str(err), log_args)
            log.warning("Unable to fetch Source details from cache - ", log_args)
            log.info("Getting details from DBservice...", log_args)
            source_data_final, published_fileds, job_control_data = self.cache_restart(transactionId, msg2)
            return source_data_final, published_fileds, job_control_data

    def get_dataprotectin_rules(self, system_url, atlas_credentials, fieldoutput, transactionId, policy_terms):
        """ Fethcing  DataProtectionRule from the Policy terms"""
        final_policy_data = []
        log_arg = {"transaction_id": transactionId}
        for policy_data in policy_terms:
            if not policy_data["DataProtectionRule"]:
                policy_data.update({"DataProtectionRule": ""})        
                final_policy_data.append(policy_data)
            else:
                rules_ids = policy_data["DataProtectionRule"]
                data_protection_finals_rules = []
                log.info("***** Process Start for Getting All Dataprotection rules *****", log_arg)
                for rule_id in rules_ids:
                    log.debug("****ATLAS API****{}****start_time***{}".format(system_url.format(guid=rule_id), str(datetime.datetime.now())), log_args)
                    data = requests.get(system_url.format(guid=rule_id), auth=atlas_credentials, timeout=time_out)
                    log.debug("****ATLAS API****{}****end_time***{}".format(system_url.format(guid=rule_id), str(datetime.datetime.now())), log_args)
                    status_code = data.status_code
                    if status_code == 200:
                        json_data = data.json()
                        trigger_name_action = []
                        for output_keys, values in fieldoutput.items():
                            trigger_name_action.append(jmespath.search(values, json_data))
                        final_trigger_name_action = "~".join(trigger_name_action)
                        data_protection_finals_rules.append(final_trigger_name_action)
                    else:
                        log.warning("No dataprotection rules found for the guid ++"+ rule_id, log_args)
                log.info("***** Process Completed for Getting All Dataprotection rules *****", log_arg)

                policy_data.update({"DataProtectionRule": "|".join(data_protection_finals_rules)})
                final_policy_data.append(policy_data)
        return final_policy_data

    def get_error_code_json(self):
        """ Reading Error codes from json file and returng Error message """
        try:
            module_dir = os.path.dirname(__file__)   #get current directory
            file_path = os.path.join(module_dir, 'error_code.json')
            with open(file_path) as f:
                data = json.load(f)
                #json_data = data.get("key")
                return data
        except Exception as err:
            log.error(err, log_args)

    def errorcodes(self, code, trans_id):
        """ Creating Error message, category,  log_args based on the error code  and trans_id"""
        msg = ''
        category= ''
        try:
            #errmsg = ErrorCode.objects.filter(error_code=code).values_list('error_message')
            errmsg = ErrorCode.objects.filter(error_code=code).values_list('error_message','err_catg__err_category_name')
            msg = errmsg[0][0]
            category=errmsg[0][1]

            log_args.update({"log_status_code": code, "transaction_id": trans_id })
            #log.error(data, log_args)
            return msg, category,log_args
        except Exception as err:
            log.error(err, log_args)
            return msg, category, log_args

    def error_msg_category(self, category, data, logargs):
        """ Writing log here based on severity """
        if category == "Error":
           log.error(data, logargs)
        elif category == "Critical":
           log.critical(data, logargs)
        elif category == "Warning":
           log.warning(data, logargs)
        else:
           log.info(data, logargs)
        return log

    def error_logprocessing(self):
        """ Processing Error log tables whcih are with status E in API call 5 """
        try:
           logs_data = ErrorLog.objects.filter(error_status_cd='E').values_list('error_column').distinct()
           error_tables = [i[0] for i in logs_data]
           error_job_control_data = ErrorLog.objects.filter(error_status_cd='E').values_list('job_control')
           erro_jobs = [i[0] for i in error_job_control_data]
           return error_tables, erro_jobs
        except Exception as err:
           log.error(str(err), log_args)

    def error_statusupd(self, jobid_list):
        """ Updating Error log table once Errored tables are processed in API Call5 """
        try:
           from datetime import datetime
           update_dt = datetime.now()
           val=ErrorLog.objects.filter(job_control_id__in=jobid_list).update(error_status_cd='P', last_update_dt=update_dt.strftime("%Y-%m-%d"), last_update_by='MDEPull')
           #val = ErrorLog.objects.filter(job_control_id__in=jobid_list).update(error_status_cd='P')
           return val
        except Exception as err:
           log.error(str(err), log_args)

    def jobupdate(self, trnsid):
        """ Updating Job status as Fail when it failed """
        try:
           #val=JobControl.objects.filter(transaction_id=trnsid).update(job_status_cd='FAIL')
           from datetime import datetime
           update_dt = datetime.now()
           log.info("pull log jobupdate called")
           val=JobControl.objects.filter(transaction_id=trnsid).update(job_status_cd='FAIL',last_update_dt=update_dt.strftime("%Y-%m-%d"),last_update_by='MDEPull')
           return val
        except Exception as err:
           log.error(str(err), log_args)

    def jobupdate_comp(self, trnsid):
        """ Updating Job status as Fail when it Completed """
        try:
           #val=JobControl.objects.filter(transaction_id=trnsid).update(job_status_cd='FAIL')
           from datetime import datetime
           update_dt = datetime.now()
           val=JobControl.objects.filter(transaction_id=trnsid).update(job_status_cd='COMP',last_update_dt=update_dt.strftime("%Y-%m-%d"),last_update_by='MDEPull')
           val_1 = JobControl.objects.filter(transaction_id=trnsid)
           jobstarttime = val_1[0].last_run_date
           self.update_catalog_lastjob_time(trnsid, jobstarttime.strftime("%Y-%m-%d %H:%M:%S"))
           return val
        except Exception as err:
           log.error(str(err), log_args)

    def update_catalog_lastjob_time(self,transid, jobstarttime):
        source_data = redismethods("get", "mde_source_{}".format(transid))
        catalog_list = source_data["Apicalls"][5]["ApiCall6"]["CatalogId"]
        try:
            cur = connection.cursor()
            try:
                for catalog in catalog_list:
                    cur.execute(
                        "update catalog_detail set last_job_timestamp='{}',is_fr_comp = '1' where catalog_name='{}'".format(
                            jobstarttime, catalog))
                cur.commit()
                cur.close()
            except Exception as ex:
                log.error(ex.args[1], log_args)
        except Exception as error:
            # log.info(error)
            log.error(error, log_args)

    def cache_restart(self, transid, data):
        """ Restarting Cache service when Pull ddin't find the redis data  """
        args = {"transaction_id":transid}
        dest_url="http://"+socket.gethostname()+":{}".format(service_data["cache_url"])
        mail_subject = "MDE_" + os.environ.get('SERVER_ENV') + "_" + data["message"][
            "refresh"] + "_" + "{}" + "_" + data["message"]["transactionid"]
        try:
           cache_msg = {"message":{"id":data["message"]["system"]}}
           cache_msg["message"]["refresh"] = data["message"]["refreshType"]
           cache_msg["message"]["environment"] = os.environ.get('SERVER_ENV')
           cache_msg["message"]["transactionid"] = data["message"]["transactionId"]
           #response = session.post(dest_url,json.dumps(cache_msg),proxies=proxies)
           response = requests.post(dest_url,json.dumps(cache_msg),proxies=proxies)
           if response.status_code == 200:
                source_data = redismethods("get", "mde_source_"+transid)
                job_control_data = redismethods("get", "mde_jobdata_"+transid)
                source_data_final = source_data.get("Apicalls")
                published_fileds = source_data.get("SourcePublishedField")
                if source_data_final and published_fileds:
                    return source_data_final, published_fileds, job_control_data
                else:
                    log.error("mde cache service not reachable", log_args)
                    return source_data_final, published_fileds, job_control_data
           else:
               log.info("unable to run cache service in Pull jobupdate")
               self.jobupdate(transid)
               mail_msg = {
                   "SUBJECT": mail_subject.format("FAIL"),
                   "ApplicationName": "MDE",
                   "Environment": os.environ.get('SERVER_ENV'),
                   "process_start_timestamp": data["message"]["dateTimestamp"],
                   "Additional Attributes": {"sub_service": "PullService"},
                   "JobType": data["message"]["refresh"],
                   "EventType": "FAILED",
                   "ErrorCode": "400",
                   "ErrorDetails": "Unable To Reach Cache Service!!!"
               }
               log.error("unable to run cache service in Pull", log_args)
        except Exception as er:
           log.info(" 1110 unable to run cache service in Pull jobupdate")
           self.jobupdate(transid)
           log.error(str(er), log_args)
           mail_msg = {
               "SUBJECT": mail_subject.format("FAIL"),
               "ApplicationName": "MDE",
               "Environment": os.environ.get('SERVER_ENV'),
               "process_start_timestamp": data["message"]["dateTimestamp"],
               "Additional Attributes": {"sub_service": "PullService"},
               "JobType": data["message"]["refresh"],
               "EventType": "FAILED",
               "ErrorCode": "400",
               "ErrorDetails": er
           }


    #get unique table quids
    def __get_unique_guids(self,table_guids):
        """ Filtering out the Duplicate table guids """
        unique_list = []
        for tg in table_guids:
            if tg not in unique_list:
                unique_list.append(tg)
        return unique_list

    def errorlog(self, col, code, status, trnsid, date):
        """ Inserting data into Error log table """
        try:
           cur = connection.cursor()
           cur.execute("select nextval('error_log_seq')")
           seq = cur.fetchone()[0]
           jobdata=JobControl.objects.filter(transaction_id=trnsid).values_list('job_control_id')
           jobid=jobdata[0][0]
           values = ErrorLog(error_log_id=seq, error_column=col,
                          error_code=ErrorCode.objects.get(error_code=code),
                            error_status_cd=ErrorStatus.objects.get(error_status_cd=status),
                            job_control=JobControl.objects.get(job_control_id=jobid), error_date=date,create_dt=date, created_by="MDE")
           values.save()
        except Exception as err:
           args = {"transaction_id":trnsid}
           log.error("Job not found: {}".format(err), log_args)

