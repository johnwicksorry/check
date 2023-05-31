import pandas as pd
import threading
import re
import time
from google.cloud import bigquery
from google.cloud import storage
from datetime import datetime
import google.auth

credentials, project_id = google.auth.default()
from google.auth.transport import requests

credentials.refresh(requests.Request())
client = bigquery.Client()
storage_client = storage.Client()

params_data = open('config_source_bucket.properties', 'r').read()
CI = eval(params_data)
project = CI['projectName']
dataset = CI['datasetName']
gcsPath = CI['gcsPath']
files_bucket = CI['filesBucket']
bucketName = CI['Bucket']
backupFolder = CI['backupFolder']
WorkingFolder = CI['WorkingFolder']
targetFolder = CI['targetFolder']
ManulFolder = CI['ManulFolder']
transferFolder = CI['transferFolder']
failedFolder = CI['failedFolder']

str1 = re.sub('gs://', '', gcsPath)
str2 = str1.find('/')
source_bucket = (str1.split('/'))[0]
path = format(str1[str2 + 1:])  # path without bucket name(for filename)

userID = credentials._service_account_email

# params_data = open('config_target_bucket.properties', 'r').read()
# target_cfg_dict = eval(params_data)
target_cfg_dict = {}
workingPath = files_bucket + WorkingFolder
transferredFilePath = files_bucket + transferFolder
failedFilePath = files_bucket + failedFolder

config_data = open('project_config.conf', 'r').read()
project_cgf_data = eval(config_data)
dcColumns = project_cgf_data['DCColumns']
pttColumns = project_cgf_data['PTTColumns']


class WatchdogTimer:
    def __init__(self, timeout, callback):
        self.timeout = timeout
        self.callback = callback
        self.timer = threading.Timer(timeout, self._handle_timeout)
        self.timer.start()

    def reset(self):
        self.timer.cancel()
        self.timer = threading.Timer(self.timeout, self._handle_timeout)
        self.timer.start()

    def _handle_timeout(self):
        self.callback()

    def cancel(self):
        self.timer.cancel()


def copy_blob(now, fileName, csvFile, folder_path=None):
    currentDate = re.sub(':', '_', now)
    csvFileName = re.sub('.csv', '', csvFile)
    if not folder_path:
        folder_path = backupFolder

    if fileName == 'DC':
        blob_name = f"{path}{csvFile}"
        destination_bucket_name = bucketName
        destination_blob_name = f"{backupFolder}{csvFileName}_{currentDate}"

    elif fileName == 'PTT':
        blob_name = f"{path}{csvFile}"
        destination_bucket_name = bucketName
        destination_blob_name = f"{backupFolder}{csvFileName}_{currentDate}"
    elif fileName in ['target', 'failed']:
        blob_name = f"{path}{csvFile}"
        destination_bucket_name = bucketName
        destination_blob_name = f"{folder_path}{csvFileName}_{currentDate}"
    else:
        raise Exception("Error in copy_blob function: Please give proper fileName and folder path to copy the file.")

    """Copies a blob from one bucket to another with a new name."""
    storage_client = storage.Client()

    src_bucket = storage_client.bucket(source_bucket)
    source_blob = src_bucket.blob(blob_name)
    destination_bucket = storage_client.bucket(destination_bucket_name)
    print("\n\t Initiating a copy file to {} folder  {}".format(folder_path, destination_bucket.name))

    blob_copy = src_bucket.copy_blob(
        source_blob, destination_bucket, destination_blob_name
    )
    print(
        "\n\tSource file {} copied from bucket {} to {} folder {}".format(
            source_blob.name, src_bucket.name, folder_path, blob_copy.name)
    )

    query = f"""INSERT INTO `{project}.{dataset}.DATA_MIGN_DGF_TRACE`(JOB_ID,USER_ID,ENTRY_DT,STEP_CODE,STEP_DETAILS,COMMENTS) 
            values(concat('DGF_','{now}'),'{userID}',CURRENT_DATETIME(),'DGF_SPLIT_{fileName}','BACKUP TAKEN','Backup taken for {source_blob.name} in bucket : {blob_copy.name}')"""
    job = client.query(query, location="US")
    result = job.result()
    print(
        f"After source file {csvFile} copy to {folder_path} folder, records inserted into the DATA_MIGN_DGF_TRACE table. \nInserted records are JOB_ID: DGF_{now},"
        f" USER_ID: {userID}, STEP_CODE: DGF_SPLIT_{fileName}, STEP_DETAILS: BACKUP TAKEN, "
        f"COMMENTS: Backup taken for {source_blob.name} in bucket : {blob_copy.name}")

    return blob_copy.name


def remove_file(file_name):
    bucket = storage.Bucket(storage_client, source_bucket)
    blob_name = file_name
    blob = bucket.blob(blob_name)
    blob.delete()
    print("\tThe source file is deleted from the bucket : ", blob_name)


def remove_old_queue():
    delta_query = f"""select distinct SOURCE_FILE_NAME,SPLIT_FILE_NAME,SPLIT_FILE_LOCATION,DELIVERY_PROJECT_ID,DELIVERY_MODULE,SOURCE_FILE_RECEIVED_TIME from {project}.{dataset}.DATA_MIGN_SPLT_DLVY_QUEUE
                        order by  SOURCE_FILE_RECEIVED_TIME,SOURCE_FILE_NAME"""
    delta_job = client.query(delta_query, location="US")
    project_ids = [row.DELIVERY_PROJECT_ID for row in delta_job]
    deltaFile = [row.SPLIT_FILE_NAME for row in delta_job]
    moduleName = [row.DELIVERY_MODULE for row in delta_job]
    print(f"Count of pending PTT and DC files to be deliver in the queue : {len(deltaFile)}")
    print(f"Pending PTT and DC file names in queue: {list(deltaFile)}")
    print("Clearing the old PTT queue because the new PTT file is present in bucket to transfer to all targets.")

    ptt_file_name = ''
    for idx in range(len(deltaFile)):
        if 'policy_term_treatments' in deltaFile[idx].lower():
            deletequery = f"""DELETE FROM `{project}.{dataset}.DATA_MIGN_SPLT_DLVY_QUEUE`
                        WHERE SPLIT_FILE_NAME='{deltaFile[idx]}' and DELIVERY_MODULE='{moduleName[idx]}'"""
            # print("\n\tDeleted record from DATA_MIGN_SPLT_DLVY_QUEUE for {} file".format(deltaFile[idx]))
            job = client.query(deletequery, location="US")
            job.result()
            print(
                f"Removed PTT record from DATA_MIGN_SPLT_DLVY_QUEUE for {project_ids[idx]} project and {deltaFile[idx]} file.")
            ptt_file_name = deltaFile[idx]
    if ptt_file_name:
        bucket = storage.Bucket(storage_client, bucketName)
        blob_name = WorkingFolder + ptt_file_name
        blob = bucket.blob(blob_name)
        blob.delete()
    print("Removed old PTT records from the queues.")
    query = f"""select distinct SOURCE_FILE_NAME,SPLIT_FILE_NAME,SPLIT_FILE_LOCATION,DELIVERY_PROJECT_ID,DELIVERY_MODULE,SOURCE_FILE_RECEIVED_TIME from {project}.{dataset}.DATA_MIGN_SPLT_DLVY_QUEUE
                            order by  SOURCE_FILE_RECEIVED_TIME,SOURCE_FILE_NAME"""
    query_res = client.query(query, location="US")
    files_list = [row.SPLIT_FILE_NAME for row in query_res]
    print(f"Count of pending files in queue after removing old PTT records : {len(files_list)}")
    print(f"Pending file names after removing old PTT records from queue is : {list(files_list)}")


def delivery_module(now, csvFile, fileNameNew):
    """
    This function deliver the split files to the target projects bucket and update the respective queues accordingly.
    """
    print("Fetching queues from bq DATA_MIGN_SPLT_DLVY_QUEUE table to deliver the targeted location.")
    delta_query = f"""select distinct SOURCE_FILE_NAME,SPLIT_FILE_NAME,SPLIT_FILE_LOCATION,DELIVERY_PROJECT_ID,DELIVERY_MODULE,DELIVERY_REATTEMPT,SOURCE_FILE_RECEIVED_TIME from {project}.{dataset}.DATA_MIGN_SPLT_DLVY_QUEUE
                        order by  SOURCE_FILE_RECEIVED_TIME,SOURCE_FILE_NAME"""
    delta_job = client.query(delta_query, location="US")
    deltaFile = [row.SPLIT_FILE_NAME for row in delta_job]
    projectID = [row.DELIVERY_PROJECT_ID for row in delta_job]
    sourceLoc = [row.SPLIT_FILE_LOCATION for row in delta_job]
    moduleName = [row.DELIVERY_MODULE for row in delta_job]
    delivery_attempt = [row.DELIVERY_REATTEMPT for row in delta_job]

    print("\n\tTotal number of PTT and DC files to be delivered :", len(deltaFile))
    if not len(deltaFile):
        print("Queue don't have any files to delivered")
        return
    # print("\n\tPending Files in the Queue", deltaFile)

    sourceFileName = ''
    failedProjectList = []  # This list to add the projectID for failed files.

    DC_queue_dict = {}
    PTT_queue_dict = {}
    for idx in range(len(deltaFile)):
        if 'data_classification' in deltaFile[idx].lower():
            if projectID[idx] not in DC_queue_dict:
                DC_queue_dict[projectID[idx]] = {"files": [deltaFile[idx]], "file_loc": [sourceLoc[idx]],
                                                 "del_attempt": [delivery_attempt[idx]],
                                                 "module_names": [moduleName[idx]]}
            else:
                files = DC_queue_dict[projectID[idx]]["files"]
                files.append(deltaFile[idx])

                file_loc = DC_queue_dict[projectID[idx]]["file_loc"]
                file_loc.append(sourceLoc[idx])

                del_attemt = DC_queue_dict[projectID[idx]]["del_attempt"]
                del_attemt.append(delivery_attempt[idx])

                module_names = DC_queue_dict[projectID[idx]]["module_names"]
                module_names.append(moduleName[idx])

                DC_queue_dict[projectID[idx]]["files"] = files
                DC_queue_dict[projectID[idx]]["file_loc"] = file_loc
                DC_queue_dict[projectID[idx]]["del_attempt"] = del_attemt
                DC_queue_dict[projectID[idx]]["module_names"] = module_names
        elif 'policy_term_treatments' in deltaFile[idx].lower():
            # PTT_project_list.append(projectID[idx])
            if projectID[idx] not in PTT_queue_dict:
                PTT_queue_dict[projectID[idx]] = {"files": [deltaFile[idx]], "file_loc": [sourceLoc[idx]],
                                                  "del_attempt": [delivery_attempt[idx]],
                                                  "module_names": [moduleName[idx]]}
            else:
                files = PTT_queue_dict[projectID[idx]]["files"]
                files.append(deltaFile[idx])

                file_loc = PTT_queue_dict[projectID[idx]]["file_loc"]
                file_loc.append(sourceLoc[idx])

                del_attemt = PTT_queue_dict[projectID[idx]]["del_attempt"]
                del_attemt.append(delivery_attempt[idx])

                module_names = PTT_queue_dict[projectID[idx]]["module_names"]
                module_names.append(moduleName[idx])

                PTT_queue_dict[projectID[idx]]["files"] = files
                PTT_queue_dict[projectID[idx]]["file_loc"] = file_loc
                PTT_queue_dict[projectID[idx]]["del_attempt"] = del_attemt
                PTT_queue_dict[projectID[idx]]["module_names"] = module_names

    if DC_queue_dict:
        for pro in DC_queue_dict.keys():
            print(
                f"In {pro} project queue, Total DC Files to transfers are {len(DC_queue_dict[pro]['files'])} and File names are: {DC_queue_dict[pro]['files']}")
    if PTT_queue_dict:
        print(f"Number of PTT files which are in the Queue to be transferred: {len(PTT_queue_dict.keys())}")
        print(f"PTT file to transfer to these all projects : {list(PTT_queue_dict.keys())}")

    AllTransferFlag = "Y"

    for file_type in ['PTT', 'DC']:
        if file_type == 'PTT':
            process_dict = PTT_queue_dict
        else:
            process_dict = DC_queue_dict
        if not process_dict:
            print(f"Don't have any {file_type} files to be delivered in the Queue")
            continue

        print(f"**** {file_type} files queue processing started. ****")
        PTT_read_again = True
        pro_len = len(process_dict.keys())
        num = 1
        for pro_name in process_dict.keys():
            if pro_name not in target_cfg_dict:
                print(
                    f"Project Name {pro_name} not present in target bucket properties. Not delivering for this project.")
                continue
            print(f"*** {num} out of {pro_len} projects for {file_type} queue {pro_name}  processing started. ***")
            files_list = process_dict[pro_name]['files']
            module_list = process_dict[pro_name]['module_names']
            loc_list = process_dict[pro_name]['file_loc']
            print(
                f"For {pro_name} project queue, files list : {files_list}, module_list : {module_list}, location list : {loc_list}")
            file_num = 1
            file_len = len(files_list)
            for idx in range(len(files_list)):
                try:
                    print(
                        f"** {file_num} out of {file_len} for {file_type} file {files_list[idx]} for {pro_name} queue delivery process started **")
                    gscPathList = getGCSPath(pro_name)
                    gcs_path = gscPathList[0]
                    sourceFilePath = loc_list[idx]
                    sourceFileName = files_list[idx]
                    if 'data_classification' in sourceFileName.lower():
                        if pro_name in failedProjectList:
                            print(
                                f"Skipping this {files_list[idx]} file transfer for this project {pro_name} because previous"
                                f" {files_list[idx - 1]} file transfer for same project failed and is still in the Queue."
                                f" Also keeping {files_list[idx]} in same queue.")
                            print(
                                f"** {file_num} out of {file_len} for {file_type} file {files_list[idx]} for {pro_name} queue delivery process finished **")
                            file_num += 1
                            continue
                        csv_name = gscPathList[1]
                        flag = 'DC'
                        print("Reading DC split file {}".format(f"{sourceFilePath}{sourceFileName}"))
                        data_tmp = pd.read_csv(f"{sourceFilePath}{sourceFileName}", encoding='latin1')
                        csv_rows = len(data_tmp.axes[0])
                        PTT_read_again = True
                        AllTransferFlag = 'N'
                    elif 'policy_term_treatments' in sourceFileName.lower():
                        # if idx != len(files_list) - 1 and sourceFileName == files_list[idx + 1]:
                        #     moreEventFlag = 'Y'
                        # else:
                        #     moreEventFlag = 'N'
                        csv_name = 'Policy_Term_Treatments'
                        flag = 'PTT'
                        print("Reading PTT split file {}".format(f"{sourceFilePath}{sourceFileName}"))
                        if PTT_read_again:
                            data_tmp = pd.read_csv(f"{sourceFilePath}{sourceFileName}", encoding='latin1')
                            csv_rows = len(data_tmp.axes[0])
                            PTT_read_again = False
                    else:
                        raise f"{sourceFileName} file is not a DC or policy_term_treatments file."

                    print("Moving {} file to delivery path {}".format(f"{sourceFilePath}{sourceFileName}", gcs_path))
                    returnVar = moveFiles(csv_name, gcs_path, sourceFileName, now, data_tmp, module_list[idx], pro_name,
                                          csvFile, fileNameNew, csv_rows, flag)
                    successFlag = returnVar[0]
                    deliveredFileName = returnVar[1]
                    if successFlag == 2:
                        print(
                            f"This file {deliveredFileName} has not been delivered as it has 902 policy term id, which is sensitive data.")
                    elif not successFlag:
                        if file_type == "DC":
                            failedProjectList.append(pro_name)
                        print(
                            "\n\t*******Source file :{} \t Split File: {} at timestamp {} \t NOT Delivered for {} \t for project {} and Module {}.*******".format(
                                fileNameNew, sourceFileName, now, deliveredFileName, pro_name, module_list[idx])
                        )
                    else:
                        print(
                            "\n\t*******Source file :{} \t Split File: {} at timestamp {} \t Delivered succesfully for {} \t for project {} and Module {}.*******".format(
                                fileNameNew, sourceFileName, now, deliveredFileName, pro_name, module_list[idx])
                        )

                    if file_type == 'PTT' and successFlag == 0:
                        AllTransferFlag = 'N'
                        # print("File type is PTT and SuccessFlag is Zero then AllTransferFlag is N")
                        # if moreEventFlag == 'N' and sameFileAllTransferFlag == 'Y':
                        #     data_tmp.to_csv(transferredFilePath + sourceFileName, index=False)  # header=Col
                        #     print("\n\tMoved PTT File to transferred Folder", transferredFilePath + sourceFileName)
                        #     bucket = storage.Bucket(storage_client, bucketName)
                        #     blob_name = path + WorkingFolder + sourceFileName
                        #     blob = bucket.blob(blob_name)
                        #     blob.delete()

                    print(
                        f"** {file_num} out of {file_len} for {file_type} file {files_list[idx]} for {pro_name} queue delivery process finished **")
                    file_num += 1

                except Exception as e:

                    error = str(e).split("\n")[0]
                    print("Error in Delivery Module {}: ".format(e))

                    print(
                        "\n\t*******Source file :{} \t Split File: {} at timestamp {} \t Delivery Failed for {} !!!\t for project {} and Module {}.*******".format(
                            fileNameNew, sourceFileName, now, sourceFileName, pro_name, module_list[idx])
                    )
                    query = f"""INSERT INTO `{project}.{dataset}.DATA_MIGN_DGF_TRACE`(JOB_ID,USER_ID,ENTRY_DT,STEP_CODE,STEP_DETAILS,COMMENTS) 
                                        values(concat('DGF_','{now}'),'{userID}',CURRENT_DATETIME(),'DGF_SPLIT_DC','DELIVERY MODULE FAILED','[{sourceFileName}] : {error}')"""
                    job = client.query(query, location="US")
                    job.result()
                    #Updated file_num for next file
                    file_num += 1

            print(f"*** {num} out of {pro_len} projects for {file_type} queue {pro_name}  processing finished. ***")
            num += 1
        print(f"**** {file_type} files queue processing finished. ****")

        #        print(f"pro_name existed, file_type {file_type}, AllTransferFlag {AllTransferFlag}")
        if file_type == 'PTT' and AllTransferFlag == 'Y':
            # data_tmp.to_csv(transferredFilePath + sourceFileName, index=False)  # header=Col
            print("\n\tMoved PTT File to transferred Folder", transferredFilePath + sourceFileName)
            bucket = storage.Bucket(storage_client, bucketName)
            blob_name = WorkingFolder + sourceFileName
            blob = bucket.blob(blob_name)
            blob.delete()


def getGCSPath(projectID):
    module_name = target_cfg_dict[projectID]['module_name']
    print("extracting target path from config_target_bucket.properties")
    # for i in new_dir:
    csv_name = module_name + "_DATA_CLASSIFICATION"
    gcs_path = target_cfg_dict[projectID]['delivery_path']
    csv_path = [gcs_path, csv_name]
    return csv_path


def moveFiles(csv_name, gcs_path, split_csv_name, now, data_tmp, i, d, csvFile, fileNameNew, csv_rows, flag):
    print("\n\tDelivering File : ", split_csv_name)

    str1 = re.sub('gs://', '', gcs_path)
    targetBucket = str1.split('/')[0]  # target bucket

    str2 = str1.find('/')
    targetPath = format(str1[str2 + 1:])  # target path without bucket

    sourceFileName = WorkingFolder + split_csv_name
    targetFileName = targetPath.rstrip('/') + '/' + csv_name + ".csv"

    print("SourceFileName is {}".format(sourceFileName))
    print("TargetPath is {}/{} and targetFileName is {} ".format(targetBucket, targetPath, targetFileName))
    blob_name = sourceFileName
    destination_blob_name = targetFileName

    storage_client = storage.Client()
    source_bucket = storage_client.bucket(bucketName)
    source_blob = source_bucket.blob(blob_name)
    destination_bucket = storage_client.bucket(targetBucket)

    try:
        if flag == 'DC':
            policy_term_ids = data_tmp.SECURITY_ELEMENT_ID
            if target_cfg_dict[d]['check_902_flag'] and 902 in list(policy_term_ids):
                faild_csv_name = "Failed_902_[" + d + "]_DATA_CLASSIFICATION_" + now + ".csv"
                print(
                    f"\n\tFound 902 policy term ID in this split file: {split_csv_name} and Moved to failed Folder: {failedFilePath + faild_csv_name}")
                print(
                    "Inserting a record into table: DATA_MIGN_SPLT_DLVY_QUEUE_HIST as the file have 902 policytermid which is sensitive data")
                query = f"""INSERT INTO `{project}.{dataset}.DATA_MIGN_SPLT_DLVY_QUEUE_HIST`(SOURCE_FILE_NAME,SOURCE_FILE_RECEIVED_TIME,DELIVERY_MODULE,DELIVERY_PROJECT_ID,SPLIT_FILE_NAME,SPLIT_TIME,SPLIT_FILE_LOCATION,LAST_DELIVERY_ATTEMPT_TIME,DELIVERY_REATTEMPT,FAILED_DESC,STATUS)
                            values('{csvFile}',CURRENT_DATETIME(),'INVALID','{d}','{faild_csv_name}',CURRENT_DATETIME(),'{failedFilePath}',CAST(NULL as DATETIME),'NO','INVALID POLICY TERM ID','FAILED PERMANENTLY')"""
                job = client.query(query, location="US")
                job.result()

                print(f"Deleting entry from DATA_MIGN_SPLT_DLVY_QUEUE table for {split_csv_name} file.")
                deletequery = f"""DELETE FROM `{project}.{dataset}.DATA_MIGN_SPLT_DLVY_QUEUE`
                            WHERE SPLIT_FILE_NAME='{split_csv_name}' and DELIVERY_MODULE='{i}'"""
                print("\n\tDeleted record from DATA_MIGN_SPLT_DLVY_QUEUE for {} file".format(split_csv_name))
                job = client.query(deletequery, location="US")
                job.result()
                source_blob.delete()

                query = f"""INSERT INTO `{project}.{dataset}.DATA_MIGN_DGF_TRACE`(JOB_ID,USER_ID,ENTRY_DT,STEP_CODE,STEP_DETAILS,COMMENTS) 
                                                        values(concat('DGF_','{now}'),'{userID}',CURRENT_DATETIME(),'DGF_SPLIT_DC','FILE NOT PROCESSED','[{csv_name}] : File have 902 policy term id. Moved file to failed folder.')"""
                job = client.query(query, location="US")
                result = job.result()

                returnVar = [2, targetFileName]
                return returnVar

        tries = int(project_cgf_data["retry_count"])
        sleep_time = int(project_cgf_data["sleep_time"])
        for ind in range(tries):
            try:
                blob_copy = source_bucket.copy_blob(
                    source_blob, destination_bucket, destination_blob_name
                )
            except Exception as e:
                if ind < tries - 1:
                    print(f"Retrying for blob copy for {ind + 1} time.......")
                    time.sleep(sleep_time)
                    continue
                else:
                    print("Final Retry is done for blob copy...")
                    raise e
            break
        print("Copied file from {}/{} to {}/{}".format(source_bucket.name, sourceFileName, destination_bucket.name,
                                                       targetFileName))

        insertquery = f"""INSERT INTO `{project}.{dataset}.DATA_MIGN_SPLT_DLVY_QUEUE_HIST`
            SELECT  SOURCE_FILE_NAME,SOURCE_FILE_RECEIVED_TIME,DELIVERY_MODULE,DELIVERY_PROJECT_ID,SPLIT_FILE_NAME,SPLIT_TIME,SPLIT_FILE_LOCATION,CURRENT_DATETIME(),"NA","","VALID [DELIVERED SUCCESSFULLY]" FROM `{project}.{dataset}.DATA_MIGN_SPLT_DLVY_QUEUE`
            WHERE SPLIT_FILE_NAME='{split_csv_name}' and DELIVERY_MODULE='{i}'"""
        print(
            "\n\tFile {} is Delivered and Moved record from DATA_MIGN_SPLT_DLVY_QUEUE table to DATA_MIGN_SPLT_DLVY_QUEUE_HIST".format(
                split_csv_name))
        print(
            f"Inserted records in DATA_MIGN_SPLT_DLVY_QUEUE_HIST table are SOURCE_FILE_NAME: {split_csv_name}, DELIVERY_MODULE: {i}, DELIVERY_PROJECT_ID: {d}")
        job = client.query(insertquery, location="US")
        result = job.result()

        query = f"""INSERT INTO `{project}.{dataset}.DATA_MIGN_DLVY_AMPT_LOG`(DELIVERY_MODULE,SPLIT_FILE_NAME,DELIVERY_ATTEMPT_TIME,STATUS)
            values('{i}','{split_csv_name}',CURRENT_DATETIME(),'PASS')"""
        job = client.query(query, location="US")
        result = job.result()

        query = f"""INSERT INTO `{project}.{dataset}.DATA_MIGN_DATA_CLASS_STAT`(FILE_NAME,SUB_FILE_NAME,TOTAL_RECORDS,STATUS,FILE_RECEIVED_DATE) 
            values('{fileNameNew}','{d}:{i}',{csv_rows},'VALID-SUCCESSFUL',CURRENT_DATETIME())"""
        job = client.query(query, location="US")
        result = job.result()
        print(f"Inserted records into DATA_MIGN_DATA_CLASS_STAT table. Records are : FILE_NAME: {fileNameNew}, "
              f"SUB_FILE_NAME: {d}:{i}, TOTAL_RECORDS: {csv_rows}, STATUS: VALID-SUCCESSFUL")

        query = f"""INSERT INTO `{project}.{dataset}.DATA_MIGN_DGF_TRACE`(JOB_ID,USER_ID,ENTRY_DT,STEP_CODE,STEP_DETAILS,COMMENTS) 
                    values(concat('DGF_','{now}'),'{userID}',CURRENT_DATETIME(),'DGF_SPLIT_DC','FILE MOVED TO TARGETED BUCKET','[{csv_name}] : Split file is moved to {gcs_path} gcs bucket')"""
        job = client.query(query, location="US")
        result = job.result()
        print(f"Inserted records into DATA_MIGN_DGF_TRACE table. Records are JOB_ID: DGF_{now}, USER_ID: {userID}, "
              f"STEP_CODE: DGF_SPLIT_{flag}, STEP_DETAILS: FILE MOVED TO TARGETED BUCKET, "
              f"COMMENTS: [{csv_name}] : Split file is moved to {gcs_path} gcs bucket")

        deletequery = f"""DELETE FROM `{project}.{dataset}.DATA_MIGN_SPLT_DLVY_QUEUE`
            WHERE SPLIT_FILE_NAME='{split_csv_name}' and DELIVERY_MODULE='{i}'"""
        print("\n\tDeleted record from DATA_MIGN_SPLT_DLVY_QUEUE for {} file".format(split_csv_name))
        job = client.query(deletequery, location="US")
        result = job.result()

        if flag == 'DC':
            data_tmp.to_csv(transferredFilePath + split_csv_name, index=False)
            print("\n\tDelivered File to transferred Folder", transferredFilePath + split_csv_name)
            source_blob.delete()

        returnVar = [1, targetFileName]
        return returnVar

    except Exception as e:
        error = str(e).split("\n")[0]
        print("Error while moving file to target bucket : {}".format(e))

        time.sleep(5)
        updatequery = f"""UPDATE `{project}.{dataset}.DATA_MIGN_SPLT_DLVY_QUEUE`
            SET LAST_DELIVERY_ATTEMPT_TIME=CURRENT_DATETIME(),	DELIVERY_REATTEMPT="YES",	FAILED_DESC="{error}",	STATUS="VALID [DELIVERY FAILED]"
            WHERE SPLIT_FILE_NAME='{split_csv_name}' and DELIVERY_MODULE='{i}'"""
        # print("\n\t Updatequery for delivery queue ", updatequery)
        print("\n\tUpdated DELIVERY FAILED record for {} file in bq DATA_MIGN_SPLT_DLVY_QUEUE table".format(
            split_csv_name))
        job = client.query(updatequery, location="US")
        result = job.result()

        query = f"""INSERT INTO `{project}.{dataset}.DATA_MIGN_DLVY_AMPT_LOG`(DELIVERY_MODULE,SPLIT_FILE_NAME,DELIVERY_ATTEMPT_TIME,STATUS)
            values('{i}','{split_csv_name}',CURRENT_DATETIME(),'FAILED')"""
        job = client.query(query, location="US")
        result = job.result()

        query = f"""INSERT INTO `{project}.{dataset}.DATA_MIGN_DGF_TRACE`(JOB_ID,USER_ID,ENTRY_DT,STEP_CODE,STEP_DETAILS,COMMENTS) 
                        values(concat('DGF_','{now}'),'{userID}',CURRENT_DATETIME(),'DGF_SPLIT_DC','FILE NOT PROCESSED','[{csv_name}] : {error}')"""
        job = client.query(query, location="US")
        result = job.result()

        returnVar = [0, targetFileName]
        return returnVar


def splitFile(data, workingPath, fileNameNew, now, csvFile):
    Col = ['PHYSICAL_NM', 'ELEMENT_NM', 'COLUMN_NM', 'SECURITY_ELEMENT_ID']
    delta_query = f"""select distinct SOURCE_FILE_NAME,SPLIT_FILE_NAME,SPLIT_FILE_LOCATION,DELIVERY_PROJECT_ID,DELIVERY_MODULE,SOURCE_FILE_RECEIVED_TIME from {project}.{dataset}.DATA_MIGN_SPLT_DLVY_QUEUE
                        order by  SOURCE_FILE_RECEIVED_TIME,SOURCE_FILE_NAME"""
    delta_job = client.query(delta_query, location="US")
    deltaFile = [row.SPLIT_FILE_NAME for row in delta_job]

    print("\n\tCount of pending files to be delivered in the queue before Splitting: {}".format(len(deltaFile)))
    print("\n\tPending Files in the Queue before Spliting", deltaFile)
    split_success_file_list = []
    split_failed_file_list = []
    for d in data.projectName.unique():
        print("creating file for project: {}".format(d))
        data_tmp = data.loc[data['projectName'] == d].copy()
        data_tmp.drop(['projectName'], axis=1, inplace=True)  # dropping projectName column
        csv_rows = len(data_tmp.axes[0])  ##Count of rows in splitted csv
        query = f"""INSERT INTO `{project}.{dataset}.DATA_MIGN_DGF_TRACE`(JOB_ID,USER_ID,ENTRY_DT,STEP_CODE,STEP_DETAILS,COMMENTS) 
                    values(concat('DGF_','{now}'),'{userID}',CURRENT_DATETIME(),'DGF_SPLIT_DC','FILE IS SPLIT','[{fileNameNew}] : File is split as per the project for {d}')"""
        job = client.query(query, location="US")
        result = job.result()

        if d in target_cfg_dict and target_cfg_dict[d]['status'] == 'ACTIVE':
            print("project name {} found in config_target_bucket.properties".format(d))
            module_name = target_cfg_dict[d]['module_name']
            # for i in new_dir:
            csv_name = module_name + "_DATA_CLASSIFICATION"
            gcs_path = target_cfg_dict[d]['delivery_path']

            split_csv_name = csv_name + "_" + now + ".csv"
            print("Splitting CSV file based on project records in {}".format(split_csv_name))
            data_tmp.to_csv(workingPath + split_csv_name, index=False, header=Col)

            print(
                "file for project {} has been generated in working folder {}. inserting a record into delivery queue table DATA_MIGN_SPLT_DLVY_QUEUE".format(
                    d, workingPath))
            query = f"""INSERT INTO `{project}.{dataset}.DATA_MIGN_SPLT_DLVY_QUEUE`
                                values('{csvFile}',CURRENT_DATETIME(),'{module_name}','{d}','{split_csv_name}',CURRENT_DATETIME(),'{workingPath}',cast(NULL as DATETIME),'','','')"""
            # print("\n\n\tINSERT QUERY INSIDE SPLIT", query)
            job = client.query(query, location="US")
            result = job.result()
            print(
                f"\n\tInserted records in DATA_MIGN_SPLT_DLVY_QUEUE. Records are SPLIT_FILE_NAME: {split_csv_name}, DELIVERY_PROJECT_ID: {d} , SPLIT_FILE_LOCATION: {workingPath}, DELIVERY_MODULE: {module_name}")
            split_success_file_list.append(split_csv_name)

        else:
            if d in target_cfg_dict and target_cfg_dict[d]['status'] == 'INACTIVE':
                print(f"project name {d} status is in INACTIVE, Hence not creating split file.")
            else:
                print("project name {} not found in config_target_bucket.properties".format(d))
            split_csv_name = "INVALID_PS_[" + d + "]_DATA_CLASSIFICATION_" + now + ".csv"
            data_tmp.to_csv(failedFilePath + split_csv_name, index=False, header=Col)
            print("\n\tMoved File to failed Folder", failedFilePath + split_csv_name)
            print(
                "inserting a record into delivery queue table DATA_MIGN_SPLT_DLVY_QUEUE_HIST as the project is not found in config_target_bucket.properties")
            query = f"""INSERT INTO `{project}.{dataset}.DATA_MIGN_SPLT_DLVY_QUEUE_HIST`(SOURCE_FILE_NAME,SOURCE_FILE_RECEIVED_TIME,DELIVERY_MODULE,DELIVERY_PROJECT_ID,SPLIT_FILE_NAME,SPLIT_TIME,SPLIT_FILE_LOCATION,LAST_DELIVERY_ATTEMPT_TIME,DELIVERY_REATTEMPT,FAILED_DESC,STATUS)
            values('{csvFile}',CURRENT_DATETIME(),'INVALID','{d}','{split_csv_name}',CURRENT_DATETIME(),'{failedFilePath}',CAST(NULL as DATETIME),'NO','INVALID PROJECT ID','FAILED PERMANENTLY')"""
            job = client.query(query, location="US")
            result = job.result()
            # print("deleting file from the working folder: {}".format(path + WorkingFolder + split_csv_name))
            # remove_file(path + WorkingFolder + split_csv_name)
            print(
                "inserting records in the bq table DATA_MIGN_DATA_CLASS_STAT and DATA_MIGN_DGF_TRACE for tracing purpose")
            query = f"""INSERT INTO `{project}.{dataset}.DATA_MIGN_DATA_CLASS_STAT`(FILE_NAME,SUB_FILE_NAME,TOTAL_RECORDS,STATUS,FILE_RECEIVED_DATE) 
            values('{fileNameNew}','{d}',{csv_rows},'REJECTED(INVALID)',CURRENT_DATETIME())"""
            job = client.query(query, location="US")
            result = job.result()

            query = f"""INSERT INTO `{project}.{dataset}.DATA_MIGN_DGF_TRACE`(JOB_ID,USER_ID,ENTRY_DT,STEP_CODE,STEP_DETAILS,COMMENTS) 
                    values(concat('DGF_','{now}'),'{userID}',CURRENT_DATETIME(),'DGF_SPLIT_DC','FILE REJECTED','File rejected due to invalid project name : {d}')"""
            job = client.query(query, location="US")
            result = job.result()
            split_failed_file_list.append(split_csv_name)
    if split_success_file_list:
        print(
            "\n\t*******Source file {} successfully splitted and project matched with the target bucket properties!!!... splitted into {} splits and placed under working dir - {} at timetsamp {}.*******".format(
                fileNameNew, len(split_success_file_list), split_success_file_list, now)
        )

    if split_failed_file_list:
        print(
            "\n\t*******Source file {} successfully splitted but project not matched with the target bucket properties!!!...splitted into {} splits and placed under working dir - {} at timetsamp {}.*******".format(
                fileNameNew, len(split_failed_file_list), split_failed_file_list, now)
        )


def get_target_projects_from_bq():
    """
    Fetch the target bucket properties from bq table
    """
    global target_cfg_dict
    target_query = f"""select distinct PROJECT_NAME,MODULE_NAME,DELIVERY_PATH,CHECK_902_FLAG,STATUS from {project}.{dataset}.DATA_MIGN_TARGET_BUCKET_PROPERTIES"""
    delta_job = client.query(target_query, location="US")
    project_name_list = [row.PROJECT_NAME for row in delta_job]
    module_name_list = [row.MODULE_NAME for row in delta_job]
    delivery_path_list = [row.DELIVERY_PATH for row in delta_job]
    check_902_flag_list = [row.CHECK_902_FLAG for row in delta_job]
    status_list = [row.STATUS for row in delta_job]
    for idx in range(len(project_name_list)):
        target_cfg_dict[project_name_list[idx]] = {'module_name': module_name_list[idx],
                                                   'delivery_path': delivery_path_list[idx],
                                                   'check_902_flag': check_902_flag_list[idx],
                                                   'status': status_list[idx]}


def hello_gcs_start(event_name):
    """
    Hello gcs function starting point.
    """
    event = event_name
    file = event
    print(f"\nProcessing file: {file['name']} inside bucket: Bucket: {event['bucket']}")
    bucketName = event['bucket']
    filename = event['name']
    folders = []
    folders.extend([backupFolder, WorkingFolder, ManulFolder, transferFolder, failedFolder])
    now = datetime.now().strftime("%Y_%m_%d_%H:%M:%S")
    csvFile = re.sub(path, '', filename)

    if f'{path}data_classification' in filename and all(x not in filename for x in folders):

        try:
            print("Fetching target bucket properties from bq.")
            get_target_projects_from_bq()

            print("file that is being processed is identified as data classification file")
            query = f"""INSERT INTO `{project}.{dataset}.DATA_MIGN_DGF_TRACE`(JOB_ID,USER_ID,ENTRY_DT,STEP_CODE,STEP_DETAILS,COMMENTS) 
                            values(concat('DGF_','{now}'),'{userID}',CURRENT_DATETIME(),'DGF_SPLIT_DC','DC FILE IN GCS COMMON BUCKET','[{csvFile}] :  File uploaded in the common bucket')"""
            job = client.query(query, location="US")
            result = job.result()
            print(
                f"File {csvFile} uploaded in common bucket and Inserted tracing records into bq DATA_MIGN_DGF_TRACE table. \nInserted records are JOB_ID: DGF_{now},"
                f" USER_ID: {userID}, STEP_CODE: DGF_SPLIT_DC, STEP_DETAILS: DC FILE IN GCS COMMON BUCKET, "
                f"COMMENTS: [{csvFile}] :  File uploaded in the common bucket")

            query = f"""INSERT INTO `{project}.{dataset}.DATA_MIGN_DGF_TRACE`(JOB_ID,USER_ID,ENTRY_DT,STEP_CODE,STEP_DETAILS,COMMENTS) 
                            values(concat('DGF_','{now}'),'{userID}',CURRENT_DATETIME(),'DGF_SPLIT_DC','CF TRIGGERED','Cloud Function triggered for [{csvFile}] file')"""
            job = client.query(query, location="US")
            result = job.result()
            print(
                f"Cloud function triggered and Inserted tracing records into bq DATA_MIGN_DGF_TRACE table. \nInserted records are JOB_ID: DGF_{now},"
                f" USER_ID: {userID}, STEP_CODE: DGF_SPLIT_DC, STEP_DETAILS: CF TRIGGERED, "
                f"COMMENTS: Cloud Function triggered for [{csvFile}] file")

            copiedFile = copy_blob(now, 'DC', csvFile)
            str1 = copiedFile.rfind('/')
            fileNameNew = format(copiedFile[str1 + 1:])

            try:
                print(f"Reading csv file {gcsPath}{csvFile}")
                data = pd.read_csv(f"{gcsPath}{csvFile}", encoding='latin1')

                dc_file_cols = list(data.axes[1])
                file_cols = [x.lower() for x in dc_file_cols]

                total_cols = len(file_cols)  # Count of columns in given csv
                total_rows = len(data.axes[0])  # Count of rows in given csv

            except:
                dc_file_cols = []
                file_cols = []
                total_cols = 0
                total_rows = 0

            exp_cols = [x.lower() for x in dcColumns]  # DC columns taking from source config properties file
            print(
                f"validating file by checking all file columns i.e {dc_file_cols} should be match with expected columns i.e {dcColumns} and total number of columns should be 5")
            if total_cols == 5 and all(_ in file_cols for _ in exp_cols):
                print(
                    "\n\t*******Validated incoming source file...{} at timestamp {} by user id {}.*******".format(
                        fileNameNew, now, userID)
                )
                final_dc_col = []
                for ix in exp_cols:
                    final_dc_col.append(dc_file_cols[file_cols.index(ix)])

                print(f"Rearranging the DC file columns as per the source config file {final_dc_col}.")
                data = data[final_dc_col]

                query = f"""INSERT INTO `{project}.{dataset}.DATA_MIGN_DGF_TRACE`(JOB_ID,USER_ID,ENTRY_DT,STEP_CODE,STEP_DETAILS,COMMENTS) 
                                values(concat('DGF_','{now}'),'{userID}',CURRENT_DATETIME(),'DGF_SPLIT_DC','FILE VALIDATED','[{fileNameNew}] : File is validated')"""
                job = client.query(query, location="US")
                result = job.result()
                print(
                    f"Inserted successfully validated file record in bq DATA_MIGN_DGF_TRACE table. \nInserted records are JOB_ID: DGF_{now},"
                    f" USER_ID: {userID}, STEP_CODE: DGF_SPLIT_DC, STEP_DETAILS: FILE VALIDATED, "
                    f"COMMENTS: [{fileNameNew}] : File is validated")

                print(
                    f"Inserting a parent file DATA_CLASSIFICATION and file name {fileNameNew} record into bq table DATA_MIGN_DATA_CLASS_LOGS")
                query = f"""INSERT INTO `{project}.{dataset}.DATA_MIGN_DATA_CLASS_LOGS`(PARENT_FILE,FILE_NAME,FILE_RECIEVED_DATE,TOTAL_RECORDS) 
                                values('DATA_CLASSIFICATION','{fileNameNew}',CURRENT_DATETIME(),{total_rows})"""
                job = client.query(query, location="US")
                result = job.result()
                print(
                    f"Inserted filename and total rows record in bq DATA_MIGN_DATA_CLASS_LOGS table. \nInserted records are PARENT_FILE: DATA_CLASSIFICATION,"
                    f"FILE_NAME: {fileNameNew}, TOTAL_RECORDS: {total_rows}")

                if total_rows:
                    splitFile(data, workingPath, fileNameNew, now, csvFile)
                    print(
                        "The file is splitted based on number of projects in the source file and the records has been inserted into the queue based on target_bucket.properties")
                else:
                    print(f"DC file {fileNameNew} don't have any records. Hence skipping the file splitting but trying"
                          f" to deliver if old queue is present.")

                print("\tCalling Delivery Module to deliver the files from queues")
                time.sleep(5)
                print("Deleting the source file {} after file splits done.".format(filename))
                remove_file(filename)
                print("File {} has been deleted from the source location".format(filename))
                delivery_module(now, csvFile, fileNameNew)

            elif not total_cols:
                print("\tFile is empty. Trying to transfer if old queues are present.")
                print("Deleting the source file {}".format(filename))
                remove_file(filename)
                print("Calling delivery module function.")
                delivery_module(now, csvFile, fileNameNew)

            else:
                print(
                    "\n\t*******validation failed, Incorrect File Format!!...{} at timestamp {} by user id {}.*******".format(
                        fileNameNew, now, userID)
                )
                query = f"""INSERT INTO `{project}.{dataset}.DATA_MIGN_DGF_TRACE`(JOB_ID,USER_ID,ENTRY_DT,STEP_CODE,STEP_DETAILS,COMMENTS) 
                            values(concat('DGF_','{now}'),'{userID}',CURRENT_DATETIME(),'DGF_SPLIT_DC','FILE REJECTED','[{fileNameNew}] : File rejected due to invalid file format')"""
                job = client.query(query, location="US")
                result = job.result()

                print(f"\t\tInserted DC file failed validation record into DATA_MIGN_DGF_TRACE. Records are"
                      f" JOB_ID: DGF_{now},USER_ID: {userID},STEP_CODE: DGF_SPLIT_DC,STEP_DETAILS: FILE REJECTED,"
                      f" COMMENTS:[{fileNameNew}] : File rejected due to invalid file format")
                currentDate = datetime.now().strftime("%Y_%m_%d_%H:%M:%S")
                copy_blob(currentDate, 'failed', csvFile, failedFolder)
                remove_file(filename)
        except Exception as e:
            error = str(e).split("\n")[0]
            print("Exception has been occured : {}".format(e))

            query = f"""INSERT INTO `{project}.{dataset}.DATA_MIGN_DGF_TRACE`(JOB_ID,USER_ID,ENTRY_DT,STEP_CODE,STEP_DETAILS,COMMENTS) 
                            values(concat('DGF_','{now}'),'{userID}',CURRENT_DATETIME(),'DGF_SPLIT_DC','FILE NOT PROCESSED','[{fileNameNew}] : {error}')"""

            job = client.query(query, location="US")
            result = job.result()
            print("tracing record has been inserted into DATA_MIGN_DGF_TRACE")

    elif f'{path}policy_term_treatments' in filename.lower() and all(x not in filename for x in folders):

        try:
            print("Fetching target bucket properties from bq.")
            get_target_projects_from_bq()

            query = f"""INSERT INTO `{project}.{dataset}.DATA_MIGN_DGF_TRACE`(JOB_ID,USER_ID,ENTRY_DT,STEP_CODE,STEP_DETAILS,COMMENTS) 
                            values(concat('DGF_','{now}'),'{userID}',CURRENT_DATETIME(),'DGF_SPLIT_PTT','POLICY_TERM_TREATMENTS FILE IN GCS COMMON BUCKET','[{csvFile}] :  File uploaded in the common bucket')"""
            job = client.query(query, location="US")
            result = job.result()

            query = f"""INSERT INTO `{project}.{dataset}.DATA_MIGN_DGF_TRACE`(JOB_ID,USER_ID,ENTRY_DT,STEP_CODE,STEP_DETAILS,COMMENTS) 
                            values(concat('DGF_','{now}'),'{userID}',CURRENT_DATETIME(),'DGF_SPLIT_PTT','CF TRIGGERED','Cloud Function triggered for [{csvFile}] file')"""
            job = client.query(query, location="US")
            result = job.result()

            copiedFile = copy_blob(now, 'PTT', csvFile)
            str1 = copiedFile.rfind('/')
            fileNameNew = format(copiedFile[str1 + 1:])
            # print("\n\tCreated copy for Policy_Term_Treatments file")

            try:
                data = pd.read_csv(f"{gcsPath}{csvFile}", encoding='latin1')
                print(f"\nNew PTT file {filename} in Bucket")

                ptt_file_cols = list(data.axes[1])
                file_cols = [x.lower() for x in ptt_file_cols]
                total_cols = len(file_cols)  # Count of columns in given csv
                total_rows = len(data.axes[0])  # Count of rows in given csv

            except:
                ptt_file_cols = []
                file_cols = []
                total_cols = 0
                total_rows = 0

            # Expected columns from PTT files
            # exp_cols = ['SECURITY_ELEMENT_ID', 'SECURITY_CLASS', 'SECURITY_CLASS_CATEGORY', 'DATA_ELEMENT',
            #             'PRIVILEGED', 'CLEANROOM_GSAM_FILTERED', 'REDACTED_GSAM_FILTERED', 'CLEANROOM_GSAM_NOT_FILTERED',
            #             'REDACTED_GSAM_NOT_FILTERED', 'ELEMENT_SOR', 'REVISED_GSAM_ELEMENTS', 'SENSITIVE_DATA_FLAG',
            #             'OTHERIDS']
            exp_cols = [x.lower() for x in
                        pttColumns]  # Fetching PTT file columns list from source config properties file
            print(
                f"validating PTT file by checking all file columns i.e {ptt_file_cols} should be match with expected "
                f"columns i.e {pttColumns} and total number of columns should be 13")
            if total_cols == 13 and all(_ in file_cols for _ in exp_cols):

                print(
                    "\n\t*******Validated incoming source file...{} at timestamp {} by user id {}.*******".format(
                        fileNameNew, now, userID)
                )
                final_ptt_cols = []
                for colm in exp_cols:
                    final_ptt_cols.append(ptt_file_cols[file_cols.index(colm)])

                print(f"Rearranging file columns as per {final_ptt_cols} expected columns")
                data = data[final_ptt_cols]

                query = f"""INSERT INTO `{project}.{dataset}.DATA_MIGN_DGF_TRACE`(JOB_ID,USER_ID,ENTRY_DT,STEP_CODE,STEP_DETAILS,COMMENTS) 
                                values(concat('DGF_','{now}'),'{userID}',CURRENT_DATETIME(),'DGF_SPLIT_PTT','FILE VALIDATED','[{fileNameNew}] : File is validated')"""
                job = client.query(query, location="US")
                result = job.result()
                print(f"Inserted file validated records into the DATA_MIGN_DGF_TRACE table. Records are "
                      f"JOB_ID: 'DGF_'{now}, USER_ID: {userID}, STEP_CODE: DGF_SPLIT_PTT, STEP_DETAILS: FILE VALIDATED,"
                      f" COMMENTS: [{fileNameNew}] : File is validated'")

                query = f"""INSERT INTO `{project}.{dataset}.DATA_MIGN_DATA_CLASS_LOGS`(PARENT_FILE,FILE_NAME,FILE_RECIEVED_DATE,TOTAL_RECORDS) 
                                values('Policy_Term_Treatments','{fileNameNew}',CURRENT_DATETIME(),{total_rows})"""
                job = client.query(query, location="US")
                result = job.result()
                print(f"PTT file {fileNameNew} have total {total_rows} records")

                csv_name = 'Policy_Term_Treatments'

                if total_rows:
                    remove_old_queue()
                    split_csv_name = csv_name + "_" + now + ".csv"
                    data.to_csv(workingPath + split_csv_name, index=False, header=pttColumns)
                    print(f"Inserting PTT records in DATA_MIGN_SPLT_DLVY_QUEUE for all target projects")
                    for project_name in target_cfg_dict:
                        if target_cfg_dict[project_name]['status'] == 'ACTIVE':
                            delv_module = target_cfg_dict[project_name]['module_name']
                            query = f"""INSERT INTO `{project}.{dataset}.DATA_MIGN_SPLT_DLVY_QUEUE`
                                values('{csvFile}',CURRENT_DATETIME(),'{delv_module}','{project_name}','{split_csv_name}',CURRENT_DATETIME(),'{workingPath}',cast(NULL as DATETIME),'','','')"""
                            job = client.query(query, location="US")
                            job.result()

                            print(
                                f"\n\tInserted records in DATA_MIGN_SPLT_DLVY_QUEUE. Records are SPLIT_FILE_NAME: {split_csv_name}, DELIVERY_PROJECT_ID: {project_name} , SPLIT_FILE_LOCATION: {workingPath}, DELIVERY_MODULE: {delv_module}")
                else:
                    print(f"PTT file {fileNameNew} don't have any records, Hence skipping the creating new queue. "
                          f"But trying to transfer old queue if any present.")
                print("\tCalling delivery_module")
                time.sleep(5)
                remove_file(filename)
                delivery_module(now, csvFile, fileNameNew)
                print("files has been delivered. hence, deleting the source file {}".format(filename))
            elif not total_cols:
                print("\tFile is empty. Trying to transfer if old queues are present.")
                print("Deleting the source file {}".format(filename))
                remove_file(filename)
                print("Calling delivery module function.")
                delivery_module(now, csvFile, fileNameNew)

            else:
                print("\t\tIncorrect File Format or columns are not matching as expected.!!")
                query = f"""INSERT INTO `{project}.{dataset}.DATA_MIGN_DGF_TRACE`(JOB_ID,USER_ID,ENTRY_DT,STEP_CODE,STEP_DETAILS,COMMENTS) 
                                    values(concat('DGF_','{now}'),'{userID}',CURRENT_DATETIME(),'DGF_SPLIT_DC','FILE REJECTED','[{fileNameNew}] : File rejected due to invalid file format')"""
                job = client.query(query, location="US")
                result = job.result()
                print(f"\t\tInserted PTT file failed validation record into DATA_MIGN_DGF_TRACE. Records are"
                      f" JOB_ID: DGF_{now}, USER_ID: {userID}, STEP_CODE: DGF_SPLIT_PTT, STEP_DETAILS: FILE REJECTED,"
                      f" COMMENTS:[{fileNameNew}] : File rejected due to invalid file format")
                currentDate = datetime.now().strftime("%Y_%m_%d_%H:%M:%S")
                copy_blob(currentDate, 'failed', csvFile, failedFolder)
                remove_file(filename)

        except Exception as e:
            print(f"Exception occurred in hello_gcp function and PTT section. Error: {e}")
            error = str(e).split("\n")[0]
            query = f"""INSERT INTO `{project}.{dataset}.DATA_MIGN_DGF_TRACE`(JOB_ID,USER_ID,ENTRY_DT,STEP_CODE,STEP_DETAILS,COMMENTS) 
                            values(concat('DGF_','{now}'),'{userID}',CURRENT_DATETIME(),'DGF_SPLIT_PTT','FILE NOT PROCESSED','[{fileNameNew}] : {error}')"""
            job = client.query(query, location="US")
            result = job.result()

    elif f'{path}target_bucket' in filename.lower():
        try:
            target_file = re.sub(path, '', filename)
            target_file_path = path + target_file
            print(f"Reading {target_file_path} file")

            bucket = storage_client.get_bucket(source_bucket)
            blob = bucket.blob(target_file_path)
            # Download the contents of the file as a string
            file_contents = blob.download_as_string()

            conf_dict = eval(file_contents)

            exist_query = f"""SELECT COUNT(1) AS table_exists FROM `{project}.{dataset}.__TABLES_SUMMARY__` WHERE table_id = 'DATA_MIGN_TARGET_BUCKET_PROPERTIES'"""
            resp = client.query(exist_query, location="US")
            resp.result()
            table_exist = [row.table_exists for row in resp][0]

            if not table_exist:
                print(f"Creating DATA_MIGN_TARGET_BUCKET_PROPERTIES table in bq {project}.{dataset}")
                delta_query = f"""CREATE TABLE {project}.{dataset}.DATA_MIGN_TARGET_BUCKET_PROPERTIES (PROJECT_NAME STRING, MODULE_NAME STRING, DELIVERY_PATH STRING, CHECK_902_FLAG STRING, STATUS STRING, ENTRY_DT DATETIME, FILENAME STRING)"""
                job = client.query(delta_query, location="US")
                job.result()

            for prj in conf_dict:
                project_name = prj
                module_name = list(conf_dict[prj].keys())[0]
                delivery_path = conf_dict[prj][module_name]
                check_902_flag = conf_dict[prj].get('CHECK_902_FLAG', 'FALSE')
                if check_902_flag not in ['TRUE', 'FALSE']:
                    raise Exception("User should give CHECK_902_FLAG value TRUE/FALSE")

                status = conf_dict[prj].get('STATUS', 'ACTIVE')
                if status not in ['ACTIVE', 'INACTIVE']:
                    raise Exception("User should provide STATUS value as ACTIVE/INACTIVE.")
                print(
                    f"For {project_name} Target bucket property details inserted into DATA_MIGN_TARGET_BUCKET_PROPERTIES table.")
                print(
                    f"Inserted details :- Project Name : {project_name}, Module Name : {module_name}, Delivery Path : {delivery_path}, Check 902 flag : {check_902_flag} and Status : {status}")
                merge_query = f"""MERGE INTO `{project}.{dataset}.DATA_MIGN_TARGET_BUCKET_PROPERTIES` DATA
                                USING (
                                    SELECT '{project_name}' as PROJECT_NAME, '{module_name}' as MODULE_NAME,
                                    '{delivery_path}' as DELIVERY_PATH,
                                    '{check_902_flag}' as CHECK_902_FLAG, '{status}' as STATUS,
                                    CURRENT_DATETIME() as ENTRY_DT, '{filename}' as FILENAME 
                                    ) AS source
                                ON DATA.PROJECT_NAME=source.PROJECT_NAME
                                WHEN MATCHED THEN
                                    UPDATE SET PROJECT_NAME=source.PROJECT_NAME,MODULE_NAME = source.MODULE_NAME, DELIVERY_PATH = source.DELIVERY_PATH,
                                    CHECK_902_FLAG=source.CHECK_902_FLAG,STATUS=source.STATUS, ENTRY_DT=source.ENTRY_DT, FILENAME=source.FILENAME
                                WHEN NOT MATCHED THEN
                                    INSERT (PROJECT_NAME, MODULE_NAME, DELIVERY_PATH, CHECK_902_FLAG, STATUS, ENTRY_DT, FILENAME)
                                    VALUES (PROJECT_NAME, MODULE_NAME, DELIVERY_PATH, CHECK_902_FLAG, STATUS, ENTRY_DT, FILENAME);
                                """
                job = client.query(merge_query, location="US")
                job.result()

            currentDate = datetime.now().strftime("%Y_%m_%d_%H:%M:%S")

            copy_blob(currentDate, 'target', csvFile, targetFolder)
        except Exception as e:
            print(f"Failed in creating table DATA_MIGN_TARGET_BUCKET_PROPERTIES. Error : {e}")
            currentDate = datetime.now().strftime("%Y_%m_%d_%H:%M:%S")
            copy_blob(currentDate, 'failed', csvFile, failedFolder)
        finally:
            remove_file(filename)
    else:
        print(
            "Please give the proper file names like \n1.data_classification \n2.policy_term_treatments \n3.target_bucket.")
        currentDate = datetime.now().strftime("%Y_%m_%d_%H:%M:%S")
        copy_blob(currentDate, 'failed', csvFile, failedFolder)
        remove_file(filename)


def create_file_to_trigger_bucket(data):
    file = data["name"]
    num = 0
    if "retrigger" in file:
        num = int(file.split('retrigger_')[1].strip(".csv"))

    if num <= 2:
        num += 1
        print(f"Restarting the cloud function for '{num}' times")
        df = pd.DataFrame()
        re_file_name = "data_classification_retrigger_" + str(num) + ".csv"
        triger_file = workingPath + re_file_name
        df.to_csv(triger_file, index=False)
        source_file = WorkingFolder + re_file_name

        src_bucket = storage_client.bucket(bucketName)
        source_blob = src_bucket.blob(source_file)
        destination_bucket = storage_client.bucket(source_bucket)

        blob_copy = src_bucket.copy_blob(
            source_blob, destination_bucket, re_file_name
        )


def hello_gcs(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """

    # Example usage
    def timeout_callback():
        create_file_to_trigger_bucket(event)

    # Create a watchdog timer with a timeout of 3 seconds
    watchdog = WatchdogTimer(537, timeout_callback)
    hello_gcs_start(event)
    # Cancel the watchdog timer
    watchdog.cancel()
