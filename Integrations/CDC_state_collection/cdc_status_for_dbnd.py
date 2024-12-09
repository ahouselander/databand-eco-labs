# Script to retrieve status of CDC subscriptions and post results to IBM Databand
#
# This script has been provided as an example of how to collect information from IBM CDC and post status into IBM Databand 
# It is provided as an example only and is supplied with no warranty or commitment to support
#
import os
import subprocess
import sys

import gzip
import json
import uuid

from datetime import datetime, timedelta

# pytz install may not be required as might be part of pythin environment
# pip install pytz 
import pytz
import requests
pad = '=='
padpad = pad*40
print(padpad)
print("Python Program to print list the files in a directory.")

# Global variables and set up
# Adjust the value of 'insancdedir' and 'executabledir' to reflect the location of the Capture Agent 
instancedir = "/opt/cdcdb2/instance" 
executabledir = '/opt/cdcdb2/bin/'
delimeter = ' : '
invalid_cdc_states = 'Failed Inactive Unknown'

# Databand access token and url of the API 
# These parameters come from the Integrations screen on the Databand UI when selecting Custom integration
# They need to be changed for the PoC environment
ACCESS_TOKEN = "insert=token-here"
DATABAND_CUSTOM_INTEGRATION_FULL_API = "insert integration url here"

# Set up default values used in the Databand notification - these will be overwritten below
cdc_stat = 'COMPLETE'
project_name = 'adrianh_API_project'
run_name = 'Instance name'
name_space = 'adrianh_run_namespace'
name_space2 = 'adrianh_run_namespace2'
dag_name = 'adrianh_CDC_monitor'
task_name = 'CDC subscription name'
error_message = 'insert error here'


# This function uses standard routine available on each CDC agent to retrieve a list of subscriptions running on that instance
# There might be multiple instances, but only a subset are probably going to be monitored
# The -A parameter is used to retrieve all subscriptions, but this could be hard coded to fetch for a single subscription
def call_to_cdc(instance):
    command=executabledir +'dmgetsubscriptionstatus -I '+ instance + ' -A'
    print(f"Now processing CDC command: {command}")
    os.system(command)
 
    # Capture the output of the command using subprocess
    result = os.popen(command).read()
    return result

# not used
def pass_to_databand():
    # Print the result
    #print("Output from command:", result)
    
    print(f"Command response has  data type {(type(result))}")
    print(f"and has {len(result.splitlines())} lines" )
    return

# This function sets up the payload for the 'push to Databand' API which is based upon Open Lineage 
# It has many fields, but for simple monitoring, some are optional
def setup_databand_payload():
    run_uid = uuid.uuid4()
    simple_payload = [
        {
            "eventType": cdc_stat,
            "eventTime": datetime.utcnow().replace(tzinfo=pytz.utc),
            "inputs": [],
            "job": {"facets": {}, "namespace": name_space, "name": dag_name },
            "outputs": [],
            "run": {
                "facets": {
                    "nominalTime": {
                        "_producer": "https://some.producer.com/version/1.0",
                        "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/NominalTimeRunFacet.json",
                        "nominalStartTime": datetime.utcnow().replace(tzinfo=pytz.utc),
                    },
                    "log": {
                        "_producer": "https://some.producer.com/version/1.0",
                        "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/BaseFacet",
                        "logBody": "very helpful log.. and very long",
                        "logUrl": "https://bucket.s3.somewhere.com/.../file.log",
                    },
                    "startTime": {
                        "_producer": "https://some.producer.com/version/1.0",
                        "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/BaseFacet",
                        "startTime": datetime.utcnow().replace(tzinfo=pytz.utc)
                        - timedelta(seconds=15),
                    },
                    "errorMessage": {
                        "_producer": "https://some.producer.com/version/1.0",
                        "_schemaURL": "https://www.ibm.com/docs/en/idr/11.4.0?topic=replication-troubleshooting",
                        "message": error_message,
                        "programmingLanguage": "JAVA/python",
                        "stackTrace": 'No trace info specified',
                    },
                    "tags": {
                        "projectName": project_name,
                        "runName": run_name,
                        "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/BaseFacet",
                        "_producer": "https://some.producer.com/version/1.0",
                    },
                },
                "runId": run_uid,
            },
            "producer": "https://custom.api",
            "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent",
        },
        {
            "eventTime": datetime.utcnow().replace(tzinfo=pytz.utc),
            "eventType": cdc_stat,
            "job": {
                "facets": {},
                "namespace": name_space2 ,
                "name": task_name ,
            },
            "run": {
                "facets": {
                    "nominalTime": {
                        "_producer": "https://some.producer.com/version/1.0",
                        "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/SQLJobFacet.json",
                        "nominalStartTime": datetime.utcnow().replace(tzinfo=pytz.utc),
                    },
                    "log": {
                        "_producer": "https://some.producer.com/version/1.0",
                        "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/BaseFacet",
                        "logBody": "very helpful log.. and very long",
                        "logUrl": "https://bucket.s3.somewhere.com/.../file.log",
                    },
                    "startTime": {
                        "_producer": "https://some.producer.com/version/1.0",
                        "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/BaseFacet",
                        "startTime": datetime.utcnow().replace(tzinfo=pytz.utc)
                        - timedelta(seconds=15),
                    },
                    "errorMessage": {
                        "_producer": "https://some.producer.com/version/1.0",
                        "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/ErrorMessageRunFacet.json",
                        "message": error_message,
                        "programmingLanguage": "JAVA/python",
                        "stackTrace": 'No stack trace information provided',
                    },
                    "parent": {
                        "_producer": "https://some.producer.com/version/1.0",
                        "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/ParentRunFacet",
                        "job": {"name": dag_name, "namespace": name_space },
                        "run": {"runId": run_uid},
                        },
                },
                "runId": uuid.uuid4(),
            },
            "producer": "https://custom.api",
            "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent",
        },
    ]
    return simple_payload



# Main body

# Firstly, look for instances of the capture/apply agent
print("")
print(f"Looking for directories in the specified instance directory: {instancedir}")
print("") 
files = os.listdir(instancedir)

# Filtering only the directories
files = [f for f in files if os.path.isdir(instancedir+'/'+f)]

# We now have a list of instances (directories) 
for x in files:
     run_name = "CDC Instance " + x
     print(padpad)
     print("Processing instance: " + x)

     # retrieve subscriptions and their status within this instance
     subscriptiondetails = call_to_cdc(x)
     print(f"Retrieved subscription ============== \n{subscriptiondetails}")
     i = 0
     #
     # Cycle through subscription status output and extract status and name
     for responseline in subscriptiondetails.splitlines():
         i = i+1
         print(f"Interpreting Line {i} {responseline}")
         if i == 1:
             subslabel, success, subsname = responseline.partition(delimeter)
             if not success or subslabel != 'Subscription':  
                 print(f"Unexpected response <{subslabel}>") 
                 exit(1)
         elif i == 2:
             statlabel, success, status   = responseline.partition(delimeter)
             if not success or statlabel != 'Status      ' : 
                 print(f"Unexpected response <{statlabel}>") 
                 exit(2)
         else:
             pass
     # We should now know the name of the subscription and its status
     # First set the appropriate databand status then assign appropriate values to other variables which go into payload
     if status.strip() in invalid_cdc_states: cdc_stat = 'FAIL'
     else: cdc_stat = 'COMPLETE'

     error_message = "The subscription has a status of: " + status  
     task_name = "Subscription " + subsname

     # Now call the function to set up the payload
     simple_payload = setup_databand_payload()

     # not post the payload to Databand
     print("Pushing status into Databand")
     resp = requests.post(
         DATABAND_CUSTOM_INTEGRATION_FULL_API,
         data=gzip.compress(json.dumps(simple_payload,default=str).encode("utf-8")),
         headers={
            "Authorization": f"Bearer {ACCESS_TOKEN}",
            "Content-Type": "application/json",
            "Content-Encoding": "gzip",
         },
         timeout=30,
     )
     if resp.ok:
         print("success")
     else:
         resp.raise_for_status()    

# We should now have cycled through the instances and subscriptions, pushing status to IBM Databand
print("Finished")
print(padpad)