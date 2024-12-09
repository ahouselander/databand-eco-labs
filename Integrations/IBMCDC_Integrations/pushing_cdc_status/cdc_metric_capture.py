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
import argparse
# 
from dbnd import dbnd_tracking, task, dataset_op_logger, log_metric
from datetime import datetime, timedelta

# THE FOLLOWING VARIABLES NEED TO BE CHECKED FOR YOUR ENVIRONMENT
executabledir = '/opt/cdcaccess/bin/'
workingdir = '/home/cdcaccess/'
job_name_prefix_dbnd = 'CDC metrics '
project_name_dbnd = 'Metrics collection'
# Specify URL of target Databand and access token
# The following parameters have already been set
databand_url = 'https://your-databand-url.databand.ai'        
databand_access_token = 'insert your databand access token here'
# END OF VARIABLES


@task
def push_cdc_metrics():
        tmstmp = datetime.now()
        log_metric('Subscription Name',subsname)
        log_metric('Source Inserts',c_src_ins)
        log_metric('Source Updates',c_src_upd)
        log_metric('Source Deletes',c_src_del)
        log_metric('Target Inserts',c_tgt_ins)
        log_metric('Target Updates',c_tgt_upd)
        log_metric('Target Deletes',c_tgt_del)
        log_metric('Timestamp Info',tmstmp)


def push_to_databand():
    with dbnd_tracking(
            conf={
                "core": {
                    "databand_url": databand_url,
                    "databand_access_token": databand_access_token,
                }
            },
            job_name=job_name_prefix_dbnd + subsname,
            run_name="hourly",
            project_name=project_name_dbnd ,
    ):
        push_cdc_metrics()
 

def handle_file(fname):
  with open(fname,'r') as file:
    for line in file:
        iii = line.find("inserts")
        if iii > 0: 
           tt = line.split("inserts")
           inscount=int((tt[1].strip()))

        uuu = line.find("updates")
        if uuu > 0: 
           tt = line.split("updates")
           updcount=int((tt[1].strip()))

        ddd = line.find("deletes")
        if ddd > 0: 
           tt = line.split("deletes")
           delcount=int((tt[1].strip()))
    
    return inscount, updcount, delcount


def call_to_chcclp_capture():        
    # First form up chcclp command to collect Capture status for this subscription
    command=executabledir+'chcclp -f '+capture_chclp + " > " + cdc_capture_output_full
    print(f"Now processing CDC command: {command}")
    os.system(command)
    # Capture the output of the command using subprocess
    fcresult = os.popen(command).read()

    command="cat "+ cdc_capture_output_full + " | grep -E 'post-filter' > " + cdc_capture_output
    print(f"Now processing grep command: {command}")
    os.system(command)
    cresult = os.popen(command).read()
    
    return cresult


def call_to_chcclp_apply():          
    # Now form up chcclp command to collect Apply status for this subscription
    command=executabledir+'chcclp -f '+apply_chclp + " > " + cdc_apply_output_full
    print(f"Now processing CDC command: {command}")
    os.system(command)
    # Capture the output of the command using subprocess
    faresult = os.popen(command).read()
    
    command="cat "+ cdc_apply_output_full + " | grep -E 'post-filter' > " + cdc_apply_output
    print(f"Now processing grep command: {command}")
    os.system(command)
    cresult = os.popen(command).read()

    return aresult


def move_to_prev():                  
    command="cp "+ cdc_capture_output + " " + cdc_capture_prev  
    print(f"Now processing cp command {command}")
    os.system(command)
    cpresult = os.popen(command).read()
    command="cp "+ cdc_apply_output + " " + cdc_apply_prev  
    print(f"Now processing cp command: {command}")
    os.system(command)
    cpresult = os.popen(command).read()

# Main code starts here
# Main code starts here
# Main code starts here
pad = '=='
padpad = pad*35
print(padpad)
print("Python script to grab CDC stats and insert into Databand via log_metric SDK ")

# Start by validating the input parameters
parser=argparse.ArgumentParser(description="Required parameters")
parser.add_argument("subscriptionname")
args=parser.parse_args()
print("Input Subscription name is ==>",args.subscriptionname)
subsname = args.subscriptionname

# Now test to see if the subscription has a chcclp input file for Capture and Apply
capture_chclp = args.subscriptionname + '_subsc_capture_stats.chcclp'
apply_chclp   = args.subscriptionname + '_subsc_apply_stats.chcclp'

if os.path.exists(capture_chclp):
    print(f"Input file {capture_chclp} exists")
else:
    print(f"Searched for {capture_chclp}")
    sys.exit("File does not exist")

if os.path.exists(apply_chclp):
    print(f"Input file {apply_chclp} exists")
else:
    print(f"Searched for {apply_chclp}")
    sys.exit("File does not exist")


# Now we know the chcclp input files exist, form up the names to capture the output 

cdc_capture_output_full = args.subscriptionname + '_capture_chcclp_out.txt'
cdc_capture_output      = args.subscriptionname + '_capture_out_filter.txt'
cdc_capture_prev        = args.subscriptionname + '_capture_out_filter.txt.prev'

cdc_apply_output_full   = args.subscriptionname + '_apply_chclp_out.txt'  
cdc_apply_output        = args.subscriptionname + '_apply_out_filter.txt'
cdc_apply_prev          = args.subscriptionname + '_apply_out_filter.txt.prev'


# Call chcclp to process capture side of subscription
outcap = call_to_chcclp_capture()
# Call chcclp to process apply side of subscription
outapp = call_to_chcclp_apply()


if os.path.exists(cdc_capture_prev):
    # Read the CAPTURE previous file to collect PREVIOUS metrics
    prev_capture = handle_file(cdc_capture_prev)
    #print(prev_capture)
    pci = prev_capture[0]
    pcu = prev_capture[1]
    pcd = prev_capture[2]
else:
    pci = 0
    pcu = 0
    pcd = 0

print("Previous capture I/U/D numbers:",pci,pcu,pcd)

if os.path.exists(cdc_apply_prev):
    # Read the APPLY previous file to collect PREVIOUS metrics
    prev_apply = handle_file(cdc_apply_prev)
    #print(prev_apply)
    pai = prev_apply[0]
    pau = prev_apply[1]
    pad = prev_apply[2]
else:
    pai = 0
    pau = 0
    pad = 0

print("Previous apply I/U/D numbers:",pai,pau,pad)

# Read the CAPTURE current file to collect CURRENT metrics
curr_capture = handle_file(cdc_capture_output)
#print(curr_capture)
cci = curr_capture[0]
ccu = curr_capture[1]
ccd = curr_capture[2]
print("Capture I/U/D numbers:",cci,ccu,ccd)

# Read the APPLY current file to collect CURRENT metrics
curr_apply = handle_file(cdc_apply_output)
#print(curr_apply)
cai = curr_apply[0]
cau = curr_apply[1]
cad = curr_apply[2]
print("Apply I/U/D numbers:",cai,cau,cad)

# Determine the DELTA statistics 
c_src_ins = cci - pci
c_src_upd = ccu - pcu
c_src_del = ccd - pcd
c_tgt_ins = cai - pai
c_tgt_upd = cau - pau
c_tgt_del = cad - pad

# Now call Databand to collect the metadata
push_to_databand()

# Copy run into to PREV file for next time
move_to_prev()

print(padpad)
