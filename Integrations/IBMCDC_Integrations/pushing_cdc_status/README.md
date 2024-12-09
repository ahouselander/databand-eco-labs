PLEASE NOTE
Code shared here is purly an indication of one way to grab info and share it with Databand in for form of metrics.
No warranty or liability is implied.

CDC INSERT - UPDATE - DELETE metric sharing.

To do the IBM CDC integration I have used the CHCCLP which is a command line interface into IBM CDC (IIDR).
To do this I have been refreshing my knowledge in aspects of the CDC CHCCLP command line processing environment then coding a script to call the CHCCLP routine, 
process the output and push the output into a Databand Environment.
 
As you may already be aware, CDC uses a capture agent to collect change information from a source, and an apply agent which receives the changes. 
CDC provides a command line interface (CHCCLP https://www.ibm.com/docs/en/idr/11.4.0?topic=replication-command-line-processor-chcclp) to administer and monitor the CDC processing. 
 
There are also Java based Management Console APIs and a set of commands to create users etc. (https://www.ibm.com/docs/en/idr/11.4.0?topic=api-management-console)
There is also a public github. Repo built by a colleague sharing lots of useful Java based routines for all things CDC, here: https://github.com/fketelaars
 
I do not have any Java skills, so I chose to use Python can do the monitoring and Databand publish.
 
Note: The monitoring of CDC via CHCCLP will not effect the performance of the existing CDC processing, since it communicates with the appropriate capture/apply agents to collect metrics.
 
I have attached three files.
 
POSTGRES_subsc_capture_stats.chcclp
This is an example CHCCLP input batch file used as input to CHCCLP to collect metrics from the capture component. POSTGRES in the file name represents the name of the subscription to be tracked.
This file will need to be copied to a file of a similar name, but replacing POSTGRES with the name of your subscription, and editing the contents to specify the correct parameters.
IBM Databand does not see any of the parameter information you are entering here. 
The Python script calling this file, redirects the output, then does a grep to only pass through insert/update/delete information.

Note: Due to my restricted test environment, my actual subscription is DB2TOPOSTG if you look inside the file. 
 
 
POSTGRES_subsc_apply_stats.chcclp
This file is very similar to the capture equivalent, except that it points to the apply agent.
It also will need copying, renaming and editing to match your environment as per the Capture.
 
A Python script which accepts the subscription name as a parameter.
This python script makes sure that the <subscription>_subsc_capture_stats.chcclp and <subscription>_subsc_apply_stats.chcclp files exist.
It then uses these two files to call as input to CHCCLP (once for capture, once for apply) redirecting the output to a  file. This file is then filtered to remove unwanted detail and then read to retrieve Insert/Update/Delete information. This info is then pushed into Databand as metrics.
The python script requires updating to reflect directory structures.
 
Strictly speaking, there is typically no reason to track both the capture and apply.
The IIDR CDC processing means that the Capture and Apply agents communicate with each other via TCPIP and there should be no loss of information.
If it is not possible to collect information from Capture (or Apply), it is normally sufficient to collect just one, except when filtering is being used on Capture side. The CHCLLP capture monitoring provided pre & post filtering statistics which could differ in the unusual case of Capture filters being used.
If it is not possible to collect capture information, the python script can be adjusted.
 
How did I test this?
I installed Databand python library via under root access (the owner of Python): pip3 install dbnd
 
I signed into the Linux CDC system and switched to the userid that admin rights over cdc directories.
I then put the files into this directory on my server: /home/cdcaccess/
 
I then ran the following (ignore the actual Numbers)
    python3 cdc_metric_capture.py POSTGRES

======================================================================
Python script to grab CDC stats and insert into Databand via log_metric SDK
Input Subscription name is ==> POSTGRES
Input file POSTGRES_subsc_capture_stats.chcclp exists
Input file POSTGRES_subsc_apply_stats.chcclp exists
Previous capture I/U/D numbers: 0 0 0
Previous apply I/U/D numbers: 0 0 0
Capture I/U/D numbers: 0 4 0
Apply I/U/D numbers: 0 2 0
DBND: Starting Tracking with DBND(1.0.25.2)
Now processing cp command cp POSTGRES_capture_out_filter.txt POSTGRES_capture_out_filter.txt.prev
Now processing cp command: cp POSTGRES_apply_out_filter.txt POSTGRES_apply_out_filter.txt.prev
======================================================================

Thats it.
Looking at the Databand UI should show the insert/update/delete actions.

![image](https://github.com/user-attachments/assets/6d6c752b-5dcc-46f1-956f-7de2d21e69da)
