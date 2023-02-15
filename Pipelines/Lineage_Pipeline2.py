# Import pandas and databand libraries
import pandas as pd
from dbnd import dbnd_tracking, task, dataset_op_logger

databand_url = 'insert_url'
databand_access_token = 'insert_token'

# Data used in this pipeline
INPUT_FILE = 'https://raw.githubusercontent.com/elenalowery/data-samples/main/Camping_Equipment.csv'

# Provide a unique suffix that will be added to various assets tracked in Databand. We use this approach because
# in a workshop many users are running the same sample pipelines,for example, '_mi'
unique_suffix = ''

@task
def read_sales_data():

    # Unique name for logging - this matches the name used in LineagePipeline1
    unique_file_name = 'local://Weekly_Sales/Camping_Equipment.csv' + unique_suffix

    # Log the data read
    with dataset_op_logger(unique_file_name, "read", with_schema=True, with_preview=True, with_stats=True,
                           with_histograms=True, ) as logger:
        retailData = pd.read_csv(INPUT_FILE)
        logger.set(data=retailData)

    return retailData

@task
def write_data_by_state(salesData):

    # Unique name for logging
    unique_file_name = 'local://Weekly_Sales/Arizona_Camping_Equipment.csv' + unique_suffix

    # Select any product line - we will write it to a separate file
    salesByState = salesData.loc[salesData['State'] == 'Arizona']
    
    # Log the filtered data read
    with dataset_op_logger(unique_file_name, "write", with_schema=True, with_preview=True) as logger:
        salesByState.to_csv("Arizona_Camping_Equipment.csv", index=False)
        logger.set(data=salesByState)

# Call and track all steps in a pipeline

def prepare_retail_data():
    with dbnd_tracking(
            conf={
                "core": {
                    "databand_url": databand_url,
                    "databand_access_token": databand_access_token,
                }
            },
            job_name="lineage_pipeline_2" + unique_suffix,
            run_name="weekly",
            project_name="Lineage Test" + unique_suffix,
    ):

        #Call the step job - read data
        salesData = read_sales_data()

        # Write data by product line
        write_data_by_state(salesData)

        print("Finished running the pipeline")

# Invoke the main function
prepare_retail_data()
