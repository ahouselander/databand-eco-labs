# Import pandas and databand libraries
import pandas as pd
from dbnd import dbnd_tracking, task, dataset_op_logger, log_metric

databand_url = 'insert_url'
databand_access_token = 'insert_token'

# Data used in this pipeline
RETAIL_FILE = "https://raw.githubusercontent.com/elenalowery/data-samples/main/Retail_Products_and_Customers.csv"

# Provide a unique suffix that will be added to various assets tracked in Databand. We use this approach because
# in a workshop many users are running the same sample pipelines. For example '_mi'
unique_suffix = '_mi'

@task
def read_raw_data():

    # Unique name for logging
    unique_file_name = RETAIL_FILE + unique_suffix

    # Log the data read
    with dataset_op_logger(unique_file_name,"read",with_schema=True,with_preview=True,with_stats=True,with_histograms=True,) as logger:
        retailData = pd.read_csv(RETAIL_FILE)
        logger.set(data=retailData)

    return retailData

@task
def filter_data(rawData):

    unique_file_name = 'script://Weekly_Sales/Filtered_df' + unique_suffix

    # Drop a few columns
    filteredRetailData = rawData.drop(['Buy', 'PROFESSION', 'EDUCATION'], axis=1)

    with dataset_op_logger(unique_file_name, "read", with_schema=True, with_preview=True) as logger:
        logger.set(data=filteredRetailData)

    return filteredRetailData

@task
def write_data_by_product_line(filteredData):

    unique_file_name_1 = 'local://Weekly_Sales/Camping_Equipment.csv' + unique_suffix
    unique_file_name_2 = 'local://Weekly_Sales/Golf_Equipment.csv' + unique_suffix

    # Select any product line - we will write it to a separate file
    campingEquipment = filteredData.loc[filteredData['Product line'] == 'Camping Equipment1']

    # Log writing the Camping Equipment csv
    with dataset_op_logger(unique_file_name_1, "write", with_schema=True,
                           with_preview=True) as logger:
        # Write the csv file
        campingEquipment.to_csv("Camping_Equipment.csv", index=False)

        logger.set(data=campingEquipment)

    # Select any product line
    golfEquipment = filteredData.loc[filteredData['Product line'] == 'Golf Equipment']

    # Log the filtered data read
    with dataset_op_logger(unique_file_name_2, "write", with_schema=True,
                           with_preview=True) as logger:
        # Write the csv file
        golfEquipment.to_csv("Golf_Equipment.csv", index=False)

        logger.set(data=golfEquipment)


def check_camping_equipment(rawData):

    metric_name = 'Sales from Alaska' + unique_suffix

    numberOfCampingEquipment_records = rawData['State'].tolist().count('Alaska')
    print(numberOfCampingEquipment_records)

    log_metric(metric_name, numberOfCampingEquipment_records)

# Call and track all steps in a pipeline

def prepare_retail_data():
    with dbnd_tracking(
            conf={
                "core": {
                    "databand_url": databand_url,
                    "databand_access_token": databand_access_token,
                }
            },
            job_name="prepare_sales_data" + unique_suffix,
            run_name="weekly",
            project_name="Retail Analytics" + unique_suffix,
    ):
        # Call the step job - read data
        rawData = read_raw_data()

        # Filter data
        filteredData = filter_data(rawData)

        # Write data by product line
        write_data_by_product_line(filteredData)

        check_camping_equipment(rawData)

        print("Finished running the pipeline")


# Invoke the main function
prepare_retail_data()
