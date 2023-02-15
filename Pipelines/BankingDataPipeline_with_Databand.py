# Import pandas and databand libraries
import pandas as pd
from dbnd import dbnd_tracking, task, dataset_op_logger


@task
def read_raw_data():
    transactionData = pd.read_csv('BankingTransactionsSummary.csv')

    # Log the data read
    with dataset_op_logger("local://Daily_Bank_Trans/BankingTransactionsSummary.csv", "read", with_schema=True,
                           with_preview=True) as logger:
        logger.set(data=transactionData)

    return transactionData


@task
def aggregate_transactions(rawData):
    # Aggregate transactions by account id
    aggregatedTransData = rawData.groupby(['account_id'], as_index=False).sum()

    with dataset_op_logger("script://DailyBankTrans/Aggregated_df", "read", with_schema=True,
                           with_preview=True) as logger:
        logger.set(data=aggregatedTransData)

    return aggregatedTransData


@task
def write_top_tier_accounts(aggregatedTransData):
    # Select accounts with aggregated transactions over 500,000
    topTierAccounts = aggregatedTransData.loc[aggregatedTransData['amount'] > 500000]

    # Log the filtered data read
    with dataset_op_logger("local://Daily_Bank_Trans/Top_Tier_Accounts.csv", "write", with_schema=True,
                           with_preview=True) as logger:
        logger.set(data=topTierAccounts)

    topTierAccounts.to_csv("Top_Tier_Accounts.csv", index=True)


# Call and track all steps in a pipeline

def prepareTransactionData():
    with dbnd_tracking(
            conf={
                "core": {
                    "databand_url": "insert_url",
                    "databand_access_token": "insert_token",

                }
            },
            job_name="prepare_trans_data",
            run_name="daily",
            project_name="Banking Analytics",
    ):
        # Call the step job - read data
        rawData = read_raw_data()

        # Aggregate transaction data
        aggregatedTrans = aggregate_transactions(rawData)

        # Write data by product line
        write_top_tier_accounts(aggregatedTrans)

        print("Finished running the pipeline")


# Invoke the main function
prepareTransactionData()
