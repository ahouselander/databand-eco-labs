# Import pandas and databand libraries
import pandas as pd

def read_raw_data():
    
    transactionData = pd.read_csv('BankingTransactionsSummary.csv')
    
    return transactionData

def aggregate_transactions(rawData):
    
    # Aggregate transactions by account id
    aggregatedTransData = rawData.groupby(['account_id'],as_index=False).sum()
    
    return aggregatedTransData

def write_top_tier_accounts(aggregatedTransData):

    # Select accounts with aggregated transactions over 500,000
    topTierAccounts = aggregatedTransData.loc[aggregatedTransData['amount'] > 500000]
        
    topTierAccounts.to_csv("TopTierAccounts.csv", index=True)

# Call and track all steps in a pipeline

def prepare_transaction_data():

    try:
        # Call the step job - read data
        rawData = read_raw_data()

        # Aggregate transaction data
        aggregatedTrans = aggregate_transactions(rawData)

        # Write data by product line
        write_top_tier_accounts(aggregatedTrans)

        print("Finished running the pipeline")

    finally:
        pass

# Invoke the main function
prepare_transaction_data()
