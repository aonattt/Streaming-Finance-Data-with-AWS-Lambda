import json
import boto3
import yfinance as yf


sDate = '2021-05-11'
eDate = '2021-05-12'
Interval = '5m'   
tickers = ['FB', 'SHOP', 'BYND', 'NFLX', 'PINS', 'SQ', 'TTD', 'OKTA', 'SNAP', 'DDOG']


kstream= boto3.client('kinesis', "us-east-2")

def lambda_handler(event, context):
    for ticker in tickers:
        data = yf.download(ticker, start=sDate, end=eDate, interval = Interval, group_by = 'ticker')
        for timestamp, rec in data.iterrows():
            record = {
              'high': rec['High'],
              'low': rec['Low'],
              'ts': str(timestamp), 
              'name': ticker
              }
            
            recJSON = json.dumps(record)+"\n"
            
            kstream.put_record(
                StreamName="Project3Stream",
                Data=recJSON,
                PartitionKey="partitionkey"
                )
    return {
        'statusCode': 200,
        'body': json.dumps("Hello from Lambda!")
    }