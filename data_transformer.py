import os
import subprocess
import sys
import json
import boto3
import yfinance as yf

#Ensuring we've installed the yfinance module
subprocess.check_call([sys.executable, 
    "-m", 
    "pip", 
    "install", 
    "--target", 
    "/tmp", 
    'yfinance'])

sys.path.append('/tmp')

def lambda_handler(event, context):
    start_date = '2021-05-11'
    end_date = '2021-05-12'
    interval = '1m'
    period= "1d"
    company_list = ['fb', 'shop', 'bynd', 'nflx', 'pins', 'sq', 'ttd', 'okta', 'snap', 'ddog']
    fh = boto3.client("firehose", "us-east-2")

    for company in company_list:
        download = yf.Ticker(company).history(start=start_date, end=end_date, interval=interval)

        for index, rows in download.iterrows():
            as_jsonstr = json.dumps({'name': company, "high": rows.High, "low": rows.Low, "timestamp": str(index)})+"\n"
            fh.put_record(DeliveryStreamName="STA9760-Project3FHv2", 
                          Record={"Data": as_jsonstr.encode('utf-8')})

    return {
        'statusCode': 200,
        'body': json.dumps(f'Completed putting record(s)! Recorded to bucket.')
    }
