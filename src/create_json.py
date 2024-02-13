import logging
import boto3
from botocore.exceptions import ClientError
import json 
import datetime 
from random import randint
import awswrangler as wr

logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)
logger.info('THIS IS B4 EVERITHING')

def dummy(event, context):

    logger.info('DUMMY LAMBDA INVOKED')
   
    ct = datetime.datetime.now()
    ts = str(ct.timestamp())

    con = wr.postgresql.connect(secret_id = "totesys_db")    
    
    df = wr.postgresql.read_sql_table(
    table="department",
    schema="public",
    con=con
    )
    con.close()

    data_dict = df.to_dict(orient='records')
    [logger.info(x) for x in data_dict]

    s3 = boto3.client('s3')
    for i,x in enumerate(data_dict):
        x['created_at'] = str(x['created_at'])
        x['last_updated'] = str(x['created_at'])
        s3.put_object(
            Body=json.dumps(x, indent=2),
            #Hard coded s3 bucket, change to new bucket name after every build
            Bucket='ingestion-zone-895623xx35',
            Key = ts+str(randint(1,10000))+'.json',
        )
    logger.info('DUMMY LAMBDA G-O-O-D-B-Y-E')
        
if __name__ == "__main__":
    dummy('s','s')