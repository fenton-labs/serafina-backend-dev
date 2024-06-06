import json
import requests
import pandas
import sqlalchemy
import psycopg2
import dbconnect


def lambda_handler(event, context):
    """Sample pure Lambda function

    Parameters
    ----------
    event: dict, required
        API Gateway Lambda Proxy Input Format

        Event doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html#api-gateway-simple-proxy-for-lambda-input-format

    context: object, required
        Lambda Context runtime methods and attributes

        Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html

    Returns
    ------
    API Gateway Lambda Proxy Output Format: dict

        Return doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html
    """

    try:
        ip = requests.get("http://checkip.amazonaws.com/")
    except requests.RequestException as e:
        # Send some context about this error to Lambda Logs
        print(e)

        raise e
    
    output = readmin_main(20)
    print(output)
    
    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "hello world",
            # "location": ip.text.replace("\n", "")
        }),
    }

def readmin_main(readmin_period_threshold):
    connection = dbconnect.create_rds_connection('sqlalchemy')
    sql = f'''
        SELECT 
        init_clms."DESYNPUF_ID" as bene_id
        
    	,init_clms."AT_PHYSN_NPI" as admin_physn_npi
        ,end_clms."AT_PHYSN_NPI" as readmin_physn_npi
        ,CAST(CAST(init_clms."CLM_ADMSN_DT" as text ) AS timestamp) as admin_date
        ,CAST(CAST(end_clms."CLM_ADMSN_DT" as text ) AS timestamp) as readmin_date
    
    	,DATE_PART('day',
    		CAST(CAST(end_clms."CLM_ADMSN_DT" as text ) AS timestamp) -
    		CAST(CAST(init_clms."CLM_ADMSN_DT" as text ) AS timestamp)
    	) as readmin_period    -- ,"CLM_DRG_CD"
        ,init_clms."ICD9_DGNS_CD_1" as admin_icd9_dgns_id
        ,end_clms."ICD9_DGNS_CD_1" as readmin_icd9_dgns_id
        ,SUM(init_clms."CLM_UTLZTN_DAY_CNT") AS admin_length
        ,SUM(end_clms."CLM_UTLZTN_DAY_CNT") AS readmin_length
      FROM public.ip_claims init_clms
        JOIN public.ip_claims end_clms on 
            (init_clms."DESYNPUF_ID" = end_clms."DESYNPUF_ID")
      WHERE init_clms."CLM_ADMSN_DT" is not null
        AND DATE_PART('day',
    		CAST(CAST(end_clms."CLM_ADMSN_DT" as text ) AS timestamp) -
    		CAST(CAST(init_clms."CLM_ADMSN_DT" as text ) AS timestamp)
    	) < {readmin_period_threshold} 
        AND DATE_PART('day',
    		CAST(CAST(end_clms."CLM_ADMSN_DT" as text ) AS timestamp) -
    		CAST(CAST(init_clms."CLM_ADMSN_DT" as text ) AS timestamp)
    	) > 0
        AND init_clms."ICD9_DGNS_CD_1" = end_clms."ICD9_DGNS_CD_1"
    
      GROUP BY init_clms."DESYNPUF_ID"
        ,init_clms."AT_PHYSN_NPI"
        ,end_clms."AT_PHYSN_NPI"
        ,init_clms."CLM_ADMSN_DT"
        ,end_clms."CLM_ADMSN_DT"
        ,init_clms."ICD9_DGNS_CD_1"
        ,end_clms."ICD9_DGNS_CD_1"
      ORDER BY readmin_period DESC, admin_length DESC, readmin_length DESC
        '''
        
    df = pandas.read_sql(sql, connection)
    return df