import json
import dbconnect
import requests
import pandas as pd
import boto3


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

    # try:
    #     ip = requests.get("http://checkip.amazonaws.com/")
    # except requests.RequestException as e:
    #     # Send some context about this error to Lambda Logs
    #     print(e)

    #     raise e
    data = rds_get()
    data = network_formatter(data)
    dynamo_write(data)
    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "hello world",
            # "location": ip.text.replace("\n", "")
        }),
    }

def rds_get():
    conn = dbconnect.create_rds_connection('sqlalchemy')
    # sql = '''
    #     select *
    #     from readmissions_20day'''

    # df_raw = pd.read_sql(sql, conn)

    # print(df_raw)

    sql = f'''
        WITH readmsns as
            (SELECT
                admin_physn_npi,
                readmin_physn_npi,  
                readmin_icd9_dgns_id, 
                COUNT(*) AS readmsn_cnt
            FROM public.readmissions_20day
            GROUP BY 
                admin_physn_npi,
                readmin_physn_npi, 
                readmin_icd9_dgns_id
            )
        SELECT readmsns.admin_physn_npi as source,
            readmsns.readmin_physn_npi as target,
            readmsns.readmin_icd9_dgns_id,
            readmsns.readmsn_cnt
        FROM readmsns
        '''
    
    df_edges = pd.read_sql(sql, conn)

    # print(df_edges)
    return df_edges

def network_formatter(data):
    npi_list = data['source'].append(data['target'])
    df_out = pd.DataFrame()
    df_out['provider'] = npi_list
    # df_out.columns 
    # print(df_out.columns)
    # print(npi_list.unique)

    # df_out.loc[0,'provider'] = "ABC"
    df_out['count'] = ""
    df_out['edges'] = ""

    for npi in npi_list:
        edges_tempTable = data[data['source'] == npi]
        # print(npi)
        totalCount = 0
        edgeCount = 0
        edgesObj = []

        for index, row in edges_tempTable.iterrows():
            # print(row)
            # totalCount = totalCount + row[1].loc['readmsn_cnt']
            edgesObjTemp = {
                f'edge{edgeCount}': {
                    'edge_npi': row['target'],
                    'edge_cnt': row['readmsn_cnt']
                }
            }
            edgesObj.append(edgesObjTemp)
            rdadmsnCntTemp = row['readmsn_cnt']
            edgeCount = edgeCount + row['readmsn_cnt']
        
        # pd.Series([100, 200.2], index=[0, 1])
        df_out.loc[df_out['provider'] == npi,'edges'] = pd.Series(edgesObj)
        df_out.loc[df_out['provider'] == npi,'count'] = edgeCount
        
        # for readmsnNPI in df_temp['target']:
        #     df_out['edges'] = readmsnNPI


    # print(df_out, "\nTotal Count: ", totalCount)
    return df_out

def dynamo_write(data):
    session = boto3.Session()
    client = session.client('dynamo')
    tables = client.list_tables()
    return


network_formatter(rds_get())