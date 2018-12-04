from celery import Celery, group
from download_carto_chunks.download_carto_dataset import download_chunks
from carto.sql import SQLClient
from carto.auth import APIKeyAuthClient

CARTO_BASE_URL= input("What is the name of the user account? ") # 'https://username.carto.com'
CARTO_API_KEY= input("What is the API KEY? ") # 'master api key or api key for the dataset'
organization= input("What is the name of the organization account? ") # 'org_name'
table_name =  input("What is the name of the dataset? ") #'table name in CARTO account'

# SQL wrapper
sql = SQLClient(APIKeyAuthClient(CARTO_BASE_URL, CARTO_API_KEY))

SPLIT_EXPORT = 500000
count_rows = "select count(*) from {}".format(table_name)

count_rows_query = sql.send(count_rows)
count_rows_result = count_rows_query['rows'][0]['count']

if count_rows_result > SPLIT_EXPORT:
    queries = []
    for i in range(1,count_rows_result,SPLIT_EXPORT):
        query = '''
                select * 
                    from {table_name}
                    order by cartodb_id
                    limit {limit} 
                offset {offset}
                '''.format(
                    table_name = table_name,
                    limit = SPLIT_EXPORT,
                    offset = i)
        queries.append(query)

    download_tasks = group([download_chunks.s(query, CARTO_BASE_URL, CARTO_API_KEY) for query in queries])
    download_jobs = download_tasks.apply_async()

    while(download_jobs.ready() is True):
        print('Download finished')
else:
    query = '''
            select * 
                from {table_name}
                order by cartodb_id
            '''.format(
                    table_name = table_name)
    result = sql.send(
        query,
        format = 'csv'
    )

    with open(f'output.csv','ab') as f:
        f.write(result)
    f.close()


