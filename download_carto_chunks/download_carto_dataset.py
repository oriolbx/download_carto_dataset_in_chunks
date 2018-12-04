from server import app
from carto.sql import SQLClient
from carto.auth import APIKeyAuthClient

@app.task
def download_chunks(query, CARTO_BASE_URL, CARTO_API_KEY):
    # SQL wrapper
    sql = SQLClient(APIKeyAuthClient(CARTO_BASE_URL, CARTO_API_KEY))
    
    result = sql.send(
        query,
        format = 'csv'
    )

    with open(f'output.csv','ab') as f:
        f.write(result)
    f.close()

    return True




    



