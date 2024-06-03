from google.cloud import bigquery 
from google.oauth2 import service_account

credentialsJSON = 'ml6-data-engineering-1cf35d18ff1d.json'
credentials = service_account.Credentials.from_service_account_file(credentialsJSON)
client = bigquery.Client(credentials=credentials)

query = '''
SELECT * except (station_group) 
FROM `ml6-data-engineering.london_bikes.ntile_j_and_c` 
where station_group = {station_group} 
'''
for i in range(1,101):
    results = client.query(query.format(station_group = i)).to_dataframe()
    results.to_csv(f'tables/export_{i}.csv', index=False, header=False)