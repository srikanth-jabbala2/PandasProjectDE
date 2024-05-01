from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DateTime
import requests
import json
from datetime import datetime


# Snowflake connection parameters
ACCOUNT = 'lwofwpw-px54627'
USER = 'SRIKANTHJABBALA'
PASSWORD = '%40Sri2kanth'
WAREHOUSE = 'COMPUTE_WH'
DATABASE = 'PANDASPROJ'
SCHEMA = 'RAW'
##

# Establishing a connection to Snowflake
conn_str = f'snowflake://{USER}:{PASSWORD}@{ACCOUNT}/{DATABASE}/{SCHEMA}?warehouse={WAREHOUSE}'

# Create the SQLAlchemy engine
engine = create_engine(conn_str)

print('Database connection established')

# Create a metadata object
metadata = MetaData()

# Define a table schema
api_data1 = Table(
    'api_data1',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String),
    Column('email', String),
    Column('created_at', DateTime, default=datetime.utcnow)
)

# Create the table in Snowflake (if not exists)
metadata.create_all(engine, checkfirst=True)

# source
response_API = requests.get('https://jsonplaceholder.typicode.com/users?size=5')
df = response_API.json()
print("Response received from API")

# Insert API data into Snowflake table
with engine.connect() as connection:
    for item in df:
        insert_query = api_data1.insert().values(
            name=item['name'],
            email=item['email']
        )
        connection.execute(insert_query)

print("Data inserted successfully into Snowflake.")

# #data insertion
# def insert_into_table(tablename, engine):
#     df2 = pd.json_normalize(df)

#     df2.to_sql(tablename, con=engine, if_exists='append', chunksize=1000, index=False)

# print('data inserted into table')

# # Function_call
# insert_into_table('apinew', engine)