from sqlalchemy import create_engine

engine = create_engine(
    'snowflake://{user}:{password}@{account_identifier}/'.format(
        user='SRIKANTHJABBALA',
        password='%40Sri2kanth',
        account_identifier='lwofwpw-PX54627',
        region='us-east-1'
    )
)
try:
    connection = engine.connect()
    results = connection.execute('select current_version()').fetchone()
    print(results[0])
finally:
    connection.close()
    engine.dispose()