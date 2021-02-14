
from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator

project_id = 'edward-303907'
staging_dataset = 'staging'
dwh_dataset = 'dwh'
gs_bucket = '56k-dwh-case'

default_args = {
    'owner': 'Ed',
    'depends_on_past': False,
    'email': ['ed@quedah.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'dwh_setup',
    default_args=default_args,
    description='Read data from GCS to staging and setup DWH.',
    schedule_interval='@once',
    start_date=datetime.now(),
)


# Dummies for task grouping
start_pipeline = DummyOperator( task_id = 'start_pipeline', dag = dag)
loaded_data_to_staging = DummyOperator( task_id = 'loaded_data_to_staging')
finish_pipeline = DummyOperator( task_id = 'finish_pipeline')

# ------------------------------------------------------------------------------
# 1. Load data from GCS to staging BQ
# ------------------------------------------------------------------------------
load_airports = GoogleCloudStorageToBigQueryOperator(
    task_id = 'load_airports',
    bucket = gs_bucket,
    source_objects = ['airport-codes.csv'],
    destination_project_dataset_table = f'{project_id}.{staging_dataset}.airport_codes',
    #schema_object = 'airports/airport_codes.json',
    schema_fields=[
        {'name': 'id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'type', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'elevation', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'continent', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'iso_country', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'iso_region', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'municipality', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'gps_code', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'iata_code', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'local_code', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'coordinates', 'type': 'STRING', 'mode': 'NULLABLE'},
    ],
    write_disposition='WRITE_TRUNCATE',
    source_format = 'csv',
    skip_leading_rows = 1
)

load_us_cities_demo = GoogleCloudStorageToBigQueryOperator(
    task_id = 'load_us_cities_demo',
    bucket = gs_bucket,
    source_objects = ['us-cities-demographics.csv'],
    destination_project_dataset_table = f'{project_id}:{staging_dataset}.us_cities_demo',
    #schema_object = 'cities/us_cities_demo.json',
    schema_fields = [
        {'name': 'city',                'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'state',               'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'median_age',          'type': 'NUMERIC', 'mode': 'NULLABLE'},
        {'name': 'male_population',     'type': 'INT64', 'mode': 'NULLABLE'},
        {'name': 'female_population',   'type': 'INT64', 'mode': 'NULLABLE'},
        {'name': 'total_population',    'type': 'INT64', 'mode': 'NULLABLE'},
        {'name': 'nr_veterans',         'type': 'INT64', 'mode': 'NULLABLE'},
        {'name': 'foreign_born',        'type': 'INT64', 'mode': 'NULLABLE'},
        {'name': 'avg_household_size',  'type': 'NUMERIC', 'mode': 'NULLABLE'},
        {'name': 'state_code',          'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'race',                'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'count',               'type': 'NUMERIC', 'mode': 'NULLABLE'},
    ],
    write_disposition='WRITE_TRUNCATE',
    source_format = 'csv',
    field_delimiter=';',
    skip_leading_rows = 1
)

load_weather = GoogleCloudStorageToBigQueryOperator(
    task_id = 'load_weather',
    bucket = gs_bucket,
    source_objects = ['GlobalLandTemperaturesByCity.csv'],
    destination_project_dataset_table = f'{project_id}:{staging_dataset}.temperature_by_city',
    #schema_object = 'weather/temperature_by_city.json',
    schema_fields = [
        {'name': 'dt',                  'type': 'DATE', 'mode': 'NULLABLE'},
        {'name': 'avg_temperature',     'type': 'FLOAT64', 'mode': 'NULLABLE'},
        {'name': 'avg_temperature_uncertainty', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
        {'name': 'city',                'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'country',             'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'latitude',            'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'longitude',           'type': 'STRING', 'mode': 'NULLABLE'},
    ],
    write_disposition='WRITE_TRUNCATE',
    source_format = 'csv',
    skip_leading_rows = 1
)

load_immigration_data = GoogleCloudStorageToBigQueryOperator(
    task_id = 'load_immigration_data',
    bucket = gs_bucket,
    source_objects = ['data/*.parquet'],
    destination_project_dataset_table = f'{project_id}:{staging_dataset}.immigration_data',
    source_format = 'parquet',
    write_disposition='WRITE_TRUNCATE',
    skip_leading_rows = 1,
    autodetect = True
)

# ------------------------------------------------------------------------------
# 2. Run checks on staging BQ
# ------------------------------------------------------------------------------
check_us_cities_demo = BigQueryCheckOperator(
    task_id = 'check_us_cities_demo',
    use_legacy_sql=False,
    #sql = f'SELECT count(*) FROM `{project_id}.{staging_dataset}.us_cities_demo`'
    sql = f'SELECT count(city) FROM `{project_id}.{staging_dataset}.us_cities_demo`'

)

check_airports = BigQueryCheckOperator(
    task_id = 'check_airports',
    use_legacy_sql=False,
    sql = f'SELECT count(id) FROM `{project_id}.{staging_dataset}.airport_codes`'
)

check_weather = BigQueryCheckOperator(
    task_id = 'check_weather',
    use_legacy_sql=False,
    sql = f'SELECT count(dt) FROM `{project_id}.{staging_dataset}.temperature_by_city`'
)


check_immigration_data = BigQueryCheckOperator(
    task_id = 'check_immigration_data',
    use_legacy_sql=False,
    sql = f'SELECT count(cicid) FROM `{project_id}.{staging_dataset}.immigration_data`'
)

# ------------------------------------------------------------------------------
# 3 Load data from GCS to DWH BQ
# ------------------------------------------------------------------------------
load_country = GoogleCloudStorageToBigQueryOperator(
    task_id = 'load_country',
    bucket = gs_bucket,
    source_objects = ['mappings/i94res.csv'],
    destination_project_dataset_table = f'{project_id}:{dwh_dataset}.D_COUNTRY',
    write_disposition='WRITE_TRUNCATE',
    source_format = 'csv',
    skip_leading_rows = 1,
    schema_fields=[
        {'name': 'COUNTRY_ID', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
        {'name': 'COUNTRY_NAME', 'type': 'STRING', 'mode': 'NULLABLE'},
    ]
)

load_port = GoogleCloudStorageToBigQueryOperator(
    task_id = 'load_port',
    bucket = gs_bucket,
    source_objects = ['mappings/i94prtl.csv'],
    destination_project_dataset_table = f'{project_id}:{dwh_dataset}.D_PORT',
    write_disposition='WRITE_TRUNCATE',
    source_format = 'csv',
    skip_leading_rows = 1,
    schema_fields=[
        {'name': 'PORT_ID', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'PORT_NAME', 'type': 'STRING', 'mode': 'NULLABLE'},
    ]
)

load_state = GoogleCloudStorageToBigQueryOperator(
    task_id = 'load_state',
    bucket = gs_bucket,
    source_objects = ['mappings/i94addrl.csv'],
    destination_project_dataset_table = f'{project_id}:{dwh_dataset}.D_STATE',
    write_disposition='WRITE_TRUNCATE',
    source_format = 'csv',
    skip_leading_rows = 1,
    schema_fields=[
        {'name': 'STATE_ID', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'STATE_NAME', 'type': 'STRING', 'mode': 'NULLABLE'},
    ]
)


# ------------------------------------------------------------------------------
# 4. Load data from staging BQ to DWH BQ
# ------------------------------------------------------------------------------

create_immigration_data = BigQueryOperator(
    task_id = 'create_immigration_data',
    use_legacy_sql = False,
    params = {
        'project_id': project_id,
        'staging_dataset': staging_dataset,
        'dwh_dataset': dwh_dataset
    },
    sql = './sql/F_IMMIGRATION_DATA.sql'
)

check_f_immigration_data = BigQueryCheckOperator(
    task_id = 'check_f_immigration_data',
    use_legacy_sql=False,
    params = {
        'project_id': project_id,
        'staging_dataset': staging_dataset,
        'dwh_dataset': dwh_dataset
    },
    sql = f'SELECT count(*) = count(distinct cicid) FROM `{project_id}.{dwh_dataset}.F_IMMIGRATION_DATA`'
)


# ------------------------------------------------------------------------------
# 5. Create DWH BQ dimension tables from SQL queries
# ------------------------------------------------------------------------------
create_d_time = BigQueryOperator(
    task_id = 'create_d_time',
    use_legacy_sql = False,
    params = {
        'project_id': project_id,
        'staging_dataset': staging_dataset,
        'dwh_dataset': dwh_dataset
    },
    sql = './sql/D_TIME.sql'
)

create_d_weather = BigQueryOperator(
    task_id = 'create_d_weather',
    use_legacy_sql = False,
    params = {
        'project_id': project_id,
        'staging_dataset': staging_dataset,
        'dwh_dataset': dwh_dataset
    },
    sql = './sql/D_WEATHER.sql'
)

create_d_airport = BigQueryOperator(
    task_id = 'create_d_airport',
    use_legacy_sql = False,
    params = {
        'project_id': project_id,
        'staging_dataset': staging_dataset,
        'dwh_dataset': dwh_dataset
    },
    sql = './sql/D_AIRPORT.sql'
)

create_d_city_demo = BigQueryOperator(
    task_id = 'create_d_city_demo',
    use_legacy_sql = False,
    params = {
        'project_id': project_id,
        'staging_dataset': staging_dataset,
        'dwh_dataset': dwh_dataset
    },
    sql = './sql/D_CITY_DEMO.sql'
)


# ------------------------------------------------------------------------------
# 6. Define task dependencies
# ------------------------------------------------------------------------------
start_pipeline >> [load_airports, load_us_cities_demo, load_weather, load_immigration_data] 

load_us_cities_demo >> check_us_cities_demo
load_airports >> check_airports
load_weather >> check_weather
load_immigration_data >> check_immigration_data

[check_us_cities_demo, check_airports, check_weather,check_immigration_data] >> loaded_data_to_staging

loaded_data_to_staging >> [load_country, load_port, load_state] >> create_immigration_data >> check_f_immigration_data

check_f_immigration_data >> [create_d_time, create_d_weather, create_d_airport, create_d_city_demo] >> finish_pipeline
