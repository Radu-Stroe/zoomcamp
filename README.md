# zoomcamp
Data Engineering Zoomcamp 2025

Homework 1: Docker, SQL and Terraform

1. 24.3.1
2. db:5432
3. 104,838; 199,013; 109,645; 27,688; 35,202

SELECT 
    SUM(CASE 
        WHEN trip_distance <= 1 THEN 1 
        ELSE 0 
    END) AS up_to_1_mile,
    SUM(CASE 
        WHEN trip_distance > 1 AND trip_distance <= 3 THEN 1 
        ELSE 0 
    END) AS between_1_and_3_miles,
    SUM(CASE 
        WHEN trip_distance > 3 AND trip_distance <= 7 THEN 1 
        ELSE 0 
    END) AS between_3_and_7_miles,
    SUM(CASE 
        WHEN trip_distance > 7 AND trip_distance <= 10 THEN 1 
        ELSE 0 
    END) AS between_7_and_10_miles,
    SUM(CASE 
        WHEN trip_distance > 10 THEN 1 
        ELSE 0 
    END) AS over_10_miles
FROM 
    green_taxi
WHERE 
    CAST(lpep_pickup_datetime AS DATE) >= '2019-10-01'
    AND CAST(lpep_pickup_datetime AS DATE) < '2019-11-01';

4. 2019-10-31

SELECT 
    CAST("lpep_pickup_datetime" AS DATE),
	MAX(trip_distance)
FROM 
    green_taxi
GROUP BY 1
ORDER BY 2 DESC
LIMIT 1;


5.  East Harlem North, East Harlem South, Morningside Heights

SELECT 
	"Zone",
	SUM(total_amount)
FROM 
	green_taxi AS gt
JOIN 
	zones AS z 
	ON gt."PULocationID" = z."LocationID"
WHERE 
	CAST(lpep_pickup_datetime AS DATE) = '2019-10-18'
GROUP BY 1
HAVING 
	SUM(total_amount) > 13000
ORDER BY 2 DESC
LIMIT 10;

6.  JFK Airport

SELECT 
	zdo."Zone" AS dropoff_zone,
	MAX(tip_amount) AS max_tip
FROM 
	green_taxi AS gt
JOIN 
	zones AS zpu
	ON gt."PULocationID" = zpu."LocationID"
JOIN 
	zones AS zdo
	ON gt."DOLocationID" = zdo."LocationID"
WHERE 
	(CAST(lpep_pickup_datetime AS DATE) BETWEEN '2019-10-01' AND '2019-10-31')
	AND zpu."Zone" = 'East Harlem North'
GROUP BY 
	zdo."Zone"
ORDER BY 
	max_tip DESC 
LIMIT 1;

7. terraform init, terraform apply -auto-approve, terraform destroy


Homework 2: Kestra

1. 128.3 MB
2. green_tripdata_2020-04.csv
3. 24,648,499
4. 1,734,051
5. 1,925,152
6. Add a timezone property set to America/New_York in the Schedule trigger configuration

Homework 3: Data-warehouse 

#### create dataset
CREATE SCHEMA `data-warehouse-homework-450219.de_bq_radu`
OPTIONS(location = 'europe-central2');

#### create external table
CREATE OR REPLACE EXTERNAL TABLE `de_bq_radu.external_yellow_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['gs://radu-de-bucket/yellow_tripdata_2024-0*.parquet']
);

#### create a non partitioned table from external table
CREATE OR REPLACE TABLE `de_bq_radu.yellow_tripdata` AS
SELECT * FROM `de_bq_radu.external_yellow_tripdata`;

1. 20,332,093

SELECT COUNT(*) 
FROM `de_bq_radu.yellow_tripdata`;

2. 0 MB for the External Table and 155.12 MB for the Materialized Table

SELECT DISTINCT PULocationID
FROM `de_bq_radu.external_yellow_tripdata`;

SELECT DISTINCT PULocationID
FROM `de_bq_radu.yellow_tripdata`;

3. BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.

SELECT PULocationID
FROM `de_bq_radu.yellow_tripdata`;

SELECT PULocationID, DOLocationID
FROM `de_bq_radu.yellow_tripdata`;

4. 8,333

SELECT COUNT(fare_amount)
FROM `de_bq_radu.yellow_tripdata`
WHERE fare_amount = 0;

5. Partition by tpep_dropoff_datetime and Cluster on VendorID

CREATE OR REPLACE TABLE `de_bq_radu.yellow_tripdata_partitoned_clustered`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `de_bq_radu.external_yellow_tripdata`;

6. 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table

SELECT DISTINCT VendorID 
FROM `de_bq_radu.yellow_tripdata` 
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';

SELECT DISTINCT VendorID 
FROM `de_bq_radu.yellow_tripdata_partitioned_clustered` 
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';

7. GCP Bucket

8. False

Workshop 1: Ingestion with dlt

1. 1.6.1

!dlt --version

2. 4

import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

@dlt.resource(name="rides")
def ny_taxi():
    client = RESTClient(
        base_url="https://us-central1-dlthub-analytics.cloudfunctions.net",
        paginator=PageNumberPaginator(
            base_page=1,
            total_path=None
        )
    )

    for page in client.paginate("data_engineering_zoomcamp_api"):
        yield page  

pipeline = dlt.pipeline(
    pipeline_name="ny_taxi_pipeline",
    destination="duckdb",
    dataset_name="ny_taxi_data"
)

load_info = pipeline.run(ny_taxi)
print(load_info)

import duckdb
from google.colab import data_table
data_table.enable_dataframe_formatter()

conn = duckdb.connect(f"{pipeline.pipeline_name}.duckdb")

conn.sql(f"SET search_path = '{pipeline.dataset_name}'")

conn.sql("DESCRIBE").df()

3. 10000

df = pipeline.dataset(dataset_type="default").rides.df()
df

4. 12.3049

with pipeline.sql_client() as client:
    res = client.execute_sql(
            """
            SELECT
            AVG(date_diff('minute', trip_pickup_date_time, trip_dropoff_date_time))
            FROM rides;
            """
        )
    # Prints column values of the first row
    print(res)

