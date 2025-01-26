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
