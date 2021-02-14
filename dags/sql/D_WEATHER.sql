
CREATE OR REPLACE TABLE `{{ params.dwh_dataset }}.D_WEATHER` AS

SELECT DISTINCT
          EXTRACT(year FROM dt) AS YEAR,
          city AS CITY,
          country AS COUNTRY,
          latitude AS LATITUDE,
          longitude AS LONGITUDE,
          AVG(avg_temperature) AVERAGE_TEMPERATURE,
          AVG(avg_temperature_uncertainty) AVERAGE_TEMPERATURE_UNCERTAINTY

FROM
          `{{ params.project_id }}.{{ params.staging_dataset }}.temperature_by_city`

WHERE
          COUNTRY = 'United States'
  AND     EXTRACT(year FROM dt) = 
          ( 
                SELECT 
                            MAX(EXTRACT(year FROM dt))
                FROM
                            `{{ params.project_id }}.{{ params.staging_dataset }}.temperature_by_city`
          )

GROUP BY
          EXTRACT(year FROM dt),
          CITY,
          COUNTRY,
          LATITUDE,
          LONGITUDE
