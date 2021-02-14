CREATE OR REPLACE TABLE `{{ params.dwh_dataset }}.D_AIRPORT` AS
SELECT DISTINCT
        SUBSTR(iso_region, 4,2) AS STATE_ID,
        name AS AIRPORT_NAME,
        iata_code AS IATA_CODE,
        local_code AS LOCAL_CODE,
        coordinates AS COORDINATES,
        elevation AS ELEVATION_FT
FROM
        `{{ params.project_id }}.{{ params.staging_dataset }}.airport_codes`
WHERE 
        iso_country = 'US'
  AND   type != 'closed'
