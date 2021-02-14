CREATE OR REPLACE TABLE `{{ params.dwh_dataset }}.F_IMMIGRATION_DATA` AS
SELECT DISTINCT
  cicid AS CICID,
  CAST(i94yr AS NUMERIC) AS YEAR,
  CAST(i94mon AS NUMERIC) AS MONTH,

  COUNTRY_NAME AS COUNTRY_OF_ORIGIN,
  PORT_NAME,

  DATE_ADD('1960-1-1', INTERVAL CAST(arrdate AS INT64) DAY) AS ARRIVAL_DATE,
  CASE
    WHEN i94mode = 1 THEN 'Air'
    WHEN i94mode = 2 THEN 'Sea'
    WHEN i94mode = 3 THEN 'Land'
    ELSE 'Not reported'
  END AS ARRIVAL_MODE,

  
  STATE_ID AS DESTINATION_STATE_ID,
  STATE_NAME AS DESTINATION_STATE,
  DATE_ADD('1960-1-1', INTERVAL CAST(depdate AS INT64) DAY) AS DEPARTURE_DATE,

  CAST(i94bir AS numeric) AS  AGE,

  CASE
    WHEN i94visa = 1 THEN 'Business'
    WHEN i94visa = 2 THEN 'Pleasure'
    WHEN i94visa = 3 THEN 'Student'
  END AS VISA_CATEGORY,
  matflag AS MATCH_FLAG,
  CAST(biryear AS numeric) AS BIRTH_YEAR,
  CASE
    WHEN gender = 'F' THEN 'FEMALE'
    WHEN gender = 'M' THEN 'MALE'
    ELSE 'UNKNOWN'
  END AS GENDER,
  insnum AS INS_NUMBER,
  airline AS AIRLINE,
  CAST(admnum AS numeric) AS ADMISSION_NUMBER,
  fltno AS FLIGHT_NUMBER,
  visatype AS VISA_TYPE
FROM
  `{{ params.project_id }}.{{ params.staging_dataset }}.immigration_data` id
LEFT JOIN
  `{{ params.dwh_dataset }}.D_COUNTRY` AS DC
ON
  DC.COUNTRY_ID = i94res
LEFT JOIN
  `{{ params.dwh_dataset }}.D_PORT` AS DP
ON
  DP.PORT_ID = i94port
LEFT JOIN
  `{{ params.dwh_dataset }}.D_STATE` AS DS
ON
  DS.STATE_ID = i94addr

