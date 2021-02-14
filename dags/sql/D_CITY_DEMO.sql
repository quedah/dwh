CREATE OR REPLACE TABLE `{{ params.dwh_dataset }}.D_CITY_DEMO` AS
SELECT DISTINCT
        city AS CITY_NAME,
        state AS STATE_NAME,
        state_code AS STATE_ID,
        median_age AS MEDIAN_AGE,
        male_population AS MALE_POPULATION,
        female_population AS FEMALE_POPULATION,
        total_population AS TOTAL_POPULATION,
        nr_veterans AS NUMBER_OF_VETERANS,
        foreign_born AS FOREIGN_BORN,
        avg_household_size AS AVERAGE_HOUSEHOLD_SIZE,
        AVG(
        IF
        (RACE = 'White',
          COUNT,
          NULL)) AS WHITE_POPULATION,
        AVG(
        IF
        (RACE = 'Black or African-American',
          COUNT,
          NULL)) AS BLACK_POPULATION,
        AVG(
        IF
        (RACE = 'Asian',
          COUNT,
          NULL)) AS ASIAN_POPULATION,
        AVG(
        IF
        (RACE = 'Hispanic or Latino',
          COUNT,
          NULL)) AS LATINO_POPULATION,
        AVG(
        IF
        (RACE = 'American Indian and Alaska Native',
          COUNT,
          NULL)) AS NATIVE_POPULATION

FROM
        `{{ params.project_id }}.{{ params.staging_dataset }}.us_cities_demo`

GROUP BY
        CITY_NAME,
        STATE_NAME,
        STATE_ID,
        MEDIAN_AGE,
        MALE_POPULATION,
        FEMALE_POPULATION,
        TOTAL_POPULATION,
        NUMBER_OF_VETERANS,
        FOREIGN_BORN,
        AVERAGE_HOUSEHOLD_SIZE
