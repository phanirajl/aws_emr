CREATE DATABASE IF NOT EXISTS powtoon_hive
LOCATION 's3://powtoon-bi/Athena_data/powtoon_hive';

DROP TABLE IF EXISTS powtoon_hive.cloudfront_logs_page_ath;

CREATE EXTERNAL TABLE IF NOT EXISTS powtoon_hive.cloudfront_logs_page_ath ( 
    log_DATE STRING,  
    user_id STRING, 
    page_path STRING, 
    referer STRING,
    tracking_referer STRING,
    medium STRING,
    campaign STRING,
    source STRING,
    visitor_id STRING,
    ip STRING,
    session_id STRING,
    operating_sys STRING,
    ad_id STRING,
    keyword STRING,
    user_agent STRING
)
PARTITIONED BY (`year` STRING,`month` STRING, `day` STRING)
ROW FORMAT DELIMITED
FIELDS   TERMINATED BY '|'
STORED AS PARQUET
LOCATION 's3://powtoon-bi/Athena_data/powtoon_hive/Athena_pages/';

DROP TABLE IF EXISTS powtoon_hive.cloudfront_logs_event_ath;

CREATE EXTERNAL TABLE IF NOT EXISTS powtoon_hive.cloudfront_logs_event_ath 
    ( 
        log_DATE STRING, 
        user_id STRING, 
        category STRING, 
        action STRING, 
        label STRING, 
        value STRING,
        visitor_id STRING,
        ip STRING,
        session_id STRING,
        operating_sys STRING,
        extra_data_json STRING
    )
PARTITIONED BY (`year` STRING,`month` STRING, `day` STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS PARQUET
LOCATION 's3://powtoon-bi/Athena_data/powtoon_hive/Athena_events/';