CREATE DATABASE IF NOT EXISTS powtoon_hive;

USE powtoon_hive;

DROP TABLE IF EXISTS cloudfront_log_stg;

CREATE EXTERNAL TABLE cloudfront_log_stg 
    (
		log_DATE STRING,
		log_TIME STRING,
		xedgelocation STRING,
		scbytes STRING,
		cip STRING,
		csmethod STRING,
		csHost STRING,
		csuristem STRING,
		scstatus STRING,
		csReferer STRING,
		csUserAgent STRING,
		csuriquery STRING,
		csCookie STRING,
		xedgeresulttype STRING,
		xedgerequestid STRING,
		xhostheader STRING,
		csprotocol STRING,
		csbytes STRING,
		timetaken STRING,
		xforwardedfor STRING,
		sslprotocol STRING,
		sslcipher STRING,
		xedgeresponseresulttype STRING
    )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES 
	(
  		"input.regex" ="^(?!#)^(?!#)([^\\s]+)\\s([^\\s]+)\\s([^\\s]+)\\s([^\\s]+)\\s([^\\s]+)\\s([^\\s]+)\\s([^\\s]+)\\s([^\\s]+)\\s([^\\s]+)\\s([^\\s]+)\\s([^\\s]+)\\s([^\\s]+)\\s([^\\s]+)\\s([^\\s]+)\\s([^\\s]+)\\s([^\\s]+)\\s([^\\s]+)\\s([^\\s]+)\\s([^\\s]+)\\s([^\\s]+)\\s([^\\s]+)\\s([^\\s]+)\\s([^\\s]+).*$"
	)
LOCATION '/user/admin/powtoon_cloudfront_logs/';

DROP TABLE IF EXISTS cloudfront_logs_page_stg;

CREATE EXTERNAL TABLE IF NOT EXISTS cloudfront_logs_page_stg 
    ( 
        log_DATE STRING, 
        log_TIME STRING, 
        user_id  STRING, 
        page_path   STRING, 
        referer   STRING,
    	tracking_referer STRING,
    	medium STRING,
    	campaign STRING,
    	source STRING,
        id STRING,
        visitor_id STRING,
        ip STRING,
        session_id STRING,
        operating_sys STRING,
        ad_id STRING,
        keyword STRING,
        user_agent STRING
    )
-- ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '/user/admin/page_stg/';

DROP TABLE IF EXISTS cloudfront_logs_event_stg;

CREATE EXTERNAL TABLE IF NOT EXISTS cloudfront_logs_event_stg 
    ( 
        log_DATE STRING, 
        log_TIME STRING, 
        user_id STRING, 
        category STRING, 
        action STRING, 
        label STRING, 
        value STRING,
        id STRING,
        visitor_id STRING,
        ip STRING,
        session_id STRING,
        operating_sys STRING,
        extra_data_json STRING
    ) 
-- ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '/user/admin/event_stg/';

INSERT INTO TABLE cloudfront_logs_event_stg
SELECT 
    log_DATE,
    log_TIME,
    reflect("java.net.URLDecoder", "decode",reflect("java.net.URLDecoder", "decode",IF(instr(csuriquery,'u=')>0,
                    substring(csuriquery,locate('u=',csuriquery)+2, IF(locate('&',csuriquery,locate('u=',csuriquery))>0, locate('&',csuriquery,locate('u=',csuriquery)), LENGTH(csuriquery)+1)-locate('u=',csuriquery)-2),''))) as userid,
    reflect("java.net.URLDecoder", "decode",reflect("java.net.URLDecoder", "decode",IF(instr(csuriquery,'c=')>0 ,
                    substring(csuriquery,locate('c=',csuriquery)+2, IF(locate('&',csuriquery,locate('c=',csuriquery))>0, locate('&',csuriquery,locate('c=',csuriquery)), LENGTH(csuriquery)+1)-locate('c=',csuriquery)-2),''))) as category,
    reflect("java.net.URLDecoder", "decode",reflect("java.net.URLDecoder", "decode",IF(instr(csuriquery,'a=')>0 ,
                    substring(csuriquery,locate('a=',csuriquery)+2, IF(locate('&',csuriquery,locate('a=',csuriquery))>0, locate('&',csuriquery,locate('a=',csuriquery)), LENGTH(csuriquery)+1)-locate('a=',csuriquery)-2),''))) as action,
    reflect("java.net.URLDecoder", "decode",reflect("java.net.URLDecoder", "decode",IF(instr(csuriquery,'l=')>0 ,
                    substring(csuriquery,locate('l=',csuriquery)+2, IF(locate('&',csuriquery,locate('l=',csuriquery))>0, locate('&',csuriquery,locate('l=',csuriquery)), LENGTH(csuriquery)+1)-locate('l=',csuriquery)-2),''))) as label,
    reflect("java.net.URLDecoder", "decode",reflect("java.net.URLDecoder", "decode",IF(instr(csuriquery,'v=')>0 ,
                    substring(csuriquery,locate('v=',csuriquery)+2, IF(locate('&',csuriquery,locate('v=',csuriquery))>0, locate('&',csuriquery,locate('v=',csuriquery)), LENGTH(csuriquery)+1)-locate('v=',csuriquery)-2),''))) as value,
    reflect('org.apache.commons.codec.digest.DigestUtils', 'sha256Hex', concat(log_DATE,log_TIME,csuriquery)) as id,
    reflect("java.net.URLDecoder", "decode",reflect("java.net.URLDecoder", "decode",IF(INSTR(csuriquery,'i=')>0 ,
                    SUBSTRING(csuriquery,locate('i=',csuriquery)+2, IF(locate('&',csuriquery,locate('i=',csuriquery))>0, locate('&',csuriquery,locate('i=',csuriquery)), LENGTH(csuriquery)+1)-locate('i=',csuriquery)-2),''))) AS visitor_id,
    cip as ip,
    reflect("java.net.URLDecoder", "decode",reflect("java.net.URLDecoder", "decode",IF(INSTR(csuriquery,'s=')>0 ,
                    SUBSTRING(csuriquery,locate('s=',csuriquery) + 2, IF(locate('&',csuriquery,locate('s=',csuriquery)) > 0, locate('&',csuriquery,locate('s=',csuriquery)), LENGTH(csuriquery)+1)-locate('s=',csuriquery)-2),''))) as session_id,
    reflect("java.net.URLDecoder", "decode",reflect("java.net.URLDecoder", "decode",
                    if(locate('(',csuseragent) > 0,
                    substring(csuseragent,instr(csuseragent,'(') + 1,instr(csuseragent,')') - (instr(csuseragent,'(') + 1)),''))) as operating_sys,
    reflect("java.net.URLDecoder", "decode",reflect("java.net.URLDecoder", "decode",IF(INSTR(csuriquery,'d=') > 0 ,
                    SUBSTRING(csuriquery,locate('d=',csuriquery)+2, IF(locate('&',csuriquery,locate('d=',csuriquery))>0, locate('&',csuriquery,locate('d=',csuriquery)), LENGTH(csuriquery)+1)-locate('d=',csuriquery)-2),''))) AS extra_data_json
    --,reflect("java.net.URLDecoder", "decode",reflect("java.net.URLDecoder", "decode",
    --				substring(	) )) as powtoon_id
FROM 
    cloudfront_log_stg
WHERE 
    csuristem = '/event.gif'
    AND
    length(reflect("java.net.URLDecoder", "decode",reflect("java.net.URLDecoder", "decode",IF(instr(csuriquery,'a=')>0 ,
                    substring(csuriquery,locate('a=',csuriquery)+2, IF(locate('&',csuriquery,locate('a=',csuriquery))>0, locate('&',csuriquery,locate('a=',csuriquery)), LENGTH(csuriquery)+1)-locate('a=',csuriquery)-2),'')))) <= 1024;

INSERT INTO TABLE cloudfront_logs_page_stg
SELECT 
    log_DATE,
    log_TIME,
    reflect("java.net.URLDecoder", "decode",reflect("java.net.URLDecoder", "decode",IF(instr(csuriquery,'u=')>0 ,
                    substring(csuriquery,locate('u=',csuriquery)+2, IF(locate('&',csuriquery,locate('u=',csuriquery))>0, locate('&',csuriquery,locate('u=',csuriquery)), LENGTH(csuriquery)+1)-locate('u=',csuriquery)-2),''))) as userid,
    reflect("java.net.URLDecoder", "decode",reflect("java.net.URLDecoder", "decode",IF(instr(csuriquery,'p=')>0 ,
                    substring(csuriquery,locate('p=',csuriquery)+2, IF(locate('&',csuriquery,locate('p=',csuriquery))>0, locate('&',csuriquery,locate('p=',csuriquery)), LENGTH(csuriquery)+1)-locate('p=',csuriquery)-2),''))) as pagepath,
    reflect("java.net.URLDecoder", "decode",reflect("java.net.URLDecoder", "decode",IF(instr(csuriquery,'r=')>0 ,
                    substring(csuriquery,locate('r=',csuriquery)+2, IF(locate(' ',csuriquery,locate('r=',csuriquery))>0, locate(' ',csuriquery,locate('r=',csuriquery)), LENGTH(csuriquery)+1)-locate('r=',csuriquery)-2),''))) as referer,
					csReferer as tracking_referer,
	reflect("java.net.URLDecoder", "decode",reflect("java.net.URLDecoder", "decode",IF(locate('utm_medium=',csReferer)>0 ,
                	substring(csReferer,locate('utm_medium=',csReferer)+11, IF(locate('&',csReferer,locate('utm_medium=',csReferer))>0, locate('&',csReferer,locate('utm_medium=',csReferer)), LENGTH(csReferer)+1)-locate('utm_medium=',csReferer)-11),''))) as medium,
    reflect("java.net.URLDecoder", "decode",reflect("java.net.URLDecoder", "decode",IF(locate('utm_campaign=',csReferer)>0 ,
                	substring(csReferer,locate('utm_campaign=',csReferer)+13, IF(locate('&',csReferer,locate('utm_campaign=',csReferer))>0, locate('&',csReferer,locate('utm_campaign=',csReferer)), LENGTH(csReferer)+1)-locate('utm_campaign=',csReferer)-13),''))) as campaign,
    reflect("java.net.URLDecoder", "decode",reflect("java.net.URLDecoder", "decode",IF(locate('utm_source=',csReferer)>0 ,
                	substring(csReferer,locate('utm_source=',csReferer)+11, IF(locate('&',csReferer,locate('utm_source=',csReferer))>0, locate('&',csReferer,locate('utm_source=',csReferer)), LENGTH(csReferer)+1)-locate('utm_source=',csReferer)-11),''))) as source,
    reflect('org.apache.commons.codec.digest.DigestUtils', 'sha256Hex', concat(log_DATE,log_TIME,csuriquery)) as id,
    reflect("java.net.URLDecoder", "decode",reflect("java.net.URLDecoder", "decode",IF(INSTR(csuriquery,'i=')>0 ,
                    SUBSTRING(csuriquery,locate('i=',csuriquery)+2, IF(locate('&',csuriquery,locate('i=',csuriquery))>0, locate('&',csuriquery,locate('i=',csuriquery)), LENGTH(csuriquery)+1)-locate('i=',csuriquery)-2),''))) AS visitor_id,
    cip as ip,
    reflect("java.net.URLDecoder", "decode",reflect("java.net.URLDecoder", "decode",
        IF(locate('s=',csuriquery) > 0 ,
                    SUBSTRING(csuriquery,locate('s=',csuriquery) + 2, IF(locate('&',csuriquery,locate('s=',csuriquery)) > 0, locate('&',csuriquery,locate('s=',csuriquery)), LENGTH(csuriquery)+1)-locate('s=',csuriquery)-2),''))) as session_id,
    reflect("java.net.URLDecoder", "decode",reflect("java.net.URLDecoder", "decode",
                    if(locate('(',csuseragent) > 0,
                    substring(csuseragent,instr(csuseragent,'(') + 1,instr(csuseragent,')') - (instr(csuseragent,'(') + 1)),''))) as operating_sys,
    regexp_replace(regexp_replace(reflect("java.net.URLDecoder", "decode",reflect("java.net.URLDecoder", "decode",
                    if(locate('ad_id',csreferer) > 0,
                    parse_url(csreferer,'QUERY','ad_id'),
                    if(locate('ad_id',csuriquery) > 0,
                    substring(csuriquery,instr(csuriquery,'ad_id') + 4,locate('ad_group_id',csuriquery) - (instr(csuriquery,'ad_id') + 4)),''))))
                    ,'&',''),'d=','') as ad_id,
    regexp_replace(regexp_replace(reflect("java.net.URLDecoder", "decode",reflect("java.net.URLDecoder", "decode",
                    if(locate('keyword=',csreferer) > 0,
                    	reflect("java.net.URLDecoder", "decode",reflect("java.net.URLDecoder", "decode",reflect("java.net.URLDecoder", "decode",
                            parse_url(csreferer,'QUERY','keyword')))),  
                    if(locate('keyword',csuriquery) > 0,
                    		reflect("java.net.URLDecoder", "decode",reflect("java.net.URLDecoder", "decode",reflect("java.net.URLDecoder", "decode",reflect("java.net.URLDecoder", "decode",reflect("java.net.URLDecoder", "decode",
                    			substring(csuriquery,locate('keyword%',csuriquery) + 7,locate('gclid',csuriquery) - (locate('keyword%',csuriquery) + 7))))))),''))))
                    ,'=',''),'&','') as keyword,
    reflect("java.net.URLDecoder", "decode",reflect("java.net.URLDecoder", "decode",csuseragent)) as user_agent
FROM 
    cloudfront_log_stg
WHERE csuristem ='/page.gif'
    AND length(reflect("java.net.URLDecoder", "decode",reflect("java.net.URLDecoder", "decode",IF(instr(csuriquery,'p=') > 0 ,
                    substring(csuriquery,locate('p=',csuriquery) + 2, IF(locate('&',csuriquery,locate('p=',csuriquery)) > 0, locate('&',csuriquery,locate('p=',csuriquery)), LENGTH(csuriquery)+1)-locate('p=',csuriquery)-2),'')))) < 1000
    AND length(csReferer) < 1000;

DROP TABLE IF EXISTS cloudfront_log_stg_errors;

CREATE EXTERNAL TABLE IF NOT EXISTS cloudfront_log_stg_errors 
    (
        log_DATE STRING,
		log_TIME STRING,
		xedgelocation STRING,
		scbytes STRING,
		cip STRING,
		csmethod STRING,
		csHost STRING,
		csuristem STRING,
		scstatus STRING,
		csReferer STRING,
		csUserAgent STRING,
		csuriquery STRING,
		csCookie STRING,
		xedgeresulttype STRING,
		xedgerequestid STRING,
		xhostheader STRING,
		csprotocol STRING,
		csbytes STRING,
		timetaken STRING,
		xforwardedfor STRING,
		sslprotocol STRING,
		sslcipher STRING,
		xedgeresponseresulttype STRING 
    )
-- ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '/user/admin/errors';

INSERT INTO TABLE cloudfront_log_stg_errors
SELECT 
    * 
FROM cloudfront_log_stg s
WHERE 
    reflect('org.apache.commons.codec.digest.DigestUtils', 'sha256Hex', concat(s.log_DATE,s.log_TIME,s.csuriquery)) NOT IN 
        ( 
                SELECT id FROM cloudfront_logs_page_stg p 
                UNION ALL 
                SELECT id FROM cloudfront_logs_event_stg e)
    AND s.log_DATE IS NOT NULL;

DROP TABLE IF EXISTS cloudfront_logs_page;

CREATE EXTERNAL TABLE IF NOT EXISTS cloudfront_logs_page 
    ( 
        log_DATE STRING,  
        user_id  STRING, 
        page_path   STRING, 
        referer   STRING,
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
-- ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '/user/admin/page';

DROP TABLE IF EXISTS cloudfront_logs_event;

CREATE EXTERNAL TABLE IF NOT EXISTS cloudfront_logs_event 
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
        --,powtoon_id INT
    ) 
-- ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '/user/admin/event';

INSERT INTO TABLE cloudfront_logs_event
SELECT 
	DISTINCT
    regexp_replace(cast(concat(log_DATE,' ',log_TIME) as timestamp),'"','') as log_DATE,
    regexp_replace(if(user_id = '',0,user_id),'"','')  as user_id,
    regexp_replace(category,'"',''),
    regexp_replace(action,'"',''),
    regexp_replace(label,'"',''),
    regexp_replace(value,'"',''),
    regexp_replace(if(visitor_id='', 0, visitor_id),'"','') as visitor_id,
    regexp_replace(ip,'"',''),
    regexp_replace(session_id,'"',''),
    regexp_replace(operating_sys,'"',''),
    regexp_replace(extra_data_json,'"','')
    --,powtoon_id
FROM 
    cloudfront_logs_event_stg
WHERE length(session_id) <= 40
	and length(ip) < 20
	and length(visitor_id) <= 64
	and (log_DATE <> '' or log_DATE is not null);

INSERT INTO TABLE cloudfront_logs_page
SELECT 
    regexp_replace(cast(concat(log_DATE,' ',log_TIME) as timestamp),'"','') as log_DATE,
    regexp_replace(if(user_id='',0,user_id),'"','') as user_id,
    regexp_replace(page_path,'"',''),
    regexp_replace(referer,'"',''),
    regexp_replace(tracking_referer,'"',''),
    regexp_replace(medium,'"',''), 
    regexp_replace(campaign,'"',''), 
    regexp_replace(source,'"',''),
    regexp_replace(if(visitor_id='', 0, visitor_id),'"','') as visitor_id,
    regexp_replace(ip,'"',''),
    regexp_replace(session_id,'"',''),
    regexp_replace(operating_sys,'"',''),
    regexp_replace(ad_id,'"',''),
    regexp_replace(keyword,'"',''),
    regexp_replace(user_agent,'"','') 
FROM
    cloudfront_logs_page_stg
WHERE length(session_id) <= 40
	and length(keyword) < 50
	and length(ip) < 20
	and length(ad_id) < 150
	and length(visitor_id) <= 64
	and (log_DATE <> '' or log_DATE is not null);

DROP TABLE IF EXISTS cloudfront_log_stg_s3;

CREATE EXTERNAL TABLE cloudfront_log_stg_s3 
    (
        log_DATE STRING,
        log_TIME STRING,
        xedgelocation STRING,
        scbytes STRING,
        cip STRING,
        csmethod STRING,
        csHost STRING,
        csuristem STRING,
        scstatus STRING,
        csReferer STRING,
        csUserAgent STRING,
        csuriquery STRING,
        csCookie STRING,
        xedgeresulttype STRING,
        xedgerequestid STRING,
        xhostheader STRING,
        csprotocol STRING,
        csbytes STRING,
        timetaken STRING,
        xforwardedfor STRING,
        sslprotocol STRING,
        sslcipher STRING,
        xedgeresponseresulttype STRING
    )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES 
    (
        "input.regex" ="^(?!#)^(?!#)([^\\s]+)\\s([^\\s]+)\\s([^\\s]+)\\s([^\\s]+)\\s([^\\s]+)\\s([^\\s]+)\\s([^\\s]+)\\s([^\\s]+)\\s([^\\s]+)\\s([^\\s]+)\\s([^\\s]+)\\s([^\\s]+)\\s([^\\s]+)\\s([^\\s]+)\\s([^\\s]+)\\s([^\\s]+)\\s([^\\s]+)\\s([^\\s]+)\\s([^\\s]+)\\s([^\\s]+)\\s([^\\s]+)\\s([^\\s]+)\\s([^\\s]+).*$"
    )
LOCATION 's3://powtoon-bi/cloudfront_tracking/powtoon_cloudfront_logs/';

-- Parquet tables
CREATE EXTERNAL TABLE IF NOT EXISTS cloudfront_logs_page_parq
    ( 
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
STORED AS PARQUET
LOCATION 's3://powtoon-bi/Daily_events/Daily_events_parquet/';

CREATE EXTERNAL TABLE IF NOT EXISTS cloudfront_logs_event_parq
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
        --powtoon_id STRING
    )
STORED AS PARQUET
LOCATION 's3://powtoon-bi/Daily_events/Daily_events_parquet/';

INSERT OVERWRITE TABLE cloudfront_logs_page_parq
SELECT * FROM cloudfront_logs_page;

INSERT OVERWRITE TABLE cloudfront_logs_event_parq 
SELECT * FROM cloudfront_logs_event;

-- Athena process
-- s3://powtoon-bi/Daily_events/
-- s3://powtoon-bi/Athena_events/
-- s3://powtoon-bi/Athena_pages/

-- Create partitioned tables and load
SET hive.exec.dynamic.partition = true;  
SET hive.exec.dynamic.partition.mode = nonstrict; 

DROP TABLE IF EXISTS cloudfront_logs_page_part;

CREATE EXTERNAL TABLE IF NOT EXISTS cloudfront_logs_page_part 
    ( 
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
PARTITIONED BY
(
        `year` STRING,
        `month` STRING,
        `day` STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS PARQUET
-- STORED AS TEXTFILE
LOCATION '/user/admin/pages_partitioned';

DROP TABLE IF EXISTS cloudfront_logs_event_part;

CREATE EXTERNAL TABLE IF NOT EXISTS cloudfront_logs_event_part
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
PARTITIONED BY
(
        `year` STRING,
        `month` STRING,
        `day` STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|' 
STORED AS PARQUET
-- STORED AS TEXTFILE
LOCATION '/user/admin/events_partitioned';

INSERT INTO TABLE cloudfront_logs_page_part
PARTITION 
(
    `year`,
    `month`,
    `day`
)
SELECT
    log_DATE,
    user_id,
    page_path,
    referer,
    tracking_referer,
    medium, 
    campaign, 
    source,
    visitor_id,
    ip,
    session_id,
    operating_sys,
    ad_id,
    keyword,
    user_agent,
    year(log_DATE) as `year`,
    month(log_DATE) as `month`,
    day(log_DATE) as `day`
FROM
    cloudfront_logs_page
WHERE (log_DATE <> '' or log_DATE is not null);

INSERT INTO TABLE cloudfront_logs_event_part
PARTITION 
(
    `year`,
    `month`,
    `day`
)
SELECT
    log_DATE,
    user_id,
    category,
    action,
    label,
    value,
    visitor_id,
    ip,
    session_id,
    operating_sys,
    extra_data_json,
    --powtoon_id,
    year(log_DATE) as `year`,
    month(log_DATE) as `month`,
    day(log_DATE) as `day`
FROM
    cloudfront_logs_event
WHERE (log_DATE <> '' or log_DATE is not null);
