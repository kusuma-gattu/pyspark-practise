CREATE EXTERNAL TABLE market_analysis1 (
     campaign_id string,
     `date` string,
     hour INT,
     os_type STRING,
     event STRUCT < 
     impression: INT,
     click: INT,
     video_ad: INT
     >
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/tmp/output_data/market_analysis1/';

SELECT * FROM market_analysis1;

output:
OK
market_analysis1.campaign_id    market_analysis1.date   market_analysis1.hour   market_analysis1.os_type market_analysis1.event
ABCDFAE 2018-10-12      13      android {"impression":1,"click":1,"video_ad":null}
ABCDFAE 2018-10-12      13      ios     {"impression":1,"click":0,"video_ad":null}
Time taken: 0.351 seconds, Fetched: 2 row(s)

CREATE EXTERNAL TABLE market_analysis2 (
     campaign_id string,
     `date` string,
     hour INT,
     gender STRING,
     event STRUCT < 
     impression: INT,
     click: INT,
     video_ad: INT
     >
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/tmp/output_data/market_analysis3/';

SELECT * FROM market_analysis2;

OUTPUT:
OK
market_analysis2.campaign_id    market_analysis2.date   market_analysis2.hour   market_analysis2.gender  market_analysis2.event
ABCDFAE 2018-10-12      13      male    {"impression":1,"click":1,"video_ad":null}
ABCDFAE 2018-10-12      13      female  {"impression":1,"click":0,"video_ad":null}
Time taken: 0.199 seconds, Fetched: 2 row(s)


TAKEWAY POINTS:
1. location should always be end statement when creating a external table
2. keywords as column names should be surrounded by escape chars.
