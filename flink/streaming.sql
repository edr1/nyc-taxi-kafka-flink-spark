SET 'execution.checkpointing.interval' = '30 s';
SET 'parallelism.default' = '1';
CREATE TABLE taxi_events (
  event_id STRING,
  tpep_pickup_datetime STRING,
  PULocationID INT,
  total_amount DOUBLE,
  proc_time AS PROCTIME()
) WITH (
 'connector'='kafka', 'topic'='nyc.taxi.trips',
 'properties.bootstrap.servers'='kafka:29092',
 'properties.group.id'='taxi-flink-v2',
 'scan.startup.mode'='earliest-offset',
 'format'='json', 'json.ignore-parse-errors'='true'
);
CREATE TABLE live_metrics (
 window_start TIMESTAMP(3), window_end TIMESTAMP(3),
 pickup_zone INT, trips BIGINT, revenue DOUBLE
) WITH ('connector'='print', 'print-identifier'='FLINK_P2_WINDOW');
CREATE TABLE hourly_patterns (
 pickup_hour BIGINT, weekday_num BIGINT, trips BIGINT
) WITH ('connector'='print', 'print-identifier'='FLINK_P2_HOUR_WEEKDAY');
CREATE TABLE live_top3 (
 window_start TIMESTAMP(3), window_end TIMESTAMP(3),
 pickup_zone INT, trips BIGINT, zone_rank BIGINT
) WITH ('connector'='print', 'print-identifier'='FLINK_P7_TOP3');
-- Keep queries inline to avoid the Flink 1.20.1 view/planner bug FLINK-37222: "Table planner exception when sql client submit job":
-- The problem stems from reusing views across different TableEnvironment instances inside the SQL Client framework
EXECUTE STATEMENT SET
BEGIN
 INSERT INTO live_metrics
 SELECT window_start,window_end,PULocationID,COUNT(*),SUM(total_amount)
 FROM TABLE(TUMBLE(TABLE taxi_events, DESCRIPTOR(proc_time), INTERVAL '1' MINUTE))
 WHERE PULocationID BETWEEN 1 AND 263 AND total_amount >= 0
  AND tpep_pickup_datetime IS NOT NULL
 GROUP BY window_start,window_end,PULocationID;
 INSERT INTO hourly_patterns
 SELECT HOUR(pickup_ts), DAYOFWEEK(pickup_ts), COUNT(*)
 FROM (
  SELECT CAST(tpep_pickup_datetime AS TIMESTAMP(3)) AS pickup_ts
  FROM taxi_events
  WHERE PULocationID BETWEEN 1 AND 263 AND total_amount >= 0
   AND tpep_pickup_datetime IS NOT NULL
 ) WHERE pickup_ts IS NOT NULL
 GROUP BY HOUR(pickup_ts), DAYOFWEEK(pickup_ts);
 INSERT INTO live_top3
 -- String partition keys force regular Top-N instead of unsupported proctime Window Top-N.
 -- Regular Top-N emits changelog updates and retains state for past windows (no TTL).
 SELECT window_start,window_end,PULocationID,trips,zone_rank FROM (
   SELECT window_start,window_end,PULocationID,trips,
   ROW_NUMBER() OVER (PARTITION BY CAST(window_start AS STRING),CAST(window_end AS STRING)
                      ORDER BY trips DESC, PULocationID ASC) AS zone_rank
   FROM (
    SELECT window_start,window_end,PULocationID,COUNT(*) AS trips
    FROM TABLE(TUMBLE(TABLE taxi_events, DESCRIPTOR(proc_time), INTERVAL '1' MINUTE))
    WHERE PULocationID BETWEEN 1 AND 263 AND total_amount >= 0
     AND tpep_pickup_datetime IS NOT NULL
    GROUP BY window_start,window_end,PULocationID
   )
 ) WHERE zone_rank <= 3;
END;
