SELECT
  CAST(b.metric_date AS date) AS "metric_date",
  CAST(b.metric_hour AS integer) AS "metric_hour",
  CAST(b.http_route AS varchar) AS "http_route",
  CAST(b.http_request_method AS varchar) AS "http_request_method",
  CAST(b.total_requests AS bigint) AS "total_requests",
  CAST(b.success_2xx_count AS bigint) AS "success_2xx_count",
  CAST(b.client_error_4xx_count AS bigint) AS "client_error_4xx_count",
  CAST(b.server_error_5xx_count AS bigint) AS "server_error_5xx_count",
  CAST(b.error_rate_pct AS double) AS "error_rate_pct",
  CAST(b.avg_duration_ms AS double) AS "avg_duration_ms",
  CAST(b.p50_duration_ms AS double) AS "p50_duration_ms",
  CAST(b.p95_duration_ms AS double) AS "p95_duration_ms",
  CAST(b.p99_duration_ms AS double) AS "p99_duration_ms",
  CAST(CAST(current_timestamp AS timestamp(6) with time zone) AS timestamp(6) with time zone) AS "_gold_ingested_at",
  CAST('{{ var("run_id", "unconfigured") }}' AS varchar) AS "_source_run_id"
FROM (
SELECT event_date metric_date,hour(event_ts) metric_hour,http_route,http_request_method,count(*) total_requests,
count_if(is_success) success_2xx_count,count_if(is_client_error) client_error_4xx_count,count_if(is_server_error) server_error_5xx_count,
round((count_if(is_client_error)+count_if(is_server_error))*100e0/count(*),2) error_rate_pct,
round(avg(duration_ms),3) avg_duration_ms,round(element_at(array_sort(array_agg(duration_ms) FILTER (WHERE duration_ms IS NOT NULL)),greatest(1,CAST(ceil(count(duration_ms)*0.5) AS bigint))),3) p50_duration_ms,
round(element_at(array_sort(array_agg(duration_ms) FILTER (WHERE duration_ms IS NOT NULL)),greatest(1,CAST(ceil(count(duration_ms)*0.95) AS bigint))),3) p95_duration_ms,
round(element_at(array_sort(array_agg(duration_ms) FILTER (WHERE duration_ms IS NOT NULL)),greatest(1,CAST(ceil(count(duration_ms)*0.99) AS bigint))),3) p99_duration_ms
FROM {{ pinned('fact_web_events') }} WHERE event_ts <= (SELECT observed_at FROM {{ clock() }} clock)
GROUP BY event_date,hour(event_ts),http_route,http_request_method
) b
