{{ config(tags=['logs']) }}
SELECT event_id FROM {{ ref('int_resolved_events') }} GROUP BY event_id HAVING count(*)<>1
