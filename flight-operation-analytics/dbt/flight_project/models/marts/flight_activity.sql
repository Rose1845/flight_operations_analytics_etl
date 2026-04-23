
{{config(
    materialized="table",
    unique_key="id"
)}}


SELECT
    s.id                                        AS snapshot_id,
    TO_TIMESTAMP(s.snapshot_time)               AS snapshot_ts,
    DATE_TRUNC('hour', TO_TIMESTAMP(s.snapshot_time))  AS snapshot_hour,
    DATE_TRUNC('day',  TO_TIMESTAMP(s.snapshot_time))  AS snapshot_day,
    COUNT(sv.id)                                AS total_flights,
    COUNT(sv.id) FILTER (WHERE sv.on_ground = FALSE) AS airborne_flights,
    COUNT(sv.id) FILTER (WHERE sv.on_ground = TRUE)  AS grounded_flights,
    COUNT(DISTINCT sv.origin_country)           AS unique_countries
FROM {{ ref('flight_staging') }} s
LEFT JOIN {{ ref('state_vectors_staging') }} sv
    ON sv.snapshot_id = s.id
GROUP BY
    s.id,
    s.snapshot_time