{{config(
    materialized="table",
    unique_key="id"
)}}

SELECT
    sv.origin_country,
    COUNT(DISTINCT sv.icao24)                           AS unique_aircraft,
    COUNT(sv.id)                                        AS total_state_vectors,
    COUNT(sv.id) FILTER (WHERE sv.on_ground = FALSE)    AS airborne_count,
    COUNT(sv.id) FILTER (WHERE sv.on_ground = TRUE)     AS grounded_count,
    ROUND(AVG(sv.velocity)::numeric, 2)                 AS avg_velocity_ms,
    ROUND(AVG(sv.baro_altitude)::numeric, 2)            AS avg_baro_altitude_m,
    ROUND(AVG(sv.geo_altitude)::numeric, 2)             AS avg_geo_altitude_m,
    ROUND(AVG(sv.vertical_rate)::numeric, 4)            AS avg_vertical_rate_ms,
    COUNT(DISTINCT sv.snapshot_id)                      AS snapshots_seen_in
FROM {{ ref('state_vectors_staging') }} sv
WHERE sv.origin_country IS NOT NULL
GROUP BY sv.origin_country
ORDER BY unique_aircraft DESC