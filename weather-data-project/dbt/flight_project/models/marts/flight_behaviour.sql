{{config(
    materialized="table",
    unique_key="id"
)}}

SELECT
    sv.icao24,
    MAX(sv.callsign)                                AS callsign,
    MAX(sv.origin_country)                          AS origin_country,

    ROUND(AVG(sv.velocity)::numeric, 2)             AS avg_speed_ms,
    ROUND(MAX(sv.velocity)::numeric, 2)             AS max_speed_ms,
    ROUND(MIN(sv.velocity) FILTER (WHERE sv.velocity > 0)::numeric, 2) AS min_speed_ms,

    ROUND(AVG(sv.baro_altitude)::numeric, 2)        AS avg_baro_alt_m,
    ROUND(MAX(sv.baro_altitude)::numeric, 2)        AS max_baro_alt_m,
    ROUND(AVG(sv.geo_altitude)::numeric, 2)         AS avg_geo_alt_m,
    ROUND(MAX(sv.geo_altitude)::numeric, 2)         AS max_geo_alt_m,

    ROUND(AVG(sv.vertical_rate)::numeric, 4)        AS avg_vertical_rate_ms,
    ROUND(MAX(sv.vertical_rate)::numeric, 4)        AS max_climb_rate_ms,
    ROUND(MIN(sv.vertical_rate)::numeric, 4)        AS max_descent_rate_ms,

    COUNT(sv.id)                                    AS total_observations,
    COUNT(sv.id) FILTER (WHERE sv.on_ground = FALSE) AS airborne_observations,
    COUNT(sv.id) FILTER (WHERE sv.on_ground = TRUE)  AS ground_observations,

    TO_TIMESTAMP(MIN(sv.last_contact))              AS first_seen,
    TO_TIMESTAMP(MAX(sv.last_contact))              AS last_seen

FROM {{ ref('state_vectors_staging') }} sv
WHERE sv.icao24 IS NOT NULL
GROUP BY sv.icao24