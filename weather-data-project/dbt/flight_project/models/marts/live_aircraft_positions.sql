
{{config(
    materialized="table",
    unique_key="id"
)}}

WITH ranked AS (
    SELECT
        sv.icao24,
        sv.callsign,
        sv.origin_country,
        sv.longitude,
        sv.latitude,
        sv.baro_altitude,
        sv.geo_altitude,
        sv.velocity,
        sv.true_track,
        sv.vertical_rate,
        sv.on_ground,
        sv.squawk,
        sv.position_source,
        sv.category,
        TO_TIMESTAMP(sv.time_position)  AS position_ts,
        TO_TIMESTAMP(sv.last_contact)   AS last_contact_ts,
        ROW_NUMBER() OVER (
            PARTITION BY sv.icao24
            ORDER BY sv.last_contact DESC NULLS LAST
        ) AS rn
    FROM {{ ref('state_vectors_staging') }} sv
    WHERE sv.longitude IS NOT NULL
      AND sv.latitude  IS NOT NULL
)

SELECT
    icao24,
    callsign,
    origin_country,
    longitude,
    latitude,
    baro_altitude,
    geo_altitude,
    velocity,
    true_track,
    vertical_rate,
    on_ground,
    squawk,
    position_source,
    category,
    position_ts,
    last_contact_ts
FROM ranked
WHERE rn = 1