{{config(
    materialized="table",
    unique_key="id"
)}}

select * from {{source('flight','flight_snapshots')}}