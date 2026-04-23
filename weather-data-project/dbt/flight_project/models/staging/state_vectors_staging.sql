{{config(
    materialized="table",
    unique_key="id"
)}}

select * from {{source('flight','state_vectors')}}