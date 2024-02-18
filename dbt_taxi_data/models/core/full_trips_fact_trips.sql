
{{
    config(
        materialized='table'
    )
}}

with green_tripdata as (
    select pickup_datetime, dropoff_datetime,pickup_locationid,dropoff_locationid,
        'Green' as service_type
    from {{ ref('stg_full_trips_green_tripdata') }}
), 
yellow_tripdata as (
    select pickup_datetime, dropoff_datetime,pickup_locationid,dropoff_locationid,
        'Yellow' as service_type
    from {{ ref('stg_full_trips_yellow_tripdata') }}
), 
fhv_tripdata as (
    select pickup_datetime, dropoff_datetime,pickup_locationid,dropoff_locationid,
        'Fhv' as service_type
    from {{ ref('stg_full_trips_fhv_tripdata') }}
), 
trips_unioned as (
    select * from green_tripdata
    union all 
    select * from yellow_tripdata
    union all 
    select * from fhv_tripdata
), 
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)


select 
    tripsdata.service_type,
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    tripsdata.pickup_datetime,
    tripsdata.dropoff_datetime
from trips_unioned as tripsdata
inner join dim_zones as pickup_zone
on tripsdata.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on tripsdata.dropoff_locationid = dropoff_zone.locationid