{{
    config(
        materialized='view'
    )
}}

with 

source as (

    select * from {{ source('full_trips', 'fhv_trips_data') }} 
    where  pickup_datetime between '2019-01-01' and '2020-01-01' 

),

renamed as (

    select
        dispatching_base_num,
        cast(pickup_datetime as timestamp) as pickup_datetime,
        cast(dropoff_datetime as timestamp) as dropoff_datetime,
        pulocationid as pickup_locationid,
        dolocationid as dropoff_locationid,
        sr_flag,
        affiliated_base_number

    from source

)


select * from renamed

-- dbt build --select <model.sql> --vars '{'is_test_run: false}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}