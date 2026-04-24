{{ config(materialized='view') }}

with source as (
    -- Reference the source we just defined in sources.yml
    select * from {{ source('raw_data', 'fulfillment_events') }}
),

renamed_and_cleaned as (
    select
        -- Identifiers
        order_number,
        SPLIT_PART(order_number, '-', 2) as customer_id,
        tracking_number,
        cast(fulfillment_line_id as integer) as fulfillment_line_id,
        site_name,
        CASE 
            WHEN site_name = 'Bengaluru (Karnataka)' THEN 'KA-1'
            WHEN site_name = 'Ahmedabad (Gujarat)' THEN 'GJ-1'
            WHEN site_name = 'Mumbai (Maharashtra)' THEN 'MH-1'
            WHEN site_name = 'Pune (Maharashtra)' THEN 'MH-2'
        END AS dc_name,

        -- Timestamps & Dates (Casting them just to be safe!)
        cast(expected_packing_date as date) as expected_packing_date,
        cast(expected_delivery_date as date) as expected_delivery_date,
        cast(approved_timestamp as timestamp) as approved_timestamp,
        cast(released_timestamp as timestamp) as released_timestamp,
        cast(pick_start_timestamp as timestamp) as pick_start_timestamp,
        cast(pick_complete_timestamp as timestamp) as pick_complete_timestamp,

        -- Metrics & Dimensions
        order_status,
        expected_weight,
        actual_weight,
        box_size,
        expected_item_qty,
        cast(picked_item_qty as integer) as picked_item_qty,
        year,
        week_number,
        CONCAT(year, '-', week_number) as fulfillment_week,
        _extracted_at,
        _loaded_at

    from source
)

select * from renamed_and_cleaned