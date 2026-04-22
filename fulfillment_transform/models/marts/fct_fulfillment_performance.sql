{{ config(materialized='table') }}

with staging as (
    -- Read from our clean prep station!
    select * from {{ ref('stg_fulfillment_events') }}
),

business_logic as (
    select
        order_number,
        fulfillment_week,
        dc_name,
        fulfillment_line_id,
        order_status,

        -- KPI 1: Total time before release
        datediff('minute', approved_timestamp, released_timestamp) as time_taken_to_release,
       
        -- KPI 2: Total time on the line
        datediff('minute', released_timestamp, pick_complete_timestamp) as minutes_on_line,
       
        -- KPI 3: Flag packages that are heavier than expected
        case
            when actual_weight > expected_weight then true
            when actual_weight <= expected_weight then false
        end as is_overweight,
       
        -- KPI 4: Flag missing items (under-picks)
        case
            when picked_item_qty < expected_item_qty then true
            when picked_item_qty = expected_item_qty then false
        end as has_missing_items,

    from staging
)

select * from business_logic