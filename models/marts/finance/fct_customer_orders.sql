-- with statements
with
-- import CTEs
base_costomers as (
    select * from {{ ref('dim_customers') }}
),

orders as (
    select * from {{ ref('stg_jaffle_shop__orders') }}
),

payments as (
    select * from {{ ref('stg_stripe__payments') }}
),

-- logical CTEs

a as (

    select

        row_number() over (
            partition by customer_id
            order by order_date, order_id
        ) as user_order_seq,
        *

    from orders
),

b as ( 

    select 

        first_name || ' ' || last_name as name, 
        * 

    from base_costomers

),

customer_order_history as (

    select 

        b.customer_id as customer_id,
        b.name as full_name,
        b.last_name as surname,
        b.first_name as givenname,

        min(order_date) as first_order_date,

        min(case 
            when a.status not in ('returned','return_pending') 
            then order_date 
        end) as first_non_returned_order_date,

        max(case 
            when a.status not in ('returned','return_pending') 
            then order_date 
        end) as most_recent_non_returned_order_date,

        coalesce(max(user_order_seq),0) as order_count,

        coalesce(count(case 
            when a.status != 'returned' 
            then 1 end),
            0
        ) as non_returned_order_count,

        sum(case 
            when a.status not in ('returned','return_pending') 
            then round(c.amount/100.0,2) 
            else 0 
        end) as total_lifetime_value,

        sum(case 
            when a.status not in ('returned','return_pending') 
            then round(c.amount/100.0,2) 
            else 0 
        end)
        / nullif(count(case 
            when a.status not in ('returned','return_pending') 
            then 1 end),
            0
        ) as avg_non_returned_order_value,

        array_agg(distinct a.order_id) as order_ids

    from a

    join b
    on a.customer_id = b.customer_id

    left outer join payments as c
    on a.order_id = c.order_id

    where a.status not in ('pending') and c.status != 'fail'

    group by b.customer_id, b.name, b.last_name, b.first_name

),

-- Final CTEs 
final as (

    select 

        orders.order_id as order_id,
        orders.customer_id as customer_id,
        last_name as surname,
        first_name as givenname,
        customer_order_history.first_order_date,
        order_count,
        total_lifetime_value,
        round(amount/100.0,2) as order_value_dollars,
        orders.status as order_status,
        payments.status as payment_status

    from orders

    join base_costomers
    on orders.customer_id = base_costomers.customer_id

    join customer_order_history
    on orders.customer_id = customer_order_history.customer_id

    left outer join payments
    on orders.order_id = payments.order_id

    where payments.status != 'fail'

)

-- Simple Select Statement
select * from final