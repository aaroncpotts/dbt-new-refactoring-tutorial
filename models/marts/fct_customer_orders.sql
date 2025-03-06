with 

-- Import CTEs

customers as (

  select * from {{ source('jaffle_shop', 'customers') }}

),

orders as (

  select * from {{ source('jaffle_shop', 'orders') }}

),

aggregate_payments as (
    select * from {{ ref('stg_stripe__aggregate_payments') }}
),
-- Logical CTEs

paid_orders as 
    (select orders.id as order_id,
        orders.user_id as customer_id,
        --order_placed_at used to order the customer_lifetime_value and other window functions later
        orders.order_date as order_placed_at,
        orders.status as order_status,
        aggregate_payments.total_amount_paid,
        aggregate_payments.payment_finalized_date,
        customers.first_name as customer_first_name,
        customers.last_name as customer_last_name
    from orders
    left join aggregate_payments on orders.id = aggregate_payments.order_id
    left join customers on orders.user_id = customers.id 
),



-- Final CTE

final as (select
    paid_orders.*,
    -- sales transaction sequence - for fully paid orders, by order ID
    row_number() over (order by paid_orders.order_id) as transaction_seq,
    --customer sales sequence - partitioned by customer, ordered by order ID
    row_number() over (partition by customer_id order by paid_orders.order_id) as customer_sales_seq,
    /* first order date - use window function partitioned by order_id to arrange orders by order_placed_at, then return 'new' if
    it's the first ranked order*/
    case
         when (
            rank() over (
                partition by customer_id
                order by order_placed_at, order_id
            ) = 1
        ) then 'new'
    else 'return' end as nvsr,
    /*customer lifetime value - sum of total_amount_paid from paid orders, partitioned by customer id
    then ordered by the order_placed_at value from paid_orders*/
    sum(total_amount_paid) over(
        partition by paid_orders.customer_id
        order by paid_orders.order_placed_at
        ) as customer_lifetime_value,
    /* hajimete no first_value - SQL Server 2012 window function. Thing you're finding the first order for, over() what you're partitioning by
    (in this case, customer_id), what you're ordering by */
    first_value(paid_orders.order_placed_at) over(
        partition by paid_orders.customer_id
        order by paid_orders.order_placed_at
    ) as fdos
    from paid_orders
    order by order_id)
-- Simple Select Statment

select * from final
