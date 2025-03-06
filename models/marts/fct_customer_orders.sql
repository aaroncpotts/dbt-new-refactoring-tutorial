with 


paid_orders as (
    select * from {{ ref('int_orders') }}
),

final as (select
    paid_orders.customer_id,
    paid_orders.order_id,
    paid_orders.order_placed_at,
    paid_orders.order_status,
    paid_orders.payment_finalized_date,
    paid_orders.customer_first_name,
    paid_orders.customer_last_name,
    -- sales transaction sequence - for fully paid orders, by order ID
    row_number() over (order by paid_orders.order_id) as transaction_seq,
    --customer sales sequence - partitioned by customer, ordered by order ID
    row_number() over (partition by customer_id order by paid_orders.order_id) as customer_sales_seq,
    /* first order date - use window function partitioned by order_id to arrange orders by order_placed_at, then return 'new' if
    it's the first ranked order*/
    case
         when (
            rank() over (
                partition by paid_orders.customer_id
                order by paid_orders.order_placed_at, paid_orders.order_id
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
    )
-- Simple Select Statment

select * from final
order by order_id