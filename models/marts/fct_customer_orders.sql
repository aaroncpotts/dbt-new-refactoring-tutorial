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
        --order_placed_at used to order the customer_lifetime_value later
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

customer_orders 
    as (select customers.id as customer_id
        , min(order_date) as first_order_date
        , max(order_date) as most_recent_order_date
        , count(orders.id) as number_of_orders
    from customers 
    left join orders
    on orders.user_id = customers.id 
    group by 1),


-- Final CTE

final as (select
    paid_orders.*,
    -- sales transaction sequence - for fully paid orders, by order ID
    row_number() over (order by paid_orders.order_id) as transaction_seq,
    --customer sales sequence - partitioned by customer, ordered by order ID
    row_number() over (partition by customer_id order by paid_orders.order_id) as customer_sales_seq,
    case when customer_orders.first_order_date = paid_orders.order_placed_at
    then 'new'
    else 'return' end as nvsr,
    /*customer lifetime value - sum of total_amount_paid from paid orders, partitioned by customer id
    then ordered by the order_placed_at value from paid_orders*/
    sum(total_amount_paid) over(
        partition by paid_orders.customer_id
        order by paid_orders.order_placed_at
        ) as customer_lifetime_value,
    customer_orders.first_order_date as fdos
    from paid_orders
    left join customer_orders using (customer_id)
    order by order_id)
-- Simple Select Statment

select * from final
