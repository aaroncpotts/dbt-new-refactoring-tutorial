with 

-- Import CTEs

customers as (

  select * from {{ source('jaffle_shop', 'customers') }}

),

orders as (

  select * from {{ source('jaffle_shop', 'orders') }}

),

payments as (

  select * from {{ source('stripe', 'payment') }}

),
-- Logical CTEs

aggregate_payments as (
    select
         orderid as order_id,
         max(created) as payment_finalized_date, 
         sum(amount) / 100.0 as total_amount_paid
from payments
where status <> 'fail'
group by 1),

paid_orders as 
    (select orders.id as order_id,
        orders.user_id as customer_id,
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
    left join orders  as orders
    on orders.user_id = customers.id 
    group by 1),

lifetime_value_generate as (
            select
            paid_orders.order_id,
            sum(t2.total_amount_paid) as clv_bad
        from paid_orders
        left join paid_orders t2 on paid_orders.customer_id = t2.customer_id and paid_orders.order_id >= t2.order_id
        group by 1
        order by paid_orders.order_id

)

-- Final CTE
-- Simple Select Statment


select
    paid_orders.*,
    row_number() over (order by paid_orders.order_id) as transaction_seq,
    row_number() over (partition by customer_id order by paid_orders.order_id) as customer_sales_seq,
    case when customer_orders.first_order_date = paid_orders.order_placed_at
    then 'new'
    else 'return' end as nvsr,
    lifetime_value_generate.clv_bad as customer_lifetime_value,
    customer_orders.first_order_date as fdos
    from paid_orders
    left join customer_orders using (customer_id)
    left outer join lifetime_value_generate on lifetime_value_generate.order_id = paid_orders.order_id
    order by order_id