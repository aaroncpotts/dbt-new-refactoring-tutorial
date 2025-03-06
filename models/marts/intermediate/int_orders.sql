with customers as (

  select * from {{ ref('stg_jaffle_shop__customers') }}

),

orders as (

  select * from {{ ref('stg_jaffle_shop__orders') }}

),

aggregate_payments as (
    select * from {{ ref('stg_stripe__aggregate_payments') }}
),


paid_orders as 
    (select 
        orders.order_id,
        orders.customer_id,
        --order_placed_at used to order the customer_lifetime_value and other window functions later
        orders.order_placed_at,
        orders.order_status,
        aggregate_payments.total_amount_paid,
        aggregate_payments.payment_finalized_date,
        customers.customer_first_name,
        customers.customer_last_name
    from orders
    left join aggregate_payments on orders.order_id = aggregate_payments.order_id
    left join customers on orders.customer_id = customers.customer_id 
)

select * from paid_orders