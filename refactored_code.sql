-- State all the tables you're going to choose from (also don't use subqueries)

with orders as (
  select * from {{ref('stg_orders')}}
  where state != 'canceled'
    and extract(year from completed_at) < '2018'
    and email not like '%company.com'
),

order_items as (
  select * from {{ref('stg_order_items')}}
),

order_totals as (
  select
  -- aggregating total rev value bc xyz
    id,
    number,
    completed_at,
    completed_at::date as completed_at_date,
    sum(total) as net_rev,
    sum(item_total) as gross_rev,
    count(id) as order_count
  from orders
  group by 1, 2, 3
),

orders_complete as (
  select
    order_items.order_id,
    orders.completed_at::date as completed_at_date,
    sum(order_items.quantity) as qty
  from order_items
    left join orders using order_id
  where (orders.is_cancelled_order = false or orders.is_pending_order != true)
  group by 1, 2
),

final as (
  select
    order_totals.completed_at_date,
    order_totals.gross_rev,
    order_totals.net_rev,
    orders_complete.qty,
    order_totals.order_count as orders,
    orders_complete.qty/order_totals.distinct_orders as avg_unit_per_order,
    order_totals.Gross_Rev/order_totals.distinct_orders as aov_gross,
    order_totals.Net_Rev/order_totals.distinct_orders as aov_net
  from order_totals
    join orders_complete using completed_at_date
  where order_totals.net_rev >= 150000
  order by completed_at_date desc
)

select * from final
