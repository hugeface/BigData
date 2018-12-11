-- 统计user和对应product在多少个订单中出现
select user_id, product_id, count(distinct orders.order_id) from
orders join order_products_prior
on (orders.order_id = order_products_prior.order_id)
group by user_id, product_id
limit 10;
