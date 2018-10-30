-- Two tables: order_products_prior, orders

-- 查询每个用户最喜爱的top 10%
select user_id, ceil(cast(count(distinct pri.product_id) as double)*0.1) as total_prod_cnt
from orders join order_products_prior pri
on orders.order_id=pri.order_id
group by user_id  -- 根据user_id进行聚合
limit 10;


select user_id, product_id,
row_number() over(partition by user_id order by usr_prod_cnt desc) as row_num
from
(select user_id, product_id,
count(1) as usr_prod_cnt
from orders join order_products_prior pri
on orders.order_id=pri.order_id)t
limit 10;