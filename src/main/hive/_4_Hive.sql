-- 查询每个用户最喜爱的top 10%
select user_id, ceil((count(1) as double)*0.1) as total_prod_id
from orders join order_products_prior pri
on orders.order_id=pri.order_id
group by user_id
limit 10;

--
select user_id, product_id,
row_number() over(partition by user_id order by usr_prod_cnt desc) as row_num
from
(select user_id, product_id,
count(1) as usr_prod_cnt
from orders join order_products_prior pri
on orders.order_id=pri.order_id)t
limit 10;