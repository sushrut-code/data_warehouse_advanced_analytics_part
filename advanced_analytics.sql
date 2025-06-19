select 
year(order_date) as order_year,
sum(sales_amount) as total_sales,
count(distinct customer_key) as total_cust,
sum(quantity) as total_quant from gold.fact_sales
where order_date is not null
group by year(order_date)
order by year(order_date);



select 
month(order_date) as order_year,
sum(sales_amount) as total_sales,
count(distinct customer_key) as total_cust,
sum(quantity) as total_quant from gold.fact_sales
where order_date is not null
group by month(order_date)
order by month(order_date);

select 
year(order_date) as order_year,
month(order_date) as order_month,
sum(sales_amount) as total_sales,
count(distinct customer_key) as total_cust,
sum(quantity) as total_quant from gold.fact_sales
where order_date is not null
group by year(order_date), month(order_date)
order by year(order_date),month(order_date);




select
order_year,
order_month,
total_sales,
sum(total_sales) over(partition by order_year order by order_year,order_month) as running_total_sales,
round(avg(total_average) over(order by order_year,order_month),2) as moving_avg_price
from (select 
year(order_date) as order_year,
month(order_date) as order_month,
sum(sales_amount) as total_sales,
count(distinct customer_key) as total_cust,
avg(price) as total_average,
sum(quantity) as total_quant from gold.fact_sales

where order_date is not null
group by year(order_date), month(order_date)
order by year(order_date),month(order_date)) t;


select 
year(f.order_date) as order_year,
p.product_name,
sum(f.sales_amount) as curr_sales 
from gold.fact_sales f
left join gold.dim_products p
on f.product_key=p.product_key
where order_date is not null
group by year(f.order_date),p.product_name;




with yearly_product_sales as (
select 
year(f.order_date) as order_year,
p.product_name,
sum(f.sales_amount) as curr_sales 
from gold.fact_sales f
left join gold.dim_products p
on f.product_key=p.product_key
where order_date is not null
group by year(f.order_date),p.product_name
)

select
order_year,
product_name,
curr_sales,
round(avg(curr_sales) over(partition by product_name),2) as avg_sales,
(curr_sales- round(avg(curr_sales) over(partition by product_name),2)) as diff_avg,
case
when (curr_sales- round(avg(curr_sales) over(partition by product_name),2)) > 0 then "above avg"
when (curr_sales- round(avg(curr_sales) over(partition by product_name),2)) < 0 then "below avg"
else "avg"
end as remarks,
lag(curr_sales) over(partition by product_name order by order_year) as prev_sales,
curr_sales- lag(curr_sales) over(partition by product_name order by order_year) as diff_py,
case
when curr_sales- lag(curr_sales) over(partition by product_name order by order_year) > 0 then "Increase"
when curr_sales- lag(curr_sales) over(partition by product_name order by order_year)<0 then "Decrease"
else "No Change"
end as remarks
from yearly_product_Sales
order by product_name,order_year;


with category_sales as (
select
category,

sum(sales_amount) as total_sales
from gold.fact_sales f
left join gold.dim_products p
on f.product_key=p.product_key
where category is not null
group by category
)

select category,
total_sales,
sum(total_sales) over() as overall_sales,
concat(round((total_sales)*100/(sum(total_sales) over()),2),"%") as percentage
from category_sales
order by total_sales desc;


with product_segments as (
select product_key,
product_name,
cost,
case
when cost < 100 then "Below 100"
when 100 < cost < 500 then "100-500"
when 500 < cost < 1000 then "500-1000"
else "Above 1000"
end as cost_range
from gold.dim_products
)

select
cost_range,
count(product_key) as total_products
from product_segments
group by cost_range;



with customer_spending as (
select
c.customer_key,
sum(f.sales_amount) as total_spending,
min(f.order_date) as first_order,
max(f.order_date) as last_order,
round(datediff(max(f.order_date),min(f.order_date))/30) as life_span
from gold.fact_sales f
left join gold.dim_customers c
on f.customer_key=c.customer_key
group by customer_key
)

select
customer_segment,
count(customer_key) as total_customers
 from
(select customer_key,
case
when life_span >= 12 and total_spending > 5000 then "VIP"
when  life_span >= 12 and total_spending <= 5000 then "Regular"
when life_span < 12 then "New"
else "n/a"
end as customer_segment

from customer_spending) t
group by customer_segment
order by total_customers;



create view report_customer1 as 
with base_query as (
select f.order_number,
f.product_key,
f.order_date,
f.sales_amount,
f.quantity,
c.customer_key,
c.customer_number,
concat(c.first_name,
" ",c.last_name) as customer_name,
year(current_date())-year(c.birthdate) as age
from gold.fact_sales f
left join gold.dim_customers c
on f.customer_key=c.customer_key
where order_date is not null
),

customer_aggregation as (
select
customer_key,
customer_number,
customer_name,
age,
count(distinct order_number) as total_orders,
sum(sales_amount) as total_sales,
count(distinct product_key) as total_products,
max(order_date) as last_order_date,
round(datediff(max(order_date),min(order_date))/30) as life_span
from base_query
group by 
	customer_key,
	customer_number,
	customer_name,
	age
    )
    
    select
	customer_key,
	customer_number,
	customer_name,
	age,
    last_order_date,
    round(datediff(current_date,last_order_date)/30) as recency,
    case
		when age < 20 then "Under 20"
		when age between 20 and 29 then "20-29"
		when age between 30 and 39 then "30-39"
		when age between 40 and 49 then "40-49"
		else " above 50"
    end as age_group,
    case
		when life_span >= 12 and total_sales > 5000 then "VIP"
		when  life_span >= 12 and total_sales <= 5000 then "Regular"
		when life_span < 12 then "New"
		else "n/a"
	end as customer_segment,
    total_orders,
	total_sales,
	total_products,
	life_span,
    case
    when total_orders=0 then 0
    else round(total_sales/total_orders,2) 
    end as avg_order_value,
    case
    when life_span=0 then total_sales
    else round(total_sales/life_span,2) 
    end as avg_monthly_spend
    from customer_aggregation;


select * from gold.report_customer1;
