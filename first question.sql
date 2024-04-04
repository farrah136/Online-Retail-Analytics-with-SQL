select * 
from tableretail
order by customer_id ,invoicedate desc

---------------
-- Customer Purchase Behavior Analysis:
select  CUSTOMER_ID , count(distinct(invoice)),
 round(avg (last_val - first_val),2) as average 
   from (
   select CUSTOMER_ID , invoice,
   last_value(to_date(invoicedate,'mm/dd/yyyy HH24:MI')) 
   over(partition by CUSTOMER_ID order by to_date(invoicedate,'mm/dd/yyyy HH24:MI') range between unbounded preceding and unbounded following) as last_val ,
first_value(to_date(invoicedate,'mm/dd/yyyy HH24:MI')) over(partition by CUSTOMER_ID order by to_date(invoicedate,'mm/dd/yyyy HH24:MI')) as first_val
from tableretail)
group by customer_id
order by average;

--------------------------------
--Average Purchase Value per Product:
select distinct(stockcode),total_quantity,total_sales,round( avg (total_sales/total_quantity) over(partition by stockcode) ,2) as avg
 from( select stockcode,sum(quantity) over(partition by stockcode) as total_quantity ,
sum(quantity*price) over (partition by stockcode) as total_sales
from tableretail)
order by total_sales desc
;
-----------------------
--clv

select distinct(customer_id) ,TOTAL_SALES,round((last_val-first_val),2)as diff,round(TOTAL_SALES/nullif((last_val-first_val),0),2) as clv
 from (
   select CUSTOMER_ID ,price, quantity,
   SUM(QUANTITY * PRICE) OVER (PARTITION BY CUSTOMER_ID) AS TOTAL_SALES,
   last_value(to_date(invoicedate,'mm/dd/yyyy HH24:MI'))
    over(partition by CUSTOMER_ID order by to_date(invoicedate,'mm/dd/yyyy HH24:MI') range between unbounded preceding and unbounded following) as last_val ,
first_value(to_date(invoicedate,'mm/dd/yyyy HH24:MI')) over(partition by CUSTOMER_ID order by to_date(invoicedate,'mm/dd/yyyy HH24:MI')) as first_val
from tableretail)
order by customer_id


-----------------------
--top 10 prdoducts
select distinct(stockcode), total_quant from(
select stockcode , sum(quantity)  total_quant,
row_number() over ( order by sum(quantity) desc) as top
from tableretail
group by stockcode
) 
where top <=10;
--------
--top 5% of customers
select distinct(customer_id),total_sales,round(rankk,2) as rank from (
select customer_id, sum(quantity *price )  total_sales ,
percent_rank() over(order by (sum(quantity *price )) desc) as rankk
from tableretail
group by customer_id
)
where rankk<=0.05

----------------------
--number of customers every month
select count(distinct customer_id)  as customer_count,
to_char(to_date(invoicedate,'mm/dd/yyyy HH24:MI'),'mm,yyyy') as month_year
from tableretail
group by to_char(to_date(invoicedate,'mm/dd/yyyy HH24:MI'),'mm,yyyy')
order by customer_count desc

---------------------
---total sales every month
select sum(quantity * price) as total_sales , to_char(to_date(invoicedate,'mm/dd/yyyy HH24:MI'),'mm,yyyy') as month_year
from tableretail
group by to_char(to_date(invoicedate,'mm/dd/yyyy HH24:MI'),'mm,yyyy')
order by total_sales desc
---------------------
--products and quantity sale for each month 
select stockcode , sum(quantity) quant ,to_char(to_date(invoicedate,'mm/dd/yyyy HH24:MI'),'mm,yyyy') as month_year
from tableretail
group by stockcode,to_char(to_date(invoicedate,'mm/dd/yyyy HH24:MI'),'mm,yyyy')
order by quant desc

--------------
--  Monthly Sales Growth Rate
with monthlysales as (
  select sum(quantity * price) as total_sales,
    to_char(to_date(invoicedate,'mm/dd/yyyy hh24:mi'),'mm,yyyy') as month_year
  from tableretail
  group by to_char(to_date(invoicedate,'mm/dd/yyyy hh24:mi'),'mm,yyyy')
)
select current_month.month_year,
  current_month.total_sales as current_month_sales,
  previous_month.total_sales as previous_month_sales,
  round(((current_month.total_sales - previous_month.total_sales) / previous_month.total_sales) * 100, 2) as growth_rate_percent
from monthlysales current_month
join monthlysales previous_month on to_date(current_month.month_year, 'mm,yyyy') = add_months(to_date(previous_month.month_year, 'mm,yyyy'), -1)
order by to_date(current_month.month_year, 'mm,yyyy');


-------------------------
--Customer Acquisition Rate:
with newcustomers as (
  select extract(year from min(to_date(invoicedate,'mm/dd/yyyy hh24:mi'))) as acquisition_year,
    extract(month from min(to_date(invoicedate,'mm/dd/yyyy hh24:mi'))) as acquisition_month,
    count(distinct customer_id) as new_customers_count
  from tableretail
  group by extract(year from to_date(invoicedate,'mm/dd/yyyy hh24:mi')),
    extract(month from to_date(invoicedate,'mm/dd/yyyy hh24:mi'))
)
select acquisition_year,
  acquisition_month,
  new_customers_count,
  lag(new_customers_count) over (order by acquisition_year, acquisition_month) as previous_month_new_customers,
  round(((new_customers_count - lag(new_customers_count) 
  over (order by acquisition_year, acquisition_month)) / lag(new_customers_count) over (order by acquisition_year, acquisition_month)) * 100, 2) as acquisition_rate_percentage
from newcustomers
order by acquisition_year, acquisition_month;
