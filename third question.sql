--third question a- 
with purchase_days as (
  select cust_id,
    calendar_dt as purchase_date,
    row_number() over (partition by cust_id order by calendar_dt) as purchase_rank
  from daily_sales
)
select cust_id,
  max(count_days) as max_days
from (
  select cust_id,
    count(*) as count_days
  from purchase_days
  group by  cust_id, purchase_date - purchase_rank
)
group by cust_id
order by cust_id;

----------------- 
--third question b

with CustomerTotalSpent as (
    select  CUST_ID,
        CALENDAR_DT as InvoiceDate,
        sum(AMT_LE) over (partition by CUST_ID order by CALENDAR_DT rows between unbounded preceding and current row) AS TotalSpent
    from  daily_sales
),
RankedCustomers as (
    select  CUST_ID,InvoiceDate,TotalSpent,
        row_number() over (partition by CUST_ID order by TotalSpent) AS Row_num
   from
        CustomerTotalSpent
)
select distinct CUST_ID,
    min(Row_num),trunc(avg( min(Row_num)) over())
from RankedCustomers
where TotalSpent >= 250
    group by cust_id 
    order by cust_id 
