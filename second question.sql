--second question
with customer_summary as (
  select distinct customer_id,
    count(distinct invoice) over (partition by customer_id) as freq,
    round(sum(quantity * price) over (partition by customer_id) / 1000, 2) as monetary,
    last_value(to_date(invoicedate,'mm/dd/yyyy hh24:mi'))
     over (order by to_date(invoicedate,'mm/dd/yyyy hh24:mi') range between unbounded preceding and unbounded following) as last_val,
    last_value(to_date(invoicedate,'mm/dd/yyyy hh24:mi')) 
    over (partition by customer_id order by to_date(invoicedate,'mm/dd/yyyy hh24:mi') range between unbounded preceding and unbounded following) as first_val
  from tableretail
),
score_calculation as (
  select distinct(customer_id), freq, monetary, last_val, first_val,
    (monetary + freq) / 2 as average,
    ntile(5) over (order by freq desc) as freq_ntile,
    ntile(5) over (order by monetary desc) as monetary_ntile,
    ntile(5) over (order by (last_val - first_val) desc) as r_score
  from customer_summary
),
score_with_fm as (
  select distinct(customer_id), freq, monetary, last_val, first_val, freq_ntile, monetary_ntile, r_score,
    ntile(5) over(order by average) as fm_score
  from
    score_calculation
)
select customer_id,freq,monetary,
  round((last_val - first_val), 2) as recency,
  fm_score, r_score,
  (
    case
      when r_score = 5 and fm_score in (5, 4) then 'champions'
      when r_score = 4 and fm_score = 5 then 'champions'
      when r_score = 5 and fm_score = 2 then 'potential loyalists'
      when r_score = 4 and fm_score in (2 , 3) then 'potential loyalists'
      when r_score = 3 and fm_score = 3 then 'potential loyalists'
      when r_score = 5 and fm_score = 3 then 'loyal customers'
      when r_score = 4 and fm_score = 4 then 'loyal customers'
      when r_score = 3 and fm_score in (4 , 5) then 'loyal customers'
      when r_score = 5 and fm_score = 1 then 'recent customers'
      when r_score = 4 and fm_score = 1 then 'promising'
      when r_score = 3 and fm_score = 1 then 'promising'
      when r_score = 3 and fm_score = 2 then 'customers needing attention'
      when r_score = 2 and fm_score in (2, 3) then 'customers needing attention'
      when r_score = 1 and fm_score = 3 then 'at risk'
      when r_score = 2 and fm_score in (4, 5) then 'at risk'
      when r_score = 1 and fm_score = 2 then 'hibernating'
      when r_score = 1 and fm_score in (4, 5) then 'cant lose them'
      when r_score = 1 and fm_score = 1 then 'lost'
      else 'undefined'
    end
  ) 
from
  score_with_fm
order by
  customer_id;
---

