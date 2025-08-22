-- First transaction for each student.
with first_payments as (
    select user_id
         , min (transaction_datetime::date) as first_payment_date  
    from skyeng_db.payments p
    where status_name = 'success'
    group by user_id
    ), 
-- Unique dates without time for the year 2016.
all_dates as (
    select distinct class_start_datetime::date as dt 
    from skyeng_db.classes c
    where class_start_datetime >= '2016-01-01' and class_start_datetime <= '2016-12-31'
    ), 
-- All dates of a student’s activity after their first transaction occurred.
all_dates_by_user as (  
    select fp.user_id, ad.dt
    from first_payments fp 
    join all_dates ad 
     on ad.dt >= fp.first_payment_date
    ), 
-- All balance changes: how many lessons were credited or deducted.
payments_by_dates as ( 
    select user_id
         , transaction_datetime::date as payment_date
         , sum(classes) as transaction_balance_change 
    from skyeng_db.payments p
    where transaction_datetime >= '2016-01-01' and transaction_datetime <= '2016-12-31'
      and status_name = 'success' 
    group by user_id, payment_date
    ), 
-- Student balances formed only by transactions; replace empty values with zeros.
payments_by_dates_cumsum as (    
    select distinct a.user_id
         , a.dt 
         , p.transaction_balance_change
         , sum(coalesce(p.transaction_balance_change, 0)) over (partition by a.user_id order by a.dt) as transaction_balance_change_cs 
    from all_dates_by_user a
    left join payments_by_dates p
    on p.user_id = a.user_id
    and p.payment_date = a.dt
    ), 
-- Balance changes due to completed lessons. Multiply classes by -1 to reflect that “–” means deductions from the balance.
classes_by_dates as ( 
    select user_id
         , date_trunc('day', class_start_datetime) as class_date
         , count(*) * -1 as classes
    from skyeng_db.classes
    where class_status in ('success', 'failed_by_student')
    and class_type != 'trial'
    group by user_id, class_date
    ), 
-- CTE for storing the cumulative sum of completed lessons.
classes_by_dates_dates_cumsum as (
    select ad.user_id
         , ad.dt 
         , cd.classes
         , sum(coalesce(cd.classes, 0)) over (partition by ad.user_id order by ad.dt) as classes_cs 
    from all_dates_by_user ad
    left join classes_by_dates cd 
    on ad.user_id = cd.user_id
    and ad.dt = cd.class_date
    ), 
-- All student balances
balances as ( 
  select cdc.user_id
       , cdc.dt
       , pdc.transaction_balance_change
       , pdc.transaction_balance_change_cs
       , cdc.classes
       , cdc.classes_cs
       , cdc.classes_cs + pdc.transaction_balance_change_cs as balance 
    from payments_by_dates_cumsum pdc 
    join classes_by_dates_dates_cumsum cdc 
    on pdc.dt = cdc.dt
    and pdc.user_id = cdc.user_id
    )
    select dt
         , sum (transaction_balance_change) as transaction_balance_change
         , sum (transaction_balance_change_cs) as transaction_balance_change_cs 
         , sum (classes) as classes 
         , sum (classes_cs) as classes_cs 
         , sum (balance) as balance 
    from balances 
    group by dt 
    order by dt

