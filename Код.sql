-- Первая транзакции для каждого студента.
with first_payments as (
    select user_id
         , min (transaction_datetime::date) as first_payment_date  
    from skyeng_db.payments p
    where status_name = 'success'
    group by user_id
    ), 
-- Уникальные даты без времени за 16 год.
all_dates as (
    select distinct class_start_datetime::date as dt 
    from skyeng_db.classes c
    where class_start_datetime >= '2016-01-01' and class_start_datetime <= '2016-12-31'
    ), 
-- Все даты жизни студента после того, как произошла его первая транзакция.
all_dates_by_user as (  
    select fp.user_id, ad.dt
    from first_payments fp 
    join all_dates ad 
     on ad.dt >= fp.first_payment_date
    ), 
-- Все изменения балансов, сколько уроков начисленно или списано.
payments_by_dates as ( 
    select user_id
         , transaction_datetime::date as payment_date
         , sum(classes) as transaction_balance_change 
    from skyeng_db.payments p
    where transaction_datetime >= '2016-01-01' and transaction_datetime <= '2016-12-31'
      and status_name = 'success' 
    group by user_id, payment_date
    ), 
-- Баланс студентов, который сформирован только транзакциями, пустые значения заменим на нули.
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
-- Изменения балансов из-за прохождения уроков. classes умножим на -1, чтобы отразить, что - — это списания с баланса.
classes_by_dates as ( 
    select user_id
         , date_trunc('day', class_start_datetime) as class_date
         , count(*) * -1 as classes
    from skyeng_db.classes
    where class_status in ('success', 'failed_by_student')
    and class_type != 'trial'
    group by user_id, class_date
    ), 
-- СТЕ для хранения кумулятивной суммы количества пройденных уроков.
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
-- Все балансы студентов
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
