insert into public.mnkh_dwh_fact_transactions (
    trans_id,
    trans_date,
    amt,
    card_num,
    oper_type,
    oper_result,
    terminal
)
select
    stg.trans_id,
    stg.trans_date,
    stg.amt,
    stg.card_num,
    stg.oper_type,
    stg.oper_result,
    stg.terminal
from public.mnkh_stg_transactions as stg;
