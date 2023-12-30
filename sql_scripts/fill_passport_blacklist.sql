insert into public.mnkh_dwh_fact_passport_blacklist (
    passport_num,
    entry_dt
)
select
    stg.passport_num,
    stg.entry_dt
from public.mnkh_stg_passport_blacklist as stg
left join public.mnkh_dwh_fact_passport_blacklist as tgt
on stg.passport_num = tgt.passport_num
where
    tgt.passport_num is null and stg.update_dt = stg.entry_dt

