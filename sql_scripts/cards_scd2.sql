delete from public.mnkh_stg_cards;
delete from public.mnkh_stg_del_cards;

insert into public.mnkh_stg_cards ( card_num, account_num, update_dt )
select
    card_num,
    account,
    coalesce(update_dt, create_dt)
from $schema.$table_name
where
    coalesce(update_dt, create_dt) > (select max_update_dt from public.mnkh_meta where schema_name = '$schema' and table_name = '$table_name');

insert into public.mnkh_stg_del_cards ( card_num )
select card_num from $schema.$table_name;

insert into public.mnkh_dwh_dim_cards_hist ( card_num, account_num, effective_from )
select
    stg.card_num,
    stg.account_num,
    stg.update_dt
from public.mnkh_stg_cards as stg
left join public.mnkh_dwh_dim_cards_hist as tgt
on stg.card_num = tgt.card_num
where tgt.card_num is null;

update public.mnkh_dwh_dim_cards_hist
set effective_to = tmp.update_dt - interval '1 seconds'
from (
	select
        stg.card_num,
        stg.account_num,
        stg.update_dt
	from public.mnkh_stg_cards as stg
	inner join public.mnkh_dwh_dim_cards_hist as tgt
	on stg.card_num = tgt.card_num
	where
        stg.account_num <> tgt.account_num
        or (stg.account_num is null and tgt.account_num is not null)
        or (stg.account_num is not null and tgt.account_num is null)
) as tmp
where public.mnkh_dwh_dim_cards_hist.card_num = tmp.card_num;

insert into public.mnkh_dwh_dim_cards_hist ( card_num, account_num, effective_from )
select
    stg.card_num,
    stg.account_num,
    stg.update_dt
from public.mnkh_stg_cards as stg
inner join public.mnkh_dwh_dim_cards_hist as tgt
on stg.card_num = tgt.card_num
where
    stg.account_num <> tgt.account_num
    or (stg.account_num is null and tgt.account_num is not null)
    or (stg.account_num is not null and tgt.account_num is null);

insert into public.mnkh_dwh_dim_cards_hist ( card_num, account_num, effective_from, deleted_flg )
select
    stg.card_num,
    stg.account_num,
    now(),
    true
from public.mnkh_dwh_dim_cards_hist as tgt
left join public.mnkh_stg_del_cards as stg
on tgt.card_num = stg.card_num
where
    stg.card_num is null and tgt.effective_to is null and not tgt.deleted_flg;

update public.mnkh_dwh_dim_cards_hist
set
	effective_to = now() - interval '1 seconds'
where
	effective_to is null
	and not deleted_flg
	and card_num in (
			select tgt.card_num
			from public.mnkh_dwh_dim_cards_hist as tgt
			left join public.mnkh_stg_del_cards as stg
			on tgt.card_num = stg.card_num
			where stg.card_num is null
	    );

update public.mnkh_meta
set max_update_dt = coalesce(
    (select max(update_dt) from public.mnkh_stg_cards),
    (select max_update_dt from public.mnkh_meta where schema_name = '$schema' and table_name = '$table_name')
)
where schema_name = '$schema' and table_name = '$table_name';

commit;
