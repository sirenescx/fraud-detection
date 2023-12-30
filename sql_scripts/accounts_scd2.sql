delete from public.mnkh_stg_accounts;
delete from public.mnkh_stg_del_accounts;

insert into public.mnkh_stg_accounts ( account_num, valid_to, client, update_dt )
select
    account,
    valid_to,
    client,
    coalesce(update_dt, create_dt)
from $schema.$table_name
where
    coalesce(update_dt, create_dt) > (select max_update_dt from public.mnkh_meta where schema_name = '$schema' and table_name = '$table_name');

insert into public.mnkh_stg_del_accounts ( account_num )
select account from $schema.$table_name;

insert into public.mnkh_dwh_dim_accounts_hist ( account_num, valid_to, client, effective_from )
select
    stg.account_num,
    stg.valid_to,
    stg.client,
    stg.update_dt
from public.mnkh_stg_accounts as stg
left join public.mnkh_dwh_dim_accounts_hist as tgt
on stg.account_num = tgt.account_num
where tgt.account_num is null;

update public.mnkh_dwh_dim_accounts_hist
set effective_to = tmp.update_dt - interval '1 seconds'
from (
	select
        stg.account_num,
        stg.valid_to,
        stg.client,
        stg.update_dt
	from public.mnkh_stg_accounts as stg
	inner join public.mnkh_dwh_dim_accounts_hist as tgt
	on stg.account_num = tgt.account_num
	where
	    -- не представляю, что айдишник клиента может поменяться, поэтому только valid_to
        stg.valid_to <> tgt.valid_to
        or (stg.valid_to is null and tgt.valid_to is not null)
        or (stg.valid_to is not null and tgt.valid_to is null)
) as tmp
where public.mnkh_dwh_dim_accounts_hist.account_num = tmp.account_num;

insert into public.mnkh_dwh_dim_accounts_hist ( account_num, valid_to, client, effective_from )
select
    stg.account_num,
    stg.valid_to,
    stg.client,
    stg.update_dt
from public.mnkh_stg_accounts as stg
inner join public.mnkh_dwh_dim_accounts_hist as tgt
on stg.account_num = tgt.account_num
where
    stg.valid_to <> tgt.valid_to
    or (stg.valid_to is null and tgt.valid_to is not null)
    or (stg.valid_to is not null and tgt.valid_to is null);

insert into public.mnkh_dwh_dim_accounts_hist ( account_num, valid_to, client, effective_from, deleted_flg )
select
    tgt.account_num,
    tgt.valid_to,
    tgt.client,
    now(),
    true
from public.mnkh_dwh_dim_accounts_hist as tgt
left join public.mnkh_stg_del_accounts as stg
on tgt.account_num = stg.account_num
where
    stg.account_num is null and tgt.effective_to is null and not tgt.deleted_flg;

update public.mnkh_dwh_dim_accounts_hist
set
	effective_to = now() - interval '1 seconds'
where
	effective_to is null
	and not deleted_flg
	and account_num in (
			select tgt.account_num
			from public.mnkh_dwh_dim_accounts_hist as tgt
			left join public.mnkh_stg_del_accounts as stg
			on tgt.account_num = stg.account_num
			where stg.account_num is null
	    );

update public.mnkh_meta
set max_update_dt = coalesce(
    (select max(update_dt) from public.mnkh_stg_accounts),
    (select max_update_dt from public.mnkh_meta where schema_name = '$schema' and table_name = '$table_name')
)
where schema_name = '$schema' and table_name = '$table_name';

commit;
