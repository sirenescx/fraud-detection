insert into public.mnkh_dwh_dim_terminals_hist (
    terminal_id,
    terminal_type,
    terminal_city,
    terminal_address,
    effective_from
)
select
    stg.terminal_id,
    stg.terminal_type,
    stg.terminal_city,
    stg.terminal_address,
    stg.update_dt
from public.mnkh_stg_terminals as stg
left join public.mnkh_dwh_dim_terminals_hist as tgt
on stg.terminal_id = tgt.terminal_id
where tgt.terminal_id is null;

update public.mnkh_dwh_dim_terminals_hist
set effective_to = tmp.update_dt - interval '1 seconds'
from (
	select
        stg.terminal_id,
        stg.terminal_type,
        stg.terminal_city,
        stg.terminal_address,
        stg.update_dt
	from public.mnkh_stg_terminals as stg
	inner join public.mnkh_dwh_dim_terminals_hist as tgt
	on stg.terminal_id = tgt.terminal_id
	where
        stg.terminal_type <> tgt.terminal_type
        or (stg.terminal_type is null and tgt.terminal_type is not null)
        or (stg.terminal_type is not null and tgt.terminal_type is null)
        or stg.terminal_city <> tgt.terminal_city
        or (stg.terminal_city is null and tgt.terminal_city is not null)
        or (stg.terminal_city is not null and tgt.terminal_city is null)
        or stg.terminal_address <> tgt.terminal_address
        or (stg.terminal_address is null and tgt.terminal_address is not null)
        or (stg.terminal_address is not null and tgt.terminal_address is null)
) as tmp
where
    public.mnkh_dwh_dim_terminals_hist.terminal_id = tmp.terminal_id
    and public.mnkh_dwh_dim_terminals_hist.effective_to is null;

insert into public.mnkh_dwh_dim_terminals_hist (
    terminal_id,
    terminal_type,
    terminal_city,
    terminal_address,
    effective_from
)
select
    stg.terminal_id,
    stg.terminal_type,
    stg.terminal_city,
    stg.terminal_address,
    stg.update_dt
from public.mnkh_stg_terminals as stg
inner join public.mnkh_dwh_dim_terminals_hist as tgt
on stg.terminal_id = tgt.terminal_id
where
    tgt.effective_to = stg.update_dt - interval '1 seconds'
    and (
        stg.terminal_type <> tgt.terminal_type
        or (stg.terminal_type is null and tgt.terminal_type is not null)
        or (stg.terminal_type is not null and tgt.terminal_type is null)
        or stg.terminal_city <> tgt.terminal_city
        or (stg.terminal_city is null and tgt.terminal_city is not null)
        or (stg.terminal_city is not null and tgt.terminal_city is null)
        or stg.terminal_address <> tgt.terminal_address
        or (stg.terminal_address is null and tgt.terminal_address is not null)
        or (stg.terminal_address is not null and tgt.terminal_address is null)
    );

insert into public.mnkh_dwh_dim_terminals_hist (
    terminal_id,
    terminal_type,
    terminal_city,
    terminal_address,
    effective_from,
    deleted_flg
)
select
    tgt.terminal_id,
    tgt.terminal_type,
    tgt.terminal_city,
    tgt.terminal_address,
    (select max(update_dt) from public.mnkh_stg_terminals) AS effective_from,
    true
from public.mnkh_dwh_dim_terminals_hist as tgt
left join public.mnkh_stg_del_terminals as stg
on tgt.terminal_id = stg.terminal_id
where
    stg.terminal_id is null and tgt.effective_to is null and not tgt.deleted_flg;

update public.mnkh_dwh_dim_terminals_hist
set
	effective_to = (select max(update_dt) from public.mnkh_stg_terminals) - interval '1 seconds'
where
	effective_to is null
	and not deleted_flg
	and terminal_id in (
			select tgt.terminal_id
			from public.mnkh_dwh_dim_terminals_hist as tgt
			left join public.mnkh_stg_del_terminals as stg
			on tgt.terminal_id = stg.terminal_id
			where stg.terminal_id is null
	    );

commit;
