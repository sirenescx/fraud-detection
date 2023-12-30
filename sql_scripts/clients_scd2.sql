delete from public.mnkh_stg_clients;
delete from public.mnkh_stg_del_clients;

insert into public.mnkh_stg_clients (
    client_id,
    last_name,
    first_name,
    patronymic,
    date_of_birth,
    passport_num,
    passport_valid_to,
    phone,
    update_dt
)
select
    client_id,
    last_name,
    first_name,
    patronymic,
    date_of_birth,
    passport_num,
    passport_valid_to,
    phone,
    coalesce(update_dt, create_dt)
from $schema.$table_name
where
    coalesce(update_dt, create_dt) > (select max_update_dt from public.mnkh_meta where schema_name = '$schema' and table_name = '$table_name');

insert into public.mnkh_stg_del_clients ( client_id )
select client_id from $schema.$table_name;

insert into public.mnkh_dwh_dim_clients_hist (
    client_id,
    last_name,
    first_name,
    patronymic,
    date_of_birth,
    passport_num,
    passport_valid_to,
    phone,
    effective_from
)
select
    stg.client_id,
    stg.last_name,
    stg.first_name,
    stg.patronymic,
    stg.date_of_birth,
    stg.passport_num,
    stg.passport_valid_to,
    stg.phone,
    stg.update_dt
from public.mnkh_stg_clients as stg
left join public.mnkh_dwh_dim_clients_hist as tgt
on stg.client_id = tgt.client_id
where tgt.client_id is null;

update public.mnkh_dwh_dim_clients_hist
set effective_to = tmp.update_dt - interval '1 seconds'
from (
	select
        stg.client_id,
        stg.last_name,
        stg.first_name,
        stg.patronymic,
        stg.date_of_birth,
        stg.passport_num,
        stg.passport_valid_to,
        stg.phone,
        stg.update_dt
	from public.mnkh_stg_clients as stg
	inner join public.mnkh_dwh_dim_clients_hist as tgt
	on stg.client_id = tgt.client_id
	where
        stg.last_name <> tgt.last_name
        or (stg.last_name is null and tgt.last_name is not null)
        or (stg.last_name is not null and tgt.last_name is null)
	    or stg.first_name <> tgt.first_name
        or (stg.first_name is null and tgt.first_name is not null)
        or (stg.first_name is not null and tgt.first_name is null)
	    or stg.patronymic <> tgt.patronymic
        or (stg.patronymic is null and tgt.patronymic is not null)
        or (stg.patronymic is not null and tgt.patronymic is null)
	    -- не знаю надо ли дату рождения, но вдруг в паспортном столе все перепутали...
	    or stg.date_of_birth <> tgt.date_of_birth
        or (stg.date_of_birth is null and tgt.date_of_birth is not null)
        or (stg.date_of_birth is not null and tgt.date_of_birth is null)
	    or stg.passport_num <> tgt.passport_num
        or (stg.passport_num is null and tgt.passport_num is not null)
        or (stg.passport_num is not null and tgt.passport_num is null)
	    or stg.passport_valid_to <> tgt.passport_valid_to
        or (stg.passport_valid_to is null and tgt.passport_valid_to is not null)
        or (stg.passport_valid_to is not null and tgt.passport_valid_to is null)
	    or stg.phone <> tgt.phone
        or (stg.phone is null and tgt.phone is not null)
        or (stg.phone is not null and tgt.phone is null)
) as tmp
where public.mnkh_dwh_dim_clients_hist.client_id = tmp.client_id;

insert into public.mnkh_dwh_dim_clients_hist (
    client_id,
    last_name,
    first_name,
    patronymic,
    date_of_birth,
    passport_num,
    passport_valid_to,
    phone,
    effective_from
)
select
    stg.client_id,
    stg.last_name,
    stg.first_name,
    stg.patronymic,
    stg.date_of_birth,
    stg.passport_num,
    stg.passport_valid_to,
    stg.phone,
    stg.update_dt
from public.mnkh_stg_clients as stg
inner join public.mnkh_dwh_dim_clients_hist as tgt
on stg.client_id = tgt.client_id
where
    stg.last_name <> tgt.last_name
    or (stg.last_name is null and tgt.last_name is not null)
    or (stg.last_name is not null and tgt.last_name is null)
    or stg.first_name <> tgt.first_name
    or (stg.first_name is null and tgt.first_name is not null)
    or (stg.first_name is not null and tgt.first_name is null)
    or stg.patronymic <> tgt.patronymic
    or (stg.patronymic is null and tgt.patronymic is not null)
    or (stg.patronymic is not null and tgt.patronymic is null)
    -- не знаю надо ли дату рождения, но вдруг в паспортном столе все перепутали...
    or stg.date_of_birth <> tgt.date_of_birth
    or (stg.date_of_birth is null and tgt.date_of_birth is not null)
    or (stg.date_of_birth is not null and tgt.date_of_birth is null)
    or stg.passport_num <> tgt.passport_num
    or (stg.passport_num is null and tgt.passport_num is not null)
    or (stg.passport_num is not null and tgt.passport_num is null)
    or stg.passport_valid_to <> tgt.passport_valid_to
    or (stg.passport_valid_to is null and tgt.passport_valid_to is not null)
    or (stg.passport_valid_to is not null and tgt.passport_valid_to is null)
    or stg.phone <> tgt.phone
    or (stg.phone is null and tgt.phone is not null)
    or (stg.phone is not null and tgt.phone is null);

insert into public.mnkh_dwh_dim_clients_hist (
    client_id,
    last_name,
    first_name,
    patronymic,
    date_of_birth,
    passport_num,
    passport_valid_to,
    phone,
    effective_from,
    deleted_flg
)
select
    tgt.client_id,
    tgt.last_name,
    tgt.first_name,
    tgt.patronymic,
    tgt.date_of_birth,
    tgt.passport_num,
    tgt.passport_valid_to,
    tgt.phone,
    now(),
    true
from public.mnkh_dwh_dim_clients_hist as tgt
left join public.mnkh_stg_del_clients as stg
on tgt.client_id = stg.client_id
where
    stg.client_id is null and tgt.effective_to is null and not tgt.deleted_flg;

update public.mnkh_dwh_dim_clients_hist
set
	effective_to = now() - interval '1 seconds'
where
	effective_to is null
	and not deleted_flg
	and client_id in (
			select tgt.client_id
			from public.mnkh_dwh_dim_clients_hist as tgt
			left join public.mnkh_stg_del_clients as stg
			on tgt.client_id = stg.client_id
			where stg.client_id is null
	    );

update public.mnkh_meta
set max_update_dt = coalesce(
    (select max(update_dt) from public.mnkh_stg_clients),
    (select max_update_dt from public.mnkh_meta where schema_name = '$schema' and table_name = '$table_name')
)
where schema_name = '$schema' and table_name = '$table_name';

commit;