-- dwh dim
create table public.mnkh_dwh_dim_terminals_hist (
    terminal_id varchar(32),
    terminal_type varchar(32),
    terminal_city varchar(32),
    terminal_address varchar(256),
    effective_from timestamp(0),
    effective_to timestamp(0) default null,
    deleted_flg boolean default false
);

create table public.mnkh_dwh_dim_clients_hist (
    client_id varchar(10),
    last_name varchar(20),
    first_name varchar(20),
    patronymic varchar(20),
    date_of_birth timestamp(0),
    passport_num varchar(15),
    passport_valid_to timestamp(0),
    phone varchar(16),
    effective_from timestamp(0),
    effective_to timestamp(0) default null,
    deleted_flg boolean default false
);

create table public.mnkh_dwh_dim_accounts_hist (
    account_num varchar(20),
    valid_to timestamp(0),
    client varchar(10),
    effective_from timestamp(0),
    effective_to timestamp(0) default null,
    deleted_flg boolean default false
);

create table public.mnkh_dwh_dim_cards_hist (
    card_num varchar(20),
    account_num varchar(20),
    effective_from timestamp(0),
    effective_to timestamp(0) default null,
    deleted_flg boolean default false
);

-- dwh fact
create table public.mnkh_dwh_fact_transactions (
    trans_id varchar(128),
    trans_date timestamp(0),
    card_num varchar(20),
    oper_type varchar(64),
    amt decimal,
    oper_result varchar(256),
    terminal varchar(32)
);

create table public.mnkh_dwh_fact_passport_blacklist (
    passport_num varchar(15),
    entry_dt date
);

-- rep
create table public.mnkh_rep_fraud (
    event_dt timestamp(0),
    passport varchar(15),
    fio varchar(64),
    phone varchar(16),
    event_type numeric,
    report_dt date
);

-- stg
create table public.mnkh_stg_terminals (
    terminal_id varchar(32),
    terminal_type varchar(32),
    terminal_city varchar(32),
    terminal_address varchar(256),
    update_dt timestamp(0)
);

create table public.mnkh_stg_del_terminals (
    terminal_id varchar(32)
);

create table public.mnkh_stg_clients (
    client_id varchar(10),
    last_name varchar(20),
    first_name varchar(20),
    patronymic varchar(20),
    date_of_birth timestamp(0),
    passport_num varchar(15),
    passport_valid_to timestamp(0),
    phone varchar(16),
    update_dt timestamp(0)
);

create table public.mnkh_stg_del_clients (
    client_id varchar(10)
);

create table public.mnkh_stg_accounts (
    account_num varchar(20),
    valid_to timestamp(0),
    client varchar(10),
    update_dt timestamp(0)
);

create table public.mnkh_stg_del_accounts (
    account_num varchar(20)
);

create table public.mnkh_stg_cards (
    card_num varchar(20),
    account_num varchar(20),
    update_dt timestamp(0)
);

create table public.mnkh_stg_del_cards (
    card_num varchar(20) ,
    account_num varchar(20),
    update_dt timestamp(0)
);

create table public.mnkh_stg_passport_blacklist (
    passport_num varchar(15),
    entry_dt timestamp(0),
    update_dt timestamp(0)
);

create table public.mnkh_stg_transactions (
    trans_id varchar(128),
    trans_date timestamp(0),
    card_num varchar(20),
    oper_type varchar(64),
    amt decimal,
    oper_result varchar(256),
    terminal varchar(32),
    update_dt timestamp(0)
);

-- meta
create table public.mnkh_meta (
    schema_name varchar(32),
    table_name varchar(64),
    max_update_dt timestamp(0)
);

insert into public.mnkh_meta ( schema_name, table_name, max_update_dt )
values
    ('info', 'accounts', to_timestamp('1800-01-01', 'YYYY-MM-DD')),
    ('info', 'cards', to_timestamp('1800-01-01', 'YYYY-MM-DD')),
    ('info', 'clients', to_timestamp('1800-01-01', 'YYYY-MM-DD'))
