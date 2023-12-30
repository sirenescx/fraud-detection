insert into public.mnkh_rep_fraud
select
    transactions.trans_date as event_dt,
    clients_accounts_cards.passport_num as passport,
    clients_accounts_cards.fio as fio,
    clients_accounts_cards.phone as phone,
    1 as event_type,
    date(transactions.trans_date) as report_dt
from public.mnkh_dwh_fact_transactions as transactions
join (
    select
        cards.card_num as card_num,
        cards.account_num as account_num,
        clients_accounts.client_id as client_id,
        clients_accounts.fio as fio,
        clients_accounts.passport_num as passport_num,
        clients_accounts.passport_valid_to as passport_valid_to,
        clients_accounts.phone as phone,
        clients_accounts.passport_blacklisted as passport_blacklisted,
        clients_accounts.passport_blacklisted_date as passport_blacklisted_date
        from (
            select
                accounts.account_num as account_num,
                clients.client_id as client_id,
                clients.fio as fio,
                clients.passport_num as passport_num,
                clients.passport_valid_to as passport_valid_to,
                clients.phone as phone,
                clients.passport_blacklisted as passport_blacklisted,
                clients.passport_blacklisted_date as passport_blacklisted_date
            from public.mnkh_dwh_dim_accounts_hist as accounts
            join (
                select
                    clients.client_id as client_id,
                    clients.last_name || ' ' || clients.first_name || ' ' || clients.patronymic as fio,
                    clients.passport_num as passport_num,
                    clients.passport_valid_to as passport_valid_to,
                    clients.phone as phone,
                    blacklisted_passports.passport_num is not null as passport_blacklisted,
                    blacklisted_passports.entry_dt as passport_blacklisted_date
                from public.mnkh_dwh_dim_clients_hist as clients
                left join public.mnkh_dwh_fact_passport_blacklist as blacklisted_passports
                on clients.passport_num = blacklisted_passports.passport_num
            ) as clients
            on accounts.client = clients.client_id
        ) as clients_accounts
        join public.mnkh_dwh_dim_cards_hist as cards
        on cards.account_num = clients_accounts.account_num
    ) as clients_accounts_cards
on transactions.card_num = clients_accounts_cards.card_num
where
    (passport_blacklisted and transactions.trans_date > passport_blacklisted_date)
    or transactions.trans_date > clients_accounts_cards.passport_valid_to;


