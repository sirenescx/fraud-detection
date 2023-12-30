insert into public.mnkh_rep_fraud
select
    transactions.trans_date as event_dt,
    clients_accounts_cards.passport_num as passport,
    clients_accounts_cards.fio as fio,
    clients_accounts_cards.phone as phone,
    2 as event_type,
    date(transactions.trans_date) as report_dt
from public.mnkh_dwh_fact_transactions as transactions
join (
    select
        cards.card_num as card_num,
        cards.account_num as account_num,
        clients_accounts.account_valid_to as account_valid_to,
        clients_accounts.client_id as client_id,
        clients_accounts.fio as fio,
        clients_accounts.passport_num as passport_num,
        clients_accounts.phone as phone
        from (
            select
                accounts.account_num as account_num,
                accounts.valid_to as account_valid_to,
                clients.client_id as client_id,
                clients.last_name || ' ' || clients.first_name || ' ' || clients.patronymic as fio,
                clients.passport_num as passport_num,
                clients.phone as phone
            from public.mnkh_dwh_dim_accounts_hist as accounts
            join public.mnkh_dwh_dim_clients_hist as clients
            on accounts.client = clients.client_id
        ) as clients_accounts
        join public.mnkh_dwh_dim_cards_hist as cards
        on cards.account_num = clients_accounts.account_num
    ) as clients_accounts_cards
on transactions.card_num = clients_accounts_cards.card_num
where transactions.trans_date > account_valid_to;
