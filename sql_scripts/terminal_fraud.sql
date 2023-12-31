with transactions as (
    select
        terminals.terminal_city as terminal_city,
        clients_transactions.client_id as client_id,
        clients_transactions.fio as fio,
        clients_transactions.passport_num as passport_num,
        clients_transactions.phone as phone,
        clients_transactions.trans_date as trans_date
    from public.mnkh_dwh_dim_terminals_hist as terminals
    join (
        select
            clients_accounts_cards.client_id as client_id,
            clients_accounts_cards.fio as fio,
            clients_accounts_cards.passport_num as passport_num,
            clients_accounts_cards.phone as phone,
            transactions.terminal as terminal,
            transactions.trans_date as trans_date
        from public.mnkh_dwh_fact_transactions as transactions
        join (
            select
                cards.card_num as card_num,
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
    ) as clients_transactions
    on terminals.terminal_id = clients_transactions.terminal
)
insert into public.mnkh_rep_fraud
select
    trans_date as event_dt,
    passport_num as passport,
    fio as fio,
    phone as phone,
    3 as event_type,
    date(trans_date) as report_dt
from (
    select
        passport_num,
        fio,
        phone,
        trans_date,
        terminal_city,
        lag(trans_date) over (partition by client_id order by trans_date) as prev_trans_date,
        lag(terminal_city) over (partition by client_id order by trans_date) as prev_terminal_city
    from transactions
) as transactions_chain
where
    terminal_city != prev_terminal_city
    and trans_date - prev_trans_date <= interval '1 hour'
