from pathlib import Path

from py_scripts.utils import (
    get_filepaths_by_prefix,
    extract_postgres_datetime_from_filename,
    get_sql_script,
    read_data_by_chunks,
    move_files_to_backup
)


def upload_transactions_to_db(
        file_prefix: str,
        data_directory_path: Path,
        sql_script_path: Path,
        cursor
):
    script: str = get_sql_script(path=sql_script_path)

    transactions_files: list[Path] = get_filepaths_by_prefix(
        prefix=file_prefix,
        directory_path=data_directory_path
    )

    for transaction_file in transactions_files:
        cursor.execute('delete from public.mnkh_stg_transactions;')
        for chunk in read_data_by_chunks(path=transaction_file):
            chunk['update_dt'] = extract_postgres_datetime_from_filename(path=transaction_file)
            chunk['amount'] = chunk['amount'].apply(lambda x: x.replace(',', '.'))
            cursor.executemany('''
            insert into public.mnkh_stg_transactions(
                trans_id,
                trans_date,
                amt,
                card_num,
                oper_type,
                oper_result,
                terminal,
                update_dt
            ) values( %s, %s, %s, %s, %s, %s, %s, %s )
            ''', chunk.values.tolist())
        cursor.execute(script)

    move_files_to_backup(transactions_files)
