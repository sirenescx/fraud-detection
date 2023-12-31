from pathlib import Path

import pandas as pd

from py_scripts.utils import (
    read_data,
    get_filepaths_by_prefix,
    extract_postgres_datetime_from_filename,
    get_sql_script,
    move_files_to_backup
)


def upload_terminals_to_db(
        file_prefix: str,
        data_directory_path: Path,
        sql_script_path: Path,
        cursor
):
    script: str = get_sql_script(path=sql_script_path)

    terminals_files: list[Path] = get_filepaths_by_prefix(
        prefix=file_prefix,
        directory_path=data_directory_path
    )

    for terminals_file in terminals_files:
        data: pd.DataFrame = read_data(path=terminals_file)
        data['update_dt'] = extract_postgres_datetime_from_filename(path=terminals_file)
        cursor.execute('delete from public.mnkh_stg_terminals;')
        cursor.executemany('''
        insert into public.mnkh_stg_terminals(
            terminal_id,
            terminal_type,
            terminal_city,
            terminal_address,
            update_dt
        ) values( %s, %s, %s, %s, %s )
        ''', data.values.tolist())
        cursor.execute('delete from public.mnkh_stg_del_terminals;')
        cursor.executemany('''
        insert into public.mnkh_stg_del_terminals(
            terminal_id
        ) values( %s )
        ''', pd.DataFrame(data.terminal_id).values.tolist())
        cursor.execute(script)

    move_files_to_backup(terminals_files)
