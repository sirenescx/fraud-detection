from pathlib import Path

import pandas as pd

from py_scripts.utils import (
    read_data,
    get_filepaths_by_prefix,
    extract_postgres_datetime_from_filename,
    get_sql_script,
    move_files_to_backup
)


def upload_passports_blacklist_to_db(
        file_prefix: str,
        data_directory_path: Path,
        sql_script_path: Path,
        cursor
):
    script: str = get_sql_script(path=sql_script_path)

    passport_blacklist_files: list[Path] = get_filepaths_by_prefix(
        prefix=file_prefix,
        directory_path=data_directory_path
    )

    for passport_blacklist_file in passport_blacklist_files:
        data: pd.DataFrame = read_data(path=passport_blacklist_file)
        data['update_dt'] = extract_postgres_datetime_from_filename(path=passport_blacklist_file)
        cursor.execute('delete from public.mnkh_stg_passport_blacklist;')
        cursor.executemany('''
        insert into public.mnkh_stg_passport_blacklist(
            entry_dt,
            passport_num,
            update_dt
        ) values( %s, %s, %s )
        ''', data.values.tolist())
        cursor.execute(script)

    move_files_to_backup(passport_blacklist_files)
