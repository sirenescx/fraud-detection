from datetime import datetime
from pathlib import Path

import psycopg2 as psycopg2
import pandas as pd


def get_db_connection(
        database: str,
        host: str,
        user: str,
        password: str,
        port: int
):
    conn = psycopg2.connect(
        database=database,
        host=host,
        user=user,
        password=password,
        port=port
    )
    conn.autocommit = False
    return conn


# сортируем по возрастанию даты
def get_filepaths_by_prefix(
        prefix: str,
        directory_path: Path
):
    return list(sorted(directory_path.glob(f'{prefix}*')))


def check_path(path: Path):
    if not path.exists():
        raise ValueError(f'File {str(path)} does not exist')
    if not path.is_file():
        raise ValueError(f'{str(path)} is not a file')


def read_data(path: Path):
    check_path(path)
    try:
        return pd.read_excel(path, header=0, index_col=None)
    except Exception as exception:
        raise ValueError(
            f'Failed to load file, cause: {exception}'
        ) from exception


def read_data_by_chunks(path: Path, chunk_size: int = 10**3):
    check_path(path)
    try:
        with pd.read_csv(path, chunksize=chunk_size, sep=';') as reader:
            for chunk in reader:
                yield chunk
    except Exception as exception:
        raise ValueError(
            f'Failed to load file, cause: {exception}'
        ) from exception


def extract_postgres_datetime_from_filename(path: Path):
    date_string: str = path.stem.split('_')[-1]
    return datetime.strptime(date_string, "%d%m%Y").strftime("%Y-%m-%d")


def get_sql_script(path: Path) -> str:
    with open(path, 'r') as _in:
        return _in.read()


# самый старый из трех файлов перемещаем в архив
def move_files_to_backup(paths: list[Path]):
    earliest_created_filepath: Path = paths[0]
    filename: str = earliest_created_filepath.name
    parent_directory: Path = earliest_created_filepath.absolute().parent.parent
    new_path: Path = parent_directory / 'archive' / filename
    new_path = new_path.with_suffix(new_path.suffix + '.backup')
    earliest_created_filepath.rename(new_path)
