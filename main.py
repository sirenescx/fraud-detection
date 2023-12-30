from pathlib import Path

import hydra
from hydra.core.config_store import ConfigStore

from py_scripts.configs.config import Config
from py_scripts.detect_frauds import detect_frauds
from py_scripts.fill_table import fill_scd2_table
from py_scripts.upload_passport_blacklist import upload_passports_blacklist_to_db
from py_scripts.upload_terminals import upload_terminals_to_db
from py_scripts.upload_transactions import upload_transactions_to_db
from py_scripts.utils import get_db_connection

CONFIG_STORE = ConfigStore.instance()
CONFIG_STORE.store(name='base_config', node=Config)


@hydra.main(version_base='1.2', config_path='configs', config_name='config')
def main(config: Config) -> None:
    data_directory: Path = Path(__file__).resolve().parent / 'data'
    sql_scripts_directory: Path = Path(__file__).resolve().parent / 'sql_scripts'

    connection = get_db_connection(
        host=config.db.host,
        port=config.db.port,
        database=config.db.database,
        user=config.db.user,
        password=config.db.password
    )
    cursor = connection.cursor()

    fill_scd2_table(
        db_schema=config.sources.clients.db_schema,
        db_table_name=config.sources.clients.db_table_name,
        sql_script_path=sql_scripts_directory / 'clients_scd2.sql',
        cursor=cursor
    )
    fill_scd2_table(
        db_schema=config.sources.accounts.db_schema,
        db_table_name=config.sources.accounts.db_table_name,
        sql_script_path=sql_scripts_directory / 'accounts_scd2.sql',
        cursor=cursor
    )
    fill_scd2_table(
        db_schema=config.sources.cards.db_schema,
        db_table_name=config.sources.cards.db_table_name,
        sql_script_path=sql_scripts_directory / 'cards_scd2.sql',
        cursor=cursor
    )
    upload_terminals_to_db(
        file_prefix=config.sources.terminals.file_prefix,
        data_directory_path=data_directory,
        sql_script_path=sql_scripts_directory / 'terminals_scd2.sql',
        cursor=cursor
    )
    upload_passports_blacklist_to_db(
        file_prefix=config.sources.passport_blacklist.file_prefix,
        data_directory_path=data_directory,
        sql_script_path=sql_scripts_directory / 'fill_passport_blacklist.sql',
        cursor=cursor
    )
    upload_transactions_to_db(
        file_prefix=config.sources.transactions.file_prefix,
        data_directory_path=data_directory,
        sql_script_path=sql_scripts_directory / 'fill_transactions.sql',
        cursor=cursor
    )
    detect_frauds(
        passport_fraud_sql_script_path=sql_scripts_directory / 'passport_fraud.sql',
        account_fraud_sql_script_path=sql_scripts_directory / 'account_fraud.sql',
        cursor=cursor
    )

    connection.commit()
    connection.close()


if __name__ == '__main__':
    main()
