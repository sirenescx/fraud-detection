from pathlib import Path

from py_scripts.utils import get_sql_script


def detect_frauds(
        passport_fraud_sql_script_path: Path,
        account_fraud_sql_script_path: Path,
        terminal_fraud_sql_script_path: Path,
        cursor
):
    passport_fraud_sql_script: str = get_sql_script(path=passport_fraud_sql_script_path)
    cursor.execute(passport_fraud_sql_script)

    account_fraud_sql_script: str = get_sql_script(path=account_fraud_sql_script_path)
    cursor.execute(account_fraud_sql_script)

    terminal_fraud_sql_script: str = get_sql_script(path=terminal_fraud_sql_script_path)
    cursor.execute(terminal_fraud_sql_script)
