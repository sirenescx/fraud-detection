from pathlib import Path

from py_scripts.utils import get_sql_script


def fill_scd2_table(
        db_schema: str,
        db_table_name: str,
        sql_script_path: Path,
        cursor
):
    script: str = get_sql_script(path=sql_script_path)
    script = script.replace('$schema', db_schema).replace('$table_name', db_table_name)
    cursor.execute(script)
