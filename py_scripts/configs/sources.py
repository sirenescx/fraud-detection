from dataclasses import dataclass


@dataclass
class FileSourceConfig:
    file_prefix: str


@dataclass
class DbSourceConfig:
    db_schema: str
    db_table_name: str


@dataclass
class SourcesConfig:
    passport_blacklist: FileSourceConfig
    terminals: FileSourceConfig
    transactions: FileSourceConfig
    accounts: DbSourceConfig
    cards: DbSourceConfig
    clients: DbSourceConfig
