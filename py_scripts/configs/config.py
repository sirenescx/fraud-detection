from dataclasses import dataclass

from py_scripts.configs.db import DbConfig
from py_scripts.configs.sources import SourcesConfig


@dataclass
class Config:
    db: DbConfig
    sources: SourcesConfig
