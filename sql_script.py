from pathlib import Path as Path_T  # for type only
from typing import List
import re


class SqlScript(object):
    # inspired by https://github.com/honza/anosql
    # see also heron_load/db_util.py
    @classmethod
    def ea_stmt(cls, txt: str) -> List[str]:
        txt = re.sub(r'/\*(\*[^/]|[^\*])*\*/', '', txt)
        return [stmt.strip()
                for stmt in txt.split(';\n')]

    @classmethod
    def find_ddl(cls, name: str, script: str) -> str:
        for stmt in cls.ea_stmt(script):
            if stmt.startswith('create '):
                if name in stmt.split('\n')[0].split():
                    return stmt
        raise KeyError(name)

    @classmethod
    def find_ddl_in(cls, name: str, path: Path_T) -> str:
        script = path.open().read()
        return cls.find_ddl(name, script)
