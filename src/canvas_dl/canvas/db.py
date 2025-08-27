import abc
import datetime
import hashlib
import json
import sqlite3
from functools import cache, cached_property
from os import PathLike
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Self, assert_never, cast, overload

from pydantic import TypeAdapter

from canvas_dl.canvas import models


class TableDescriptor[DB, Info, Handler]:
    handler: Callable[[Info, DB], Handler]
    info: Info

    def __init__(self, handler: Callable[[Info, DB], Handler], info: Info) -> None:
        self.handler = handler
        self.info = info

    @overload
    def __get__(self, obj: None, objtype: Any = None) -> Self: ...  # noqa: ANN401
    @overload
    def __get__(self, obj: DB, objtype: Any = None) -> Handler: ...  # noqa: ANN401
    def __get__(self, obj, objtype=None):
        match obj:
            case None:
                return self
            case db:
                return self.handler(self.info, db)

    def __repr__(self) -> str:
        return f'TableDescriptor(info={self.info!r}, handler={self.handler!r})'


class _DBResourceTableInfo[M: models.Model, ID](abc.ABC):
    table_name: str
    model: type[M]
    id_column_names: tuple[str, ...]

    @abc.abstractmethod
    def id_to_column_dict(self, id_: ID, /) -> dict[str, int]: ...

    def query_select(self, what: str | tuple[str, ...], *, current_only: bool = True) -> str:
        match what:
            case str(single):
                what = single
            case multiple:
                what = ','.join(multiple)
        where = self.query_where_ids()
        if current_only:
            where = f'{where} AND is_current = 1'
        return f'SELECT {what} FROM {self.table_name} WHERE {where}'

    def query_where_ids(self) -> str:
        return '(' + ' AND '.join(f'{n} = :{n}' for n in self.id_column_names) + ')'

    if not TYPE_CHECKING:
        query_select = cache(query_select)
        query_where_ids = cache(query_where_ids)

    @cached_property
    def query_ids_named_placeholders(self) -> str:
        return ', '.join(f':{n}' for n in self.id_column_names)


class _DBRTI_Simple[M: models.Model, ID: int](_DBResourceTableInfo[M, ID]):
    id_column_names = ('id',)

    def __init__(self, table_name: str, model: type[M], /) -> None:
        self.table_name = table_name
        self.model = model

    def id_to_column_dict(self, id_: ID, /) -> dict[str, int]:
        return {'id': id_}


class _DBRTI_WithinCourse[M: models.Model, MainID: int](
    _DBResourceTableInfo[M, tuple[MainID, models.CourseId]]
):
    id_column_names = 'id', 'course_id'

    def __init__(self, table_name: str, model: type[M], /) -> None:
        self.table_name = table_name
        self.model = model

    def id_to_column_dict(self, ids: tuple[MainID, models.CourseId], /) -> dict[str, int]:
        id_, course_id = ids
        return {'id': id_, 'course_id': course_id}


class _DBRTI_WithinCourseModule[M: models.Model, MainID: int](
    _DBResourceTableInfo[M, tuple[MainID, models.CourseId, models.ModuleId]]
):
    id_column_names = 'id', 'course_id', 'module_id'

    def __init__(self, table_name: str, model: type[M], /) -> None:
        self.table_name = table_name
        self.model = model

    def id_to_column_dict(
        self, ids: tuple[MainID, models.CourseId, models.ModuleId], /
    ) -> dict[str, int]:
        id_, course_id, module_id = ids
        return {'id': id_, 'course_id': course_id, 'module_id': module_id}


class _DBResourceTable[M: models.Model, ID]:
    __info: _DBResourceTableInfo[M, ID]
    __db: 'CanvasDB'

    def __init__(self, info: _DBResourceTableInfo[M, ID], db: 'CanvasDB') -> None:
        self.__info = info
        self.__db = db

    @property
    def __db_con(self) -> sqlite3.Connection:
        return self.__db.db

    def create_table(self) -> None:
        id_type = 'INTEGER NOT NULL'
        columns = [
            *(f'{c} {id_type}' for c in self.__info.id_column_names),
            # timestamp of when this was first saved
            'saved_on TEXT NOT NULL',
            # timestamp of when this was most recently re-saved (and had no changes to its data)
            'last_seen_on TEXT NOT NULL',
            # nth of this id which we've saved
            'version INTEGER NOT NULL',
            # 1 iif this is the most recent saved version of this thing saved, 0 otherwise
            'is_current INTEGER NOT NULL CHECK (is_current IN (0, 1))',
            # json blob with the actual info
            'data TEXT NOT NULL',
            # hash of the data (possibly with some transformations applied beforehand, via code in models.py)
            'data_hash INTEGER NOT NULL',
        ]
        self.__db_con.execute(f'CREATE TABLE {self.__info.table_name}({", ".join(columns)}) STRICT')
        self.__db_con.execute(
            f'CREATE UNIQUE INDEX idx_{self.__info.table_name}_current ON {self.__info.table_name} ({", ".join(self.__info.id_column_names)}) WHERE is_current = 1'
        )

    def get(self, id_: ID, /) -> M | None:
        with self.__db_con:
            id_cols = self.__info.id_to_column_dict(id_)
            res = self.__db_con.execute(self.__info.query_select(('data',)), id_cols)
            match res.fetchone():
                case None:
                    return None
                case (str(data),):
                    return TypeAdapter(self.__info.model).validate_json(data)
                case unreachable:
                    assert_never(unreachable)

    def insert(self, id_: ID, item: M, /, *, dry_run: bool = False) -> tuple[bool, int]:
        """
        return value is tuple of (did we actually write a new item to the db?, version number of the written item)

        if `dry_run=True` passed, will give what the return value would be for those arguments, but no actual db changes will be made
        """
        source_date = datetime.datetime.now()  # TODO should be time of request not time of db write
        with self.__db_con:
            id_cols = self.__info.id_to_column_dict(id_)
            existing_data_hash, prev_version = cast(
                tuple[str, int] | None,
                self.__db_con.execute(
                    self.__info.query_select(('data_hash', 'version')), tuple(id_cols.values())
                ).fetchone(),
            ) or (None, -1)
            new_data = json.dumps(item._raw, sort_keys=True)
            # (we moduldo the hash digest down to a signed 64-bit int since that's what sqlite3 takes)
            new_data_hash = int.from_bytes(
                item.hash_for_db(hashlib.sha256(usedforsecurity=False)).digest()[-8:],
                'big',
                signed=True,
            )
            if existing_data_hash is not None and existing_data_hash == new_data_hash:
                if not dry_run:
                    # TODO last_seen_on should be max'd not just set
                    self.__db_con.execute(
                        f'UPDATE {self.__info.table_name} SET last_seen_on = :last_seen_on WHERE {self.__info.query_where_ids()} AND version = :version',
                        id_cols | {'last_seen_on': source_date, 'version': prev_version},
                    )
                return False, prev_version
            else:
                new_version = prev_version + 1
                if not dry_run:
                    self.__db_con.execute(
                        f'UPDATE {self.__info.table_name} SET is_current = 0 WHERE {self.__info.query_where_ids()} AND version = :version',
                        id_cols | {'version': prev_version},
                    )
                    self.__db_con.execute(
                        f'INSERT INTO {self.__info.table_name} VALUES({self.__info.query_ids_named_placeholders}, :saved_on, :last_seen_on, :version, :is_current, :data, :data_hash)',
                        id_cols
                        | {
                            'saved_on': source_date,
                            'last_seen_on': source_date,
                            'version': new_version,
                            'is_current': 1,
                            'data': new_data,
                            'data_hash': new_data_hash,
                        },
                    )
                return True, new_version


class CanvasDB:
    db: sqlite3.Connection

    # tables
    courses = TableDescriptor(
        _DBResourceTable, _DBRTI_Simple[models.Course, models.CourseId]('course', models.Course)
    )
    folders = TableDescriptor(
        _DBResourceTable, _DBRTI_Simple[models.Folder, models.FolderId]('folder', models.Folder)
    )
    files = TableDescriptor(
        _DBResourceTable, _DBRTI_Simple[models.File, models.FileId]('file', models.File)
    )
    modules = TableDescriptor(
        _DBResourceTable,
        _DBRTI_WithinCourse[models.Module, models.ModuleId]('module', models.Module),
    )
    module_items = TableDescriptor(
        _DBResourceTable,
        _DBRTI_WithinCourseModule[models.ModuleItem, models.ModuleItemId](
            'moduleitem', models.ModuleItem
        ),
    )

    def __init__(self, path: str | PathLike, /) -> None:
        path = Path(path)
        needs_setup = not path.exists()
        self.db = sqlite3.connect(path)
        if needs_setup:
            self.courses.create_table()
            self.folders.create_table()
            self.files.create_table()
            self.modules.create_table()
            self.module_items.create_table()
