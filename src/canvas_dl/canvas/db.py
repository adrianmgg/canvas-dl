import abc
import datetime
import json
import sqlite3
from functools import cache
from os import PathLike
from pathlib import Path
from typing import Any, Self, assert_never, cast, overload

from pydantic import TypeAdapter

from canvas_dl.canvas import models


class _DBResourceTableInfo[M: models.Model, ID](abc.ABC):
    table_name: str
    model: type[M]
    id_column_names: tuple[str, ...]

    @abc.abstractmethod
    def id_to_column_dict(self, id_: ID, /) -> dict[str, int]: ...

    def descriptor(self) -> '_DBResourceTableDescriptor[M, ID]':
        return _DBResourceTableDescriptor(self)

    @cache
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

    @cache
    def query_where_ids(self) -> str:
        return '(' + ' AND '.join(f'{n} = ?' for n in self.id_column_names) + ')'


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


class _DBResourceTableDescriptor[M: models.Model, ID]:
    info: _DBResourceTableInfo[M, ID]

    def __init__(self, info: _DBResourceTableInfo[M, ID], /) -> None:
        self.info = info

    @overload
    def __get__(self, obj: 'None', objtype: Any = None) -> Self: ...
    @overload
    def __get__(self, obj: 'CanvasDB', objtype: Any = None) -> '_DBResourceTableBound[M, ID]': ...
    def __get__(
        self, obj: 'CanvasDB | None', objtype: Any = None
    ) -> 'Self | _DBResourceTableBound[M, ID]':
        if obj is None:
            return self
        return _DBResourceTableBound(self.info, obj)


class _DBResourceTableBound[M: models.Model, ID]:
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
        ]
        self.__db_con.execute(f'CREATE TABLE {self.__info.table_name}({", ".join(columns)}) STRICT')
        self.__db_con.execute(
            f'CREATE UNIQUE INDEX idx_{self.__info.table_name}_current ON {self.__info.table_name} ({", ".join(self.__info.id_column_names)}) WHERE is_current = 1'
        )

    def get(self, id_: ID, /) -> M | None:
        with self.__db_con:
            id_cols = self.__info.id_to_column_dict(id_)
            res = self.__db_con.execute(
                self.__info.query_select(('data',), tuple(id_cols.values()))
            )
            match res.fetchone():
                case None:
                    return None
                case (str(data),):
                    return TypeAdapter(self.__info.model).validate_json(data)
                case unreachable:
                    assert_never(unreachable)

    def insert(self, id_: ID, item: M, /) -> None:
        source_date = datetime.datetime.now()  # TODO should be time of request not time of db write
        with self.__db_con:
            id_cols = self.__info.id_to_column_dict(id_)
            existing_data, prev_version = cast(
                tuple[str, int] | None,
                self.__db_con.execute(
                    self.__info.query_select(('data', 'version')), tuple(id_cols.values())
                ).fetchone(),
            ) or (None, -1)
            new_data = json.dumps(item._raw, sort_keys=True)
            if existing_data is not None and existing_data == new_data:
                self.__db_con.execute(
                    f'UPDATE {self.__info.table_name} SET last_seen_on = ? WHERE {self.__info.query_where_ids()} AND version = ?',
                    (source_date, *id_cols.values(), prev_version),
                )
            else:
                self.__db_con.execute(
                    f'UPDATE {self.__info.table_name} SET is_current = 0 WHERE {self.__info.query_where_ids()} AND version = ?',
                    (*id_cols.values(), prev_version),
                )
                self.__db_con.execute(
                    f'INSERT INTO {self.__info.table_name} ({", ".join(self.__info.id_column_names)}, saved_on, last_seen_on, version, is_current, data) VALUES({", ".join(["?"] * (len(self.__info.id_column_names) + 5))})',
                    (*id_cols.values(), source_date, source_date, prev_version + 1, 1, new_data),
                )


class CanvasDB:
    db: sqlite3.Connection

    # tables
    courses = _DBRTI_Simple[models.Course, models.CourseId]('course', models.Course).descriptor()
    folders = _DBRTI_Simple[models.Folder, models.FolderId]('folder', models.Folder).descriptor()
    files = _DBRTI_Simple[models.File, models.FileId]('file', models.File).descriptor()
    modules = _DBRTI_WithinCourse[models.Module, models.ModuleId](
        'module', models.Module
    ).descriptor()
    module_items = _DBRTI_WithinCourseModule[models.ModuleItem, models.ModuleItemId](
        'moduleitem', models.ModuleItem
    ).descriptor()

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
