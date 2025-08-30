import abc
import datetime
from copy import deepcopy
from typing import Any, Literal, Self

import pydantic
from pydantic import BaseModel, RootModel, model_validator
from yarl import URL


class DBResourceItem(abc.ABC):
    @abc.abstractmethod
    def to_db_json(self, /) -> Any: ...  # noqa: ANN401
    @abc.abstractmethod
    def to_db_json_hash_normalized(self, /) -> Any: ...  # noqa: ANN401
    @classmethod
    @abc.abstractmethod
    def from_db_json(cls, data: Any, /) -> Self: ...  # noqa: ANN401


class Model(BaseModel, DBResourceItem):
    class Config:
        extra = 'allow'

    _raw: Any

    # TODO add a wrap validator to debug log any extra fields?

    @model_validator(mode='wrap')
    @classmethod
    def store_raw_data(cls, data: Any, handler: pydantic.ModelWrapValidatorHandler[Self]) -> Self:  # noqa: ANN401
        ret = handler(data)
        ret._raw = data
        return ret

    def to_db_json(self, /) -> Any:  # noqa: ANN401
        return self._raw

    def to_db_json_hash_normalized(self, /) -> Any:  # noqa: ANN401
        return self._raw

    @classmethod
    def from_db_json(cls, data: Any, /) -> Self:  # noqa: ANN401
        return cls.model_validate(data)


class IdModel(RootModel[int], DBResourceItem):
    def to_db_json(self, /) -> Any:  # noqa: ANN401
        return self.root

    def to_db_json_hash_normalized(self, /) -> Any:  # noqa: ANN401
        return self.root

    @classmethod
    def from_db_json(cls, data: Any, /) -> Self:  # noqa: ANN401
        match data:
            case int(val):
                return cls(val)
            case other:
                raise TypeError(f'expected int argument, got {type(other)}')

    def __str__(self) -> str:
        return str(self.root)


class CourseId(IdModel): ...


class FolderId(IdModel): ...


class FileId(IdModel): ...


class ModuleId(IdModel): ...


class ModuleItemId(IdModel): ...


# https://developerdocs.instructure.com/services/canvas/resources/courses#course
class Course(Model):
    id: CourseId
    # TODO sis_course_id, integration_id, sis_import_id
    uuid: str
    name: str
    course_code: str
    original_name: str | None = None
    workflow_state: str
    account_id: int
    root_account_id: int
    enrollment_term_id: int
    # TODO grading_periods, grading_standard_id, grade_passback_setting
    created_at: datetime.datetime
    start_at: datetime.datetime
    end_at: datetime.datetime
    locale: str | None = None
    # TODO enrollments
    total_students: int
    # TODO calendar, default_view, syllabus_body, needs_grading_count, term, etc ...
    image_download_url: str | None = None
    banner_image_download_url: str | None = None

    def to_db_json_hash_normalized(self, /) -> Any:  # noqa: ANN401
        data = deepcopy(super().to_db_json_hash_normalized())
        for url_key in 'image_download_url', 'banner_image_download_url':
            if url_key in data and isinstance(url := data[url_key], str):
                try:
                    data[url_key] = str(URL(url).without_query_params('token'))
                except ValueError:  # don't fail if the url is for some reason not able to be parsed
                    pass
        return data

    def __str__(self) -> str:
        return f'Course<id={self.id!r}, name={self.name!r}>'


# https://developerdocs.instructure.com/services/canvas/resources/files
class Folder(Model):
    id: FolderId
    name: str
    full_name: str
    parent_folder_id: FolderId | None
    created_at: datetime.datetime
    updated_at: datetime.datetime
    files_count: int
    folders_count: int
    files_url: str  # TODO: URL
    folders_url: str  # TODO: URL
    locked: bool
    locked_for_user: bool
    # ... more fields ...


# https://developerdocs.instructure.com/services/canvas/resources/files
class File(Model):
    id: FileId
    uuid: str
    display_name: str
    filename: str
    content_type: str | None = None
    url: str  # TODO: url
    size: int
    """filesize in bytes"""
    created_at: datetime.datetime
    updated_at: datetime.datetime
    modified_at: datetime.datetime
    unlock_at: datetime.datetime | None
    lock_at: datetime.datetime | None
    locked: bool
    hidden: bool
    hidden_for_user: bool
    media_entry_id: str | None
    """ "identifier for file in third-party transcoding service" """
    # ... more fields ...


# https://developerdocs.instructure.com/services/canvas/resources/modules
class Module(Model):
    id: ModuleId
    position: int
    """ "the position of this module in the course (1-based)" """
    name: str
    unlock_at: datetime.datetime | None = None
    require_sequential_progress: bool
    """ "Whether module items must be unlocked in order" """
    requirement_type: str  # TODO enum
    prerequisite_module_ids: list[ModuleId]
    items_count: int
    items_url: str
    items: None = None
    """ (always null since we will not be using include[]=items) """
    state: str  # TODO enum
    completed_at: datetime.datetime | None = None
    publish_final_grade: bool | None = None
    # other documented fields: published


# https://developerdocs.instructure.com/services/canvas/resources/modules
class ModuleItemCompletionRequirement(Model):
    pass  # TODO


# https://developerdocs.instructure.com/services/canvas/resources/modules
class ModuleItemContentDetails(Model):
    pass  # TODO


# https://developerdocs.instructure.com/services/canvas/resources/modules
class ModuleItem(Model):
    id: ModuleItemId
    module_id: ModuleId
    position: int
    """ "the position of this item in the module (1-based)" """
    title: str
    indent: int
    """ "0-based indent level; module items may be indented to show a hierarchy" """
    type: Literal[
        'File',
        'Page',
        'Discussion',
        'Assignment',
        'Quiz',
        'SubHeader',
        'ExternalUrl',
        'ExternalTool',
    ]
    """ "the type of object referred to" """
    content_id: int | None = None
    """ "the id of the object referred to. applies to 'File', 'Discussion', 'Assignment', 'Quiz', 'ExternalTool' types" """
    html_url: str
    """ "link to the item in Canvas" """
    url: str | None = None
    """ "link to the Canvas API object, if applicable" """
    page_url: str | None = None
    """ "(only for 'Page' type) unique locator for the linked wiki page" """
    external_url: str | None = None
    completion_requirement: ModuleItemCompletionRequirement | None = None
    content_details: ModuleItemContentDetails
    # other documented fields: new_tab, published
