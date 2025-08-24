import datetime
from typing import NewType

from pydantic import BaseModel


class Model(BaseModel):
    class Config:
        extra = 'allow'


CourseId = NewType('CourseId', int)
FolderId = NewType('FolderId', int)
FileId = NewType('FileId', int)


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
    # TODO calendar, default_view, syllabus_body, needs_grading_count, term
    # etc ...

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
