from canvas_dl.canvas.models import File
from typing import (
    AsyncIterator,
    TypedDict,
    Unpack,
)
import operator
from pathlib import Path
from types import TracebackType
import aiohttp
import aiohttp.typedefs
from aiohttp import ClientSession
from contextlib import AbstractAsyncContextManager
import typing
import http.cookies
from http.cookiejar import MozillaCookieJar
from yarl import URL
from pydantic import TypeAdapter

from canvas_dl.canvas import models


class AioHttpMozillaCookieJar(aiohttp.cookiejar.CookieJar):
    # TODO save(...) ?

    def load(self, file_path: aiohttp.typedefs.PathLike) -> None:
        jar = MozillaCookieJar()
        jar.load(str(file_path), ignore_discard=True, ignore_expires=True)

        @operator.call
        def cookies():  # noqa: ANN202
            for cookie in jar:
                morsel = http.cookies.Morsel[str]()
                assert cookie.value is not None
                morsel.set(cookie.name, cookie.value, cookie.value)  # TODO coded value
                # TODO expires
                morsel['path'] = cookie.path
                morsel['domain'] = cookie.domain
                morsel['secure'] = cookie.secure
                yield cookie.name, morsel

        self.update_cookies(cookies)


class _ApiTypes:
    type params = dict[str, str | list[str]]
    type method = str


class _ApiKwargs(TypedDict, total=False):
    params: _ApiTypes.params
    method: _ApiTypes.method


class Canvas(AbstractAsyncContextManager['Canvas', None]):
    url: URL
    _session: ClientSession

    def __init__(self, url: URL, session: ClientSession) -> None:
        self.url = url
        self._session = session

    @classmethod
    def new_simple(cls: type[typing.Self], /, url: URL, *, cookies_file: Path) -> typing.Self:
        cookie_jar = AioHttpMozillaCookieJar()
        cookie_jar.load(str(cookies_file))
        return cls(url, ClientSession(cookie_jar=cookie_jar))

    async def __aenter__(self) -> typing.Self:
        _ = await self._session.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
        /,
    ) -> None:
        await self._session.__aexit__(exc_type, exc_value, traceback)

    @property
    def _api_url(self) -> URL:
        return self.url.joinpath('api/v1/')

    async def _api(
        self,
        path: str | URL,
        /,
        *,
        params: _ApiTypes.params | None = None,
        method: _ApiTypes.method = 'GET',
    ) -> aiohttp.ClientResponse:
        match URL(path):
            case URL(absolute=True) as absolute_url:
                url = absolute_url
            case relative_endpoint:
                url = self._api_url.join(relative_endpoint)
        response = await self._session.request(method=method, url=url, params=params)
        # print(await response.json())
        return response

    async def _api_json[T](
        self,
        type_: type[T],
        path: str | URL,
        /,
        **kwargs: Unpack[_ApiKwargs],
    ) -> tuple[aiohttp.ClientResponse, T]:
        response = await self._api(path, **kwargs)
        # print(await response.text())
        return response, TypeAdapter(type_).validate_json(await response.text())

    # https://developerdocs.instructure.com/services/canvas/basics/file.pagination
    async def _paginate[T](
        self,
        type_: type[T],
        path: str | URL,
        /,
        **kwargs: Unpack[_ApiKwargs],
    ) -> AsyncIterator[T]:
        Page = list[type_]  # type: ignore[valid-type]
        # some kwargs need to be passed on for every page, but the ones that effect the final url should *not* be
        kwargs_always = _ApiKwargs()
        match kwargs.pop('method', None):
            case str(method):
                kwargs_always['method'] = method
        # first page
        response, items = await self._api_json(Page, path, **(kwargs | kwargs_always))
        for item in items:
            yield item
        # subsequent pages
        while (next_page := response.links.get('next')) is not None and 'url' in next_page:
            response, items = await self._api_json(Page, next_page['url'], **kwargs_always)
            for item in items:
                yield item

    def list_courses(self) -> AsyncIterator[models.Course]:
        return self._paginate(
            models.Course,
            'courses',
            params={
                'include[]': [
                    'needs_grading_count',
                    'syllabus_body',
                    'public_description',
                    'total_scores',
                    'current_grading_period_scores',
                    'grading_periods',
                    'term',
                    'account',
                    'course_progress',
                    'sections',
                    'storage_quota_used_mb',
                    'total_students',
                    'passback_status',
                    'favorites',
                    'teachers',
                    'observed_users',
                    'tabs',
                    'course_image',
                    'banner_image',
                    'concluded',
                    'post_manually',
                ]
            },
        )

    # https://developerdocs.instructure.com/services/canvas/resources/files#method.folders.list_all_folders
    def list_course_folders(self, course_id: int) -> AsyncIterator[models.Folder]:
        return self._paginate(models.Folder, f'courses/{course_id}/folders')

    # https://developerdocs.instructure.com/services/canvas/resources/files#method.files.api_index
    def list_folder_files(self, folder_id: int) -> AsyncIterator[File]:
        return self._paginate(
            models.File,
            f'folders/{folder_id}/files',
            params={'include[]': ['user', 'usage_rights']},
        )
