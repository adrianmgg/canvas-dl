import asyncio
import functools
import hashlib
import tempfile
from collections import deque
from collections.abc import Callable, Coroutine
from functools import partial
from pathlib import Path

import click
from aiohttp import ClientResponseError
from yarl import URL

from canvas_dl import canvas
from canvas_dl.canvas.db import CanvasDB
from canvas_dl.util import normalize_for_filename


def validate_site(_ctx: click.Context, _param: click.Argument, value: URL) -> URL:
    if not value.absolute:
        raise click.BadParameter("url not absolute (are you missing 'https://' at the start?)")
    return value


def async_main[**P, R](main_fn: Callable[P, Coroutine[None, None, R]]) -> Callable[P, R]:
    @functools.wraps(main_fn)
    def sync_main(*args: P.args, **kwargs: P.kwargs) -> R:
        return asyncio.run(main_fn(*args, **kwargs))

    return sync_main


@click.command()
@click.argument('site', type=URL, callback=validate_site)
@click.option(
    '--cookies',
    'cookies_file',
    type=click.Path(exists=True, readable=True, file_okay=True, dir_okay=False, path_type=Path),
    default='./cookies.txt',
)
@click.option(
    '--out',
    '-o',
    'output_root',
    type=click.Path(file_okay=False, dir_okay=True, allow_dash=False, path_type=Path),
    default='./dl/',
)
@async_main
async def main(*, site: URL, cookies_file: Path, output_root: Path) -> None:
    output_root.mkdir(exist_ok=True, parents=True)
    db = CanvasDB(output_root / 'db.sqlite3')
    async with canvas.Canvas.new_simple(url=site, cookies_file=cookies_file) as api:
        async for course in api.list_courses():
            course_files_dir = output_root / 'courses' / str(course.id) / 'files'

            async for folder in api.list_course_folders(course.id):
                try:
                    async for file in api.list_folder_files(folder.id):
                        outpath = (
                            course_files_dir / f'{folder.full_name}/[{file.id}] {file.filename}'
                        )
                        print(f'downloading {outpath}')
                        async with api._session.get(file.url) as file_response:
                            if not file_response.ok:
                                print(f'  ERR: failed downloading ({file_response.status})')
                            else:
                                outpath.parent.mkdir(parents=True, exist_ok=True)
                                outpath.write_bytes(await file_response.read())
                except:
                    print(f'ERROR LISTING FILES FOR FOLDER {folder} (this happens sometimes)')

            db.courses.insert(course.id, course)
            async for module in api.list_course_modules(course.id):
                db.modules.insert((module.id, course.id), module)
                async for module_item in api.list_module_items(course.id, module.id):
                    db.module_items.insert((module_item.id, course.id, module.id), module_item)
