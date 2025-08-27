import asyncio
import pprint
from pathlib import Path

import click
from yarl import URL

from canvas_dl import canvas
from canvas_dl.canvas.db import CanvasDB


def validate_site(ctx, param, value: URL) -> URL:
    if not value.absolute:
        raise click.BadParameter("url not absolute (are you missing 'https://' at the start?)")
    return value


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
def cli(**kwargs) -> None:
    asyncio.run(main(**kwargs))


async def main(*, site: URL, cookies_file: Path, output_root: Path) -> None:
    output_root.mkdir(exist_ok=True, parents=True)
    db = CanvasDB(output_root / 'db.sqlite3')
    async with canvas.Canvas.new_simple(url=site, cookies_file=cookies_file) as api:
        async for course in api.list_courses():
            db.courses.insert(course.id, course)
            async for module in api.list_course_modules(course.id):
                db.modules.insert((module.id, course.id), module)
                async for module_item in api.list_module_items(course.id, module.id):
                    db.module_items.insert((module_item.id, course.id, module.id), module_item)
        # async for course in api.list_courses():
        #     print(f'course: {course.name}')
        #     async for module in api.list_course_modules(course.id):
        #         print(f'    module #{module.position} {module.name!r} ({module.id})')
        #         async for module_item in api.list_module_items(course.id, module.id):
        #             print(
        #                 f'        item #{module_item.position} ({module_item.type}) {module_item.title!r}'
        #             )
        #     # async for folder in api.list_course_folders(course.id):
        #     #     print(f'{folder.full_name}')
        #     #     try:
        #     #         async for file in api.list_folder_files(folder.id):
        #     #             print(f'{course.id}/{folder.full_name}/{file.filename}')
        #     #             outpath = Path(f'{course.id}/{folder.full_name}/{file.filename}')
        #     #             async with api._session.get(file.url) as file_response:
        #     #                 if not file_response.ok:
        #     #                     print(f'  ERR: failed downloading ({file_response.status})')
        #     #                 else:
        #     #                     outpath.parent.mkdir(parents=True, exist_ok=True)
        #     #                     outpath.write_bytes(await file_response.read())
        #     #     except:
        #     #         print(f'ERROR LISTING FILES FOR FOLDER {folder}')
