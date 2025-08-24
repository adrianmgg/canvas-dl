from pathlib import Path
import asyncio
import pprint
import click
from yarl import URL

from canvas_dl import canvas


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
def cli(**kwargs) -> None:
    asyncio.run(main(**kwargs))


async def main(*, site: URL, cookies_file: Path) -> None:
    async with canvas.Canvas.new_simple(url=site, cookies_file=cookies_file) as api:
        async for course in api.list_courses():
            print(f'course: {course.name}')
            async for folder in api.list_course_folders(course.id):
                print(f'{folder.full_name}')
                try:
                    async for file in api.list_folder_files(folder.id):
                        print(f'{course.id}/{folder.full_name}/{file.filename}')
                        outpath = Path(f'{course.id}/{folder.full_name}/{file.filename}')
                        async with api._session.get(file.url) as file_response:
                            if not file_response.ok:
                                print(f'  ERR: failed downloading ({file_response.status})')
                            else:
                                outpath.parent.mkdir(parents=True, exist_ok=True)
                                outpath.write_bytes(await file_response.read())
                except:
                    print(f'ERROR LISTING FILES FOR FOLDER {folder}')
