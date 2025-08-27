import asyncio
import hashlib
import tempfile
from collections import deque
from functools import partial
from pathlib import Path

import click
from aiohttp import ClientResponseError
from yarl import URL

from canvas_dl import canvas
from canvas_dl.canvas.db import CanvasDB
from canvas_dl.util import normalize_for_filename


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
            course_files_dir = output_root / 'courses' / str(course.id) / 'files'

            db.courses.insert(course.id, course)
            # async for module in api.list_course_modules(course.id):
            #     db.modules.insert((module.id, course.id), module)
            #     async for module_item in api.list_module_items(course.id, module.id):
            #         db.module_items.insert((module_item.id, course.id, module.id), module_item)

            # folders & files of the course
            root_folder = (await api.course_resolve_folder_path(course.id, ''))[0]
            seen_folders = set[int]()
            db.folders.insert(root_folder.id, root_folder)
            folders_to_process = deque([(root_folder, course_files_dir)])
            while len(folders_to_process) > 0:
                cur_folder, folder_contents_dir = folders_to_process.pop()
                if root_folder.id in seen_folders:
                    print('WTF??')
                seen_folders.add(root_folder.id)
                print(f'dl folder {folder_contents_dir.relative_to(course_files_dir)}')
                assert folder_contents_dir.is_relative_to(course_files_dir)
                # subfolders
                try:
                    async for subfolder in api.list_folder_subfolders(cur_folder.id):
                        db.folders.insert(subfolder.id, subfolder)
                        folders_to_process.append(
                            (
                                subfolder,
                                folder_contents_dir / normalize_for_filename(subfolder.name),
                            )
                        )
                except ClientResponseError as e:
                    print(f'ERROR LISTING SUBFOLDERS FOR FOLDER {cur_folder.id=}! : {e=}')
                # files
                try:
                    async for file in api.list_folder_files(cur_folder.id):
                        filename_normalized = normalize_for_filename(file.filename)
                        would_insert, version_num = db.files.insert(file.id, file, dry_run=True)
                        file_out_path = folder_contents_dir / filename_normalized
                        file_numbered_out_path = (
                            file_out_path / f'{version_num:05d} {filename_normalized}'
                        )
                        if would_insert:  # don't bother downloading if no metadata changes
                            print(
                                f'dl  file {file_out_path.relative_to(course_files_dir)} ({file.display_name})'
                            )
                            with tempfile.NamedTemporaryFile(
                                delete_on_close=False, delete=False
                            ) as download_tempfile:
                                download_hasher = hashlib.sha256(usedforsecurity=False)
                                async with api._session.get(file.url) as resp:
                                    async for chunk in resp.content.iter_chunked(4096):
                                        download_tempfile.write(chunk)
                                        download_hasher.update(chunk)
                            download_hash = download_hasher.digest()
                            # breakpoint()
                            if file_out_path.exists() and file_out_path.is_dir():
                                existing_versions = sorted(
                                    file_out_path.iterdir(), key=lambda p: p.name[:5], reverse=True
                                )
                                latest_existing_version = next(iter(existing_versions), None)
                                if latest_existing_version is not None:
                                    with latest_existing_version.open('rb') as f:
                                        existing_version_hash = hashlib.file_digest(
                                            f, partial(hashlib.sha256, usedforsecurity=False)
                                        )
                                    if existing_version_hash != download_hash:
                                        assert not file_numbered_out_path.exists()
                                        Path(download_tempfile.name).rename(file_numbered_out_path)
                            elif file_out_path.exists():  # (but is not a directory)
                                single_existing = file_out_path
                                with single_existing.open('rb') as f:
                                    existing_version_hash = hashlib.file_digest(
                                        f, partial(hashlib.sha256, usedforsecurity=False)
                                    )
                                if existing_version_hash != download_hash:
                                    with tempfile.TemporaryDirectory(
                                        'canvas-dl', f'{file.id}-v{version_num}'
                                    ) as tmpdirname:
                                        tmpdir = Path(tmpdirname)
                                        single_existing.rename(tmpdir / 'old_file')
                                        file_out_path.mkdir()
                                        # TODO for now this just assumes the previously downloaded one was version #0
                                        assert version_num != 0
                                        (tmpdir / 'old_file').rename(f'00000 {filename_normalized}')
                                        Path(download_tempfile.name).rename(file_numbered_out_path)
                            else:  # no prev version exists on disk
                                file_out_path.parent.mkdir(exist_ok=True, parents=True)
                                Path(download_tempfile.name).rename(file_out_path)
                        db.files.insert(file.id, file)
                except ClientResponseError as e:
                    print(f'ERROR LISTING FILES FOR FOLDER {cur_folder.id=}! : {e=}')

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
