#!/usr/bin/env python
# -*- coding: utf-8 -*-

import collections
import json
import os
import re
from collections import defaultdict
from sys import platform
from shutil import copyfile
from pathlib import Path
from typing import List

import prefect
from prefect import Flow, task
from prefect.utilities.debug import raise_on_exception
from pipeop import pipes

BOUNDARY = u"==========\r\n"
DATA_FILE = u"clips.json"
OUTPUT_DIR = u"output"
MY_CLIPPINGS = Path(u'My Clippings.txt')


# Tasks
# *****

def _get_path_to_kindle_clippings() -> Path:

    if platform == 'darwin': # Mac / OSX 
        path_to_clippings = Path(r"/Volumes/Kindle/documents/My Clippings.txt")
    
    elif platform == 'win32': # Windows
        raise NotImplementedError(
           r"Please replace this with absolute path to 'My Clippings.txt' for Windows..."
        )

    elif platform == "linux" or platform == "linux2":
        raise NotImplementedError(
           r"Please replace this with absolute path to 'My Clippings.txt' for Linux..."
        )
       
    return path_to_clippings


def _copy_clippings_from_kindle_to_cwd(clippings_local: Path, clippings_kindle: Path) -> None:

    if clippings_kindle.exists():
        copyfile(clippings_kindle, clippings_local)
    else:
        raise Exception(
        "Cannot find Kindle!\n"
        f"Ensure Kindle is plugged in and 'My Clippings.txt' is saved at {clippings_kindle}"
    )


@task
def _extract_kindle_clippings(clippings_local: Path = Path(u"My Clippings.txt")) -> Path:

    logger = prefect.context.get("logger")
    
    if not clippings_local.exists():
        kindle_clippings = _get_path_to_kindle_clippings()
        _copy_clippings_from_kindle_to_cwd(clippings_local, clippings_kindle)
    else:
        logger.info("Clippings already exists locally.  Delete local copy to extract again...")
        
    return clippings_local

@task
def _load_clippings_to_string(filename: Path):

    with open(filename, 'rb') as f:
        content = f.read().decode('utf-8')
    
    return content


def _split_clippings_string_into_list(raw_clippings: str) -> List[str]:

    return (
        raw_clippings.replace(u"\r", "")
        .replace(u"\n\n", "\n")
        .replace(u"\ufeff", "")
        .split("==========")
    )


def _link_books_with_quotes(clippings: List[str]) -> defaultdict(list):

    books_with_quotes = defaultdict(list)

    for clipping in clippings:
        split_clipping = clipping.strip(u"\n").split(u"\n")

        book = split_clipping[0]

        # NOTE: don't save empty quotes (i.e. len==2)
        if len(split_clipping) == 3: 
            quote = split_clipping[2]

            books_with_quotes[book].append(quote)

    import ipdb; ipdb.set_trace()
    return books_with_quotes


def _link_books_with_quotes_and_location(clippings: List[str]) -> defaultdict(list):

    books_with_quotes_and_location = defaultdict(list) 

    location_regex = re.compile(r"(location \d+-\d+)")

    for clipping in clippings:
        split_clipping = clipping.strip(u"\n").split(u"\n")
    
        book = split_clipping[0]

        # NOTE: don't save empty quotes (i.e. len==2)
        if len(split_clipping) == 3: 
            location = location_regex.findall(split_clipping[1])[0]
            quote = split_clipping[2]

            books_with_quotes_and_location[book].append([quote, location])

    return books_with_quotes_and_location


@task
def _transform_clippings_into_ordereddict(raw_clippings: str):

    clippings = _split_clippings_string_into_list(raw_clippings)

    return _link_books_with_quotes(clippings)


# Flow
# ****

@pipes
def _etl_clippings_to_folder():

    with Flow("Extract clippings from Kindle to files") as flow:

        clippings_local = _extract_kindle_clippings()
        clippings_by_book = (
            _load_clippings_to_string(clippings_local) 
            >> _transform_clippings_into_ordereddict 
        )

        # _save_clippings_to_files(clippings_by_book)

    return flow


# @task
# def _

def export_txt(clips):
    """
    Export each book's clips to single text.
    """
    for book in clips:
        lines = []
        for pos in sorted(clips[book]):
            lines.append(clips[book][pos].encode('utf-8'))

        filename = os.path.join(OUTPUT_DIR, u"%s.md" % book)
        with open(filename, 'w') as f:
            f.write("\n\n---\n\n".join(str(lines)))


def load_clips():
    """
    Load previous clips from DATA_FILE
    """
    try:
        with open(DATA_FILE, 'rb') as f:
            return json.load(f)
    except (IOError, ValueError):
        return {}


def save_clips(clips):
    """
    Save new clips to DATA_FILE
    """
    with open(DATA_FILE, 'w') as f:
        json.dump(clips, f)


def main():

    local_clippings = Path("My Clippings.txt")
    kindle_clippings = _get_path_to_kindle_clippings()
    _copy_clippings_from_kindle(local_clippings, kindle_clippings)

    raw_clippings = _load_clippings_to_string(local_clippings)
    _split_clippings_into_ordereddict(raw_clippings)

    # load old clips
    clips = collections.defaultdict(dict)
    clips.update(load_clips())

    # extract clips
    sections = get_sections(MY_CLIPPINGS)
    for section in sections:
        clip = get_clip(section)
        if clip:
            clips[clip['book']][str(clip['position'])] = clip['content']

    # remove key with empty value
    clips = {k: v for k, v in clips.items() if v}

    # save/export clips
    save_clips(clips)
    export_txt(clips)


if __name__ == '__main__':
    # main()
    flow = _etl_clippings_to_folder()

    with raise_on_exception():
        flow.run()

