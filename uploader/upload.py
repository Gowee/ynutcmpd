#!/usr/bin/env python3
import os.path
import itertools
import subprocess
import json
import logging
from io import BytesIO
import os
import functools
import re
import sys
from itertools import chain
from pathlib import Path
from tempfile import gettempdir


from more_itertools import peekable
import requests
import yaml
import mwclient
from zhconv_rs import zhconv as zhconv_

zhconv = lambda s, v: zhconv_(s, v) if type(s) == str else s
import img2pdf


# from getbook import getbook

CONFIG_FILE_PATH = os.path.join(os.path.dirname(__file__), "config.yml")
POSITION_FILE_PATH = os.path.join(os.path.dirname(__file__), ".position")
DATA_DIR = Path(__file__).parent / "../crawler/data"
BLOB_DIR = Path(__file__).parent / "blobs"
RETRY_TIMES = 3
# TEMP_DIR = Path()gettempdir())

USER_AGENT = "ynutcmpd/0.0 (+https://github.com/gowee/ynutcmpd)"

# RESP_DUMP_PATH = "/tmp/wmc_upload_resp_dump.html"

LOGLEVEL = os.environ.get("LOGLEVEL", "INFO").upper()
logging.basicConfig(level=LOGLEVEL)
logger = logging.getLogger(__name__)


def call(command, *args, **kwargs):
    kwargs["shell"] = True
    return subprocess.check_call(command, *args, **kwargs)


def load_position(name):
    logger.info(f'Loading position from {POSITION_FILE_PATH + "." + name}')
    if os.path.exists(POSITION_FILE_PATH + "." + name):
        with open(POSITION_FILE_PATH + "." + name, "r") as f:
            return f.read().strip()
    else:
        return None


def store_position(name, position):
    with open(POSITION_FILE_PATH + "." + name, "w") as f:
        f.write(position)


def retry(times=RETRY_TIMES):
    def wrapper(fn):
        tried = 0

        @functools.wraps(fn)
        def wrapped(*args, **kwargs):
            while True:
                try:
                    return fn(*args, **kwargs)
                except Exception as e:
                    nonlocal tried
                    tried += 1
                    if tried >= times:
                        raise Exception(f"Failed finally after {times} tries") from e
                    logger.info(f"Retrying {fn}")

        return wrapped

    return wrapper


@retry(3)
def fetch_file(url, session=None):
    resp = (session and session.get or requests.get)(
        url, headers={"User-Agent": USER_AGENT}
    )
    assert len(resp.content) != 0, "Got empty file"
    if "Content-Length" in resp.headers:
        # https://blog.petrzemek.net/2018/04/22/on-incomplete-http-reads-and-the-requests-library-in-python/
        expected_size = int(resp.headers["Content-Length"])
        actual_size = resp.raw.tell()
        assert (
            expected_size == actual_size
        ), f"Incomplete download: {actual_size}/{expected_size}"
    return resp.content


@retry(3)
def fetch_volume(filename, image_urls):
    cached_path = BLOB_DIR / filename
    if cached_path.exists():
        with cached_path.open("rb") as f:
            blob = f.read()
            assert blob
            return blob
    session = requests.Session()  # <del>activate connection reuse</del>
    images = []
    for url in image_urls:
        logger.debug(f"Downloading {url}")
        images.append(fetch_file(url, session))
    blob = img2pdf.convert(images)
    with cached_path(cached_path, "wb") as f:
        f.write(blob)
    return blob


def main():
    with open(CONFIG_FILE_PATH, "r") as f:
        config = yaml.safe_load(f.read())

    username, password = config["username"], config["password"]
    site = mwclient.Site("commons.wikimedia.org")
    site.login(username, password)
    site.requests["timeout"] = 125
    site.chunk_size = 1024 * 1024 * 64

    logger.info(f"Signed in as {username}")

    def getopt(item, default=None):
        return config.get(item, config.get(item, default))

    book_paths = sorted(DATA_DIR.glob("*.json"))

    template = getopt("template")
    batch_link = getopt("link") or getopt("name")

    booknavi = getopt("booknavi")

    last_position = load_position("ynutcm")

    if last_position is not None:
        book_paths = iter(book_paths)
        logger.info(f"Last processed: {last_position}")
        next(
            itertools.dropwhile(
                lambda book_path: str(book_path) != last_position, book_paths
            )
        )  # lazy!
        # TODO: peek and report?

    failcnt = 0

    for path in book_paths:
        with open(path) as f:
            book = json.load(f)
        title = zhconv(book["detail"]["title"], "zh-hant")
        category_name = "Category:" + title
        byline = zhconv(book["detail"]["author"], "zh-hant")
        if getopt("apply_tortoise_shell_brackets_to_starting_of_byline", False):
            # e.g. "(魏)王弼,(晋)韩康伯撰   (唐)邢璹撰"
            byline = re.sub(
                r"^([（(〔[][题題][]）)〕])?[（(〔[](.{0,3}?)[]）)〕]",
                r"\1〔\2〕",
                byline,
            )

        def genvols():
            for ivol, (volume_name, volume_path, image_urls) in enumerate(
                map(lambda triple: triple.values(), book["fulltextpath"])
            ):
                volume_name = zhconv(volume_name, "zh-hant")
                # pagename = "File:" + book['name'] + ".pdf"
                # volume_name = f"第{ivol+1}冊" if len(volurls) > 1 else ""
                # volume_name_wps = (
                #    (" " + volume_name) if volume_name else ""
                # )  # with preceding space
                volume_name = volume_name.replace("(", "（").replace(")", "）")
                filename = f"YNUTCM-{volume_name}.pdf"
                pagename = "File:" + filename
                assert all(char not in set(r'["$*|\]</^>@#') for char in pagename)
                comment = f'Upload {title} {volume_name} ({1+ivol}/{len(book["fulltextpath"])}) by {book["detail"]["author"]} (batch task; ynutcm; {batch_link}; [[{category_name}|{title}]])'
                yield ivol + 1, filename, pagename, volume_name, volume_path, image_urls, comment

        volsit = peekable(genvols())
        prev_filename = None
        for (
            nth,
            pagename,
            filename,
            volume_name,
            volume_path,
            image_urls,
            comment,
        ) in volsit:
            try:
                next_filename = volsit.peek()[2]
            except StopIteration:
                next_filename = None
            additional_fields = "\n".join(
                f"  |JSONFIELD-{k}={zhconv(v, 'zh-hant') if k not in ('cover', 'fullTextPath') else v}"
                for k, v in book["detail"].items()
            )
            category_page = site.pages[category_name]
            # TODO: for now we do not create a seperated category suffixed with the edition
            if not category_page.exists:
                category_wikitext = (
                    """{{Wikidata Infobox}}
{{Category for book|zh}}
{{zh|%s}}

[[Category:Chinese-language books by title]]
    """
                    % title
                )
                category_page.edit(
                    category_wikitext,
                    f"Creating (batch task; ynutcm; {batch_link})",
                )
            volume_wikitext = f"""=={{{{int:filedesc}}}}==
{{{{{booknavi}|prev={prev_filename or ""}|next={next_filename or ""}|nth={nth}|total={len(book["fulltextpath"])}|number={book["detail"]["number"]}|totalnum={book["detail"]["totalnum"]}|callnum={book["detail"]["callnum"]}|docNo={book["detail"]["docNo"]}|class={zhconv(book["detail"]["class"], "zh-Hant")}}}}}
{{{{{template}
  |volname={volume_name}
  |volpath={volume_path}
{additional_fields}
}}}}

[[{category_name}]]
    """
            print(volume_wikitext)
            page = site.pages[pagename]
            try:
                if not page.exists:
                    logger.info(f"Downloading images for {pagename}")
                    binary = fetch_volume(filename, image_urls)
                    logger.info(f"Uploading {pagename} ({len(binary)} B)")

                    @retry()
                    def do1():
                        r = site.upload(
                            BytesIO(binary),
                            filename=filename,
                            description=volume_wikitext,
                            comment=comment,
                        )
                        assert (r or {}).get("result", {}) == "Success" or (
                            r or {}
                        ).get("warnings", {}).get("exists"), f"Upload failed {r}"

                    do1()
                else:
                    if getopt("skip_on_existing", False):
                        logger.debug(f"{pagename} exists, skipping")
                    else:
                        logger.info(f"{pagename} exists, updating wikitext")

                        @retry()
                        def do2():
                            r = page.edit(
                                volume_wikitext, comment + " (Updating metadata)"
                            )
                            assert (r or {}).get(
                                "result", {}
                            ) == "Success", f"Update failed {r}"

                        do2()
            except Exception as e:
                failcnt += 1
                logger.warning("Upload failed", exc_info=e)
                if not getopt("skip_on_failures", False):
                    raise e
            prev_filename = filename
        input("Press any key to continue")
        store_position("ynutcm", str(path))
    # logger.info(f"Batch done with {failcnt} failures.")


if __name__ == "__main__":
    main()
