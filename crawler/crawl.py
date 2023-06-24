#!/usr/bin/env python3
import json
import urllib.parse
from pathlib import Path
import logging
import functools
import sys

import requests
from bs4 import BeautifulSoup

OUTPUT_DIR = Path(__file__).parent / "data"

USER_AGENT = "ynutcmpdbot/0.0"

# Data model: curl --output - 'http://guji.ynutcm.edu.cn/Yngj/Data/Pager?channelId=&channelUnikey=gj&isDes=true' -X POST -H 'Content-Length: 0'

URL_PAGER = "http://guji.ynutcm.edu.cn/Yngj/Adm/Data/Pager?engine=solr"
URL_BOOK_DETAIL = "http://guji.ynutcm.edu.cn/Yngj/Adm/Data/Detail_gj"
URL_VOLUME_VIEW = "http://guji.ynutcm.edu.cn/Yngj/Data/PicView"

CHANNEL_ID = 3123
PAGE_SIZE = 12

LOGLEVEL = os.environ.get("LOGLEVEL", "INFO").upper()
logging.basicConfig(level=LOGLEVEL)
logger = logging.getLogger(__name__)


def retry(times=3):
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
                    logger.info(f"Retrying {fn} {tried}/{times} due to {e}", exc_info=e)

        return wrapped

    return wrapper


requests.get = retry(5)(requests.get)
requests.post = retry(5)(requests.post)


def books(start_page=1):
    logger.info(f"Starting from page {start_page}")
    page = total_page = start_page
    while page <= total_page:
        logger.info(f"Retrieving page {page}/{total_page}")
        d = {
            "PageSize": PAGE_SIZE,
            "Page": page,
            "SortBy": "UpdateTime",
            "SortOrder": "desc",
            "IsHighlight": True,
            "query[GroupType]": "channel",
            "query[GroupId]": CHANNEL_ID,
            "query[isWork]": True,
        }
        resp = requests.post(URL_PAGER, data=d, headers={"User-Agent": USER_AGENT})
        d = resp.json()
        total_page = d["TotalPages"]
        for row in d["Rows"]:
            yield row
        page += 1


def images(number, totalnum, title, path):
    logger.info(f"Retrieving images for {path}")
    params = {"number": number, "totalnum": totalnum, "title": title, "path": path}
    resp = requests.get(
        URL_VOLUME_VIEW, params=params, headers={"User-Agent": USER_AGENT}
    )
    html = BeautifulSoup(resp.content, features="lxml")
    for img in html.select("#galley li img"):
        yield urllib.parse.urljoin(URL_VOLUME_VIEW, img.attrs["src"].replace("\\", "/"))


def book_detail(book_id):
    logger.info(f"Retrieving book {book_id}")
    d = {"id": book_id, "isView": True}
    resp = requests.post(URL_BOOK_DETAIL, data=d, headers={"User-Agent": USER_AGENT})
    d = resp.json()
    return d


def volumes(book):
    for vol in book["fulltextpath"]:
        yield images(
            book["detail"]["number"],
            book["detail"]["totalnum"],
            book["detail"]["title"],
            vol["tpath"],
        )


def main():
    start_page = int(sys.argv[1]) if len(sys.argv) > 1 else 1

    if not OUTPUT_DIR.exists():
        logger.warn(f"Output directory {OUTPUT_DIR} does not exist, creating...")
        OUTPUT_DIR.mkdir()

    for book in books():
        book_id = book["Id"]
        book = book_detail(book_id)
        for entry, imageset in zip(book["fulltextpath"], volumes(book)):
            entry["IMAGES"] = list(imageset)
            if not entry["IMAGES"]:
                logger.warning(f"No image found in a volume of {book_id}")
        with open(OUTPUT_DIR / f"{book_id}.json", "w") as f:
            json.dump(book, f, indent=2, ensure_ascii=False)


if __name__ == "__main__":
    main()
