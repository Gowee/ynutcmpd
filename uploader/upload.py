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
from pywikibot import Site, Page, FilePage
from zhconv_rs import zhconv as zhconv_

zhconv = lambda s, v: zhconv_(s, v) if type(s) == str else s
import img2pdf


# from getbook import getbook

CONFIG_FILE_PATH = os.path.join(os.path.dirname(__file__), "config.yml")
POSITION_FILE_PATH = os.path.join(os.path.dirname(__file__), ".position")
DATA_DIR = Path(__file__).parent / "../crawler/data"
BLOB_DIR = Path(__file__).parent / "blobs"
CACHE_FILE_PATH = Path(__file__).parent / ".cache.pdf"
RETRY_TIMES = 3
CHUNK_SIZE = 4 * 1024 * 1024
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
                    logger.warning(f"Retrying {fn}", exc_info=e)

        return wrapped

    return wrapper


@retry(7)
def fetch_file(url, session=None):
    resp = (session and session.get or requests.get)(
        url, headers={"User-Agent": USER_AGENT}
    )
    resp.raise_for_status()
    assert len(resp.content) != 0, "Got empty file"
    if "Content-Length" in resp.headers:
        # https://blog.petrzemek.net/2018/04/22/on-incomplete-http-reads-and-the-requests-library-in-python/
        expected_size = int(resp.headers["Content-Length"])
        actual_size = resp.raw.tell()
        assert (
            expected_size == actual_size
        ), f"Incomplete download: {actual_size}/{expected_size}"
    return resp.content

def construct_failure_page(url, page_name=""):
    qr = QRCode(box_size=3)
    qr.add_data(url)
    qr.make()
    qr_img = qr.make_image()

    # ref: https://stackoverflow.com/a/1970930/5488616
    #      https://stackoverflow.com/a/68648910/5488616
    #      ChatGPT
    
    font = ImageFont.truetype(str(FONT_FILE_PATH), size=20)

    t = datetime.datetime.now(timezone.utc)
    assert t.tzinfo == timezone.utc
    if page_name:
        page_name = " " + page_name
    texts = [f"The page{page_name} links to an broken url:", *textwrap.wrap(urlquote(url, safe=":/"), break_on_hyphens=False), "Access time: " + str(t)]

    margin = (5, 5)
    spacing = 3 
    mask_images = [font.getmask(text, "L") for text in texts]
    width = max(max(mask_image.size[0] for mask_image in mask_images) ,  qr_img.size[0]) + margin[0] * 2
    height = sum(mask_image.size[1] for mask_image in mask_images) + margin[1] * 2+ (len(mask_images) - 1) * spacing + spacing + qr_img.size[1]
    # mask_image = font.getmask(text, "L")
    img = Image.new("RGB", (width, height), (255,255,255))
    y = margin[1]
    for mask_image in mask_images:
        # need to use the inner `img.im.paste` due to `getmask` returning a core
        img.im.paste((0,0,0), (margin[0], y, margin[0] + mask_image.size[0], y+mask_image.size[1]), mask_image)
        y += mask_image.size[1] + spacing
    img.paste(qr_img, (0, y))

    output = BytesIO()
    img.save(output, format="JPEG")
    return output.getvalue()

@retry(3)
def fetch_volume(filename, image_urls):
    #return CACHE_FILE_PATH
    # cached_path = BLOB_DIR / filename
    # if cached_path.exists():
    #     with cached_path.open("rb") as f:
    #         blob = f.read()
    #         assert blob
    #         return blob
    session = requests.Session()  # <del>activate connection reuse</del>
    images = []
    for i, url in enumerate(image_urls):
        if url.endswith(".db"):
            continue
        logger.debug(f"Downloading {url}")
        assert url.endswith(".jpg"), "Expected JPG: " + url
        try:
            image = fetch_file(url, session)
        except Exception as e:
            logger.warning(f"Failed to download {url}, using placeholder image", exc_info=e)
            image = construct_failure_page(url, page_name=f"({i+1}/{len(image_urls)})")
            failures += 1
        images.append(image)
    blob = img2pdf.convert(images)
    # with cached_path.open("wb") as f:
    #     f.write(blob)
    logger.info(f"PDF constructed for {filename} ({len(blob)} B)")
    with CACHE_FILE_PATH.open("wb") as f:
        f.write(blob)
    return CACHE_FILE_PATH


def main():
    with open(CONFIG_FILE_PATH, "r") as f:
        config = yaml.safe_load(f.read())

    username, password = config["username"], config["password"]
    site = Site("commons")
    site.login()
    # site.login(username, password)
    # site.requests["timeout"] = 125
    # site.chunk_size = 1024 * 1024 * 64

    # logger.info(f"Signed in as {username}")
    logger.info("Up")

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
        urlid = int(path.stem)
        title = zhconv(book["detail"]["title"], "zh-hant")
        category_name = "Category:" + title
        byline = zhconv(book["detail"]["author"] or "", "zh-hant")
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
                # pagename = "File:" + book['name'] + ".pdf"
                # volume_name = f"第{ivol+1}冊" if len(volurls) > 1 else ""
                # volume_name_wps = (
                #    (" " + volume_name) if volume_name else ""
                # )  # with preceding space
                volume_name = re.sub(
                    r"\s+",
                    " ",
                    (
                        volume_name.strip()
                        .replace("(", "（")
                        .replace(")", "）")
                        .replace("{", "（")
                        .replace("}", "）")
                    ),
                )
                volume_name_simplified = re.sub(
                    r"[0-9-]+(.+?)[0-9-]+$", r"\1", volume_name
                )
                filename = zhconv(f"YNUTCM-{volume_name}.pdf", "zh-hant")
                pagename = "File:" + filename
                assert all(char not in set(r'["$*|\]</^>@#') for char in pagename)
                comment = f'Upload {title} {volume_name} ({1+ivol}/{len(book["fulltextpath"])}) by {book["detail"]["author"]} (batch task; ynutcm; {batch_link}; [[{category_name}|{title}]])'
                yield ivol + 1, filename, pagename, byline, volume_name, volume_path, volume_name_simplified, image_urls, comment

        volsit = peekable(genvols())
        prev_filename = None
        for (
            nth,
            filename,
            pagename,
            byline,
            volume_name,
            volume_path,
            volume_name_simplified,
            image_urls,
            comment,
        ) in volsit:
            try:
                next_filename = volsit.peek()[1]
            except StopIteration:
                next_filename = None
            additional_fields = "\n".join(
                [
                    f"  |JSONFIELD-{k}={zhconv('' if v is None else v, 'zh-hant')}"
                    for k, v in book["detail"].items()
                ]
                + [
                    f"  |JSONFIELD-{k}-original={'' if v is None else v}"
                    for k, v in book["detail"].items()
                ]
            )
            category_page = Page(site, category_name)
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
                category_page.text = category_wikitext
                category_page.save(
                    f"Creating (batch task; ynutcm; {batch_link})",
                )
            volume_wikitext = f"""=={{{{int:filedesc}}}}==
{{{{{booknavi}|prev={prev_filename or ""}|next={next_filename or ""}|nth={nth}|total={len(book["fulltextpath"])}|number={book["detail"]["number"]}|totalnum={book["detail"]["totalnum"]}|callnum={book["detail"]["callnum"]}|docNo={book["detail"]["docNo"]}|class={zhconv(book["detail"]["class"], "zh-Hant")}}}}}
{{{{{template}
  |bookurlid={urlid}
  |volname={zhconv(volume_name, 'zh-hant')}
  |volname-original={volume_name}
  |volpath={zhconv(volume_path, 'zh-hant')}
  |volpath-original={volume_path}
  |simpvolname={zhconv(volume_name_simplified, 'zh-hant')}
  |byline={byline}
{additional_fields}
}}}}

[[{category_name}]]
    """
            # print(volume_wikitext)
            if not image_urls:
                logger.warning(f"No images for {pagename}!")
                continue
            page = FilePage(site, pagename)
            try:
                if not page.exists():
                    logger.info(f"Downloading images for {pagename}")
                    binary = fetch_volume(filename, image_urls)
                    logger.info(f"Uploading {pagename}")

                    @retry()
                    def do1():
                        r = site.upload(
                            source_filename=binary,
                            filepage=page,
                            text=volume_wikitext,
                            comment=comment,
                            asynchronous=True,
                            chunk_size=CHUNK_SIZE,
                            ignore_warnings=["was-deleted"],
                            # report_success=True,
                        )
                        assert r, "Upload failed"
                        # assert (
                        #     r.get("result") or r.get("upload", {}).get("result")
                        # ) == "Success" or (r or {}).get("warnings", {}).get(
                        #     "exists"
                        # ), f"Upload failed {r}"

                    do1()
                else:
                    if getopt("skip_on_existing", False):
                        logger.debug(f"{pagename} exists, skipping")
                    else:
                        logger.info(f"{pagename} exists, updating wikitext")

                        @retry()
                        def do2():
                            page.text = volume_wikitext
                            r = page.save(comment + " (Updating metadata)")
                            # assert (r or {}).get(
                            #     "result", {}
                            # ) == "Success", f"Update failed {r}"
                            assert r, f"Update failed {repr(r)}"

                        do2()
            except Exception as e:
                failcnt += 1
                logger.warning("Upload failed", exc_info=e)
                if not getopt("skip_on_failures", False):
                    raise e
            prev_filename = filename
        # input("Press any key to continue")
        store_position("ynutcm", str(path))
    # logger.info(f"Batch done with {failcnt} failures.")


if __name__ == "__main__":
    main()
