#!/usr/bin/env python3
import yaml
import os
import re
import json
import sys
import functools
from pathlib import Path
from zhconv_rs import zhconv

import mwclient

CONFIG_FILE_PATH = os.path.join(os.path.dirname(__file__), "config.yml")
DATA_DIR = Path(__file__).parent / "../crawler/data"


def main():
    with open(CONFIG_FILE_PATH, "r") as f:
        config = yaml.safe_load(f.read())

    def getopt(item, default=None):  # get batch config or fallback to global config
        return config.get(item, config.get(item, default))

    template = "Template:" + getopt("template")
    batch_link = getopt("link") or getopt("name")
    category_name = re.search(r"(Category:.+?)[]|]", batch_link).group(1)

    book_paths = sorted(DATA_DIR.glob("*.json"))
    books = []
    for book_path in book_paths:
        with open(book_path) as f:
            book = json.load(f)
            books.append(book)

    lines = [
        f"Category: {batch_link}, Template: {{{{Template|{template}}}}}, Books: {len(books)}, Files: {sum(map(lambda e: len(e['fulltextpath']), books))}\n",
    ]

    for book in books:
        title = zhconv(book["detail"]["title"] or "", "zh-hant")
        byline = zhconv(book["detail"]["author"] or "", "zh-hant")
        lines.append(f"* 《{title}》 {byline}")

        for ivol, (volume_name, volume_path, image_urls) in enumerate(
            map(lambda triple: triple.values(), book["fulltextpath"])
        ):
            # pagename = "File:" + book['name'] + ".pdf"
            # volume_name = f"第{ivol+1}冊" if len(volurls) > 1 else ""
            # volume_name_wps = (
            #    (" " + volume_name) if volume_name else ""
            # )  # with preceding space
            # filename = f'{book["name"]}{volume_name_wps}.pdf'
            volume_name = zhconv(
                volume_name.strip().replace("(", "（").replace(")", "）"), "zh-hant"
            )
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

            filename = f"YNUTCM-{volume_name}.pdf"
            pagename = "File:" + filename
            assert all(char not in set(r'["$*|\]</^>@#') for char in pagename)
            comment = f'Upload {title} {volume_name} ({1+ivol}/{len(book["fulltextpath"])}) by {byline} (batch task; ynutcm; {batch_link}; [[{category_name}|{title}]])'
            lines.append(f"** [[:{pagename}]]")

    lines.append("")
    lines.append("[[" + category_name + "]]")
    lines.append("")

    if len(sys.argv) < 2:
        print("\n".join(lines))
    else:
        print(f"Writing file list")
        pagename = sys.argv[1]

        username, password = config["username"], config["password"]
        site = mwclient.Site("commons.wikimedia.org")
        site.login(username, password)
        site.requests["timeout"] = 125
        site.chunk_size = 1024 * 1024 * 64

        site.pages[pagename].edit("\n".join(lines), f"Writing file list to {pagename}")


if __name__ == "__main__":
    main()
