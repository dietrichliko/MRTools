#!/usr/bin/env python

import os
import fnmatch
import pathlib
import concurrent.futures as futures

import ROOT  # type: ignore

SAMPLES_PATH = pathlib.Path("/eos/vbc/experiments/cms/store/user/liko/CompStop/")
URL_PREFIX = "root://eos.grid.vbc.ac.at/"


def get_entries(url: str) -> int:
    tfile = ROOT.TFile(url)
    tree = tfile.Get("Events")
    return tree.GetEntries()


def main():

    urls = []
    for name in os.listdir(SAMPLES_PATH):
        if not fnmatch.fnmatch(name, "*-006-nanoAOD"):
            continue
        for root, _dirnames, filenames in os.walk(SAMPLES_PATH / name):
            for name in filenames:
                urls.append(URL_PREFIX + os.path.join(root, name))

    with futures.ThreadPoolExecutor(max_workers=4) as executor:
        f_to_url = {executor.submit(get_entries, url): url for url in urls}

        for f in futures.as_completed(f_to_url):
            url = f_to_url[f]
            print(f"{f.result()} - {url}")


if __name__ == "__main__":
    main()
