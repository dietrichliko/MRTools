#!/usr/bin/env python

empty_config_data = {
    "binaries": {},
    "site": {
        "cern": {},
        "clip": {},
    },
    "samples_cache": {},
}

print(empty_config_data | {"site": "test"})
