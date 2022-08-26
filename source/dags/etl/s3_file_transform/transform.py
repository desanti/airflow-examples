#!/usr/bin/env python3

import sys
import logging

from data_cleaning import data_cleaning


arg_list = ["script", "input", "output", "job_title_filter"]
args = dict(zip(arg_list, sys.argv))


logging.info("Starting data cleaning...")

data_cleaning(args.get("input"), args.get("output"), args.get("job_title_filter"))

logging.info("Completed data cleaning!")
