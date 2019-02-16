#!/usr/bin/env python3
"""
A script to take files that are currently on disk, extract the relevant
content, and append as new rows in a CSV.
"""

# This script successfully handles both old and new *.htm file formats
# and appends their contents to OrderedDicts of lists. These OrderedDicts
# can be directly converted to a single CSV by pandas, which can output
# to a CSV file.
#
# TODO: After I write a convertOldCSVToNewCSV.py script, and convert the
# already collected data (approx 1.38MM rows) into this new format, I'd
# like to switch the data collection/normalization cron-job to use this
# script.
# Before the transition can occur, I'll need to run a batch job to process
# the backlog of new format files that never got appended to the
# pre-existing dataset. That will require a chunked version of this script.
#
# TODO: Is the order of column names correct? Seems like its transposed
# for downstream columns at least

import csv
import os
import re
import shutil

from collections import OrderedDict
from glob import iglob

import pandas as pd

OLD_STYLE_FILE = "/home/ed/CMS_data_backup/1464220921.htm"
NEW_STYLE_FILE = "/home/ed/Documents/CableModemStats/modemOutput/1537722781.htm"
modem_files = [OLD_STYLE_FILE] + [NEW_STYLE_FILE]
# modem_files = [NEW_STYLE_FILE]


full_df_filename = "/home/ed/Documents/CableModemStats/modemOutput/testData.csv"
shutil.copyfile(full_df_filename+"_bkup", full_df_filename)

# full_df_filename = "/home/ed/Documents/CableModemStats/modemOutput/testData.csv"
# full_df = pd.read_csv(full_df_filename, quotechar='\0',keep_default_na=False, header=None, low_memory=False)

# modem_files = iglob("/home/ed/Documents/CableModemStats/modemOutput/*.htm")
timestamp_re = re.compile(r"(\d+)")

files_to_remove = []

def get_timestamp_from_filename(filename):
    return timestamp_re.findall(filename)[-1]


def convert_downstream_df_to_list_of_values(downstream_df):
    """
    Both the new and old format downstream values look very similar. They are,
    essentially, 5x4 DataFrames. A small wrinkle is introduced by the HTML
    formatting, which this function handles.

    Args:
        downstream_df [pd.DataFrame]:
            A DataFrame taken directly from a call to pd.read_html(filename)[0]

    Returns:
        A list of 20 values. 
            Units on physical measurements are stripped -- e.g., 37 dB --> 37
    """
    # Fix the offset in the 6th row of the raw dataframe
    for i in range(1,5):
        downstream_df.iloc[5,i] = downstream_df.iloc[5,i+1]

    downstream_values = downstream_df.iloc[1:6,1:5].values.flatten()

    # Remove units by dropping anything after a space
    downstream_values = [val.split(" ")[0] for val in downstream_values]

    return downstream_values


def convert_upstream_df_to_list_of_values(upstream_df):
    """
    The new and old format upstream values are quite different. The old format
    is a 8x2 DataFrame, with 7 values of interest to us that get stored in 7
    columns of the output CSV. (The original CSV format used 10 columns by
    splitting up the Upstream Modulation row.) The new format is an 8x5 
    DataFrame, with 28 values of interest to be stored in 28 columns.

    Just as in the downstream case, physical units are stripped from values.

    Args:
        upstream_df [pd.DataFrame]:
            A DataFrame taken directly from a call to pd.read_html(filename)[2]

    Returns:
        A list of either 7 or 28 values. 
            Units on physical measurements are stripped -- e.g., 37 dB --> 37
            Upstream Modulation is split into substrings by whitespace.
    """

    upstream_values = upstream_df.iloc[1:,1:].values.flatten("F")

    # Remove units by dropping anything after a space
    # Avoid dropping anything from the Upstream Modulation row, though.
    upstream_values = [val.split(" ") if "QAM" not in val else [val] for val in upstream_values]
    upstream_values = [item for sublist in upstream_values for item in sublist]
    upstream_values = [val for val in upstream_values if val not in ["Hz", "Msym/sec", "dBmV"]]

    return upstream_values


def convert_signal_stats_df_to_list_of_values(signal_stats_df):
    """
    Both the new and old format signal_stats values look very similar. They are
    4x4 DataFrames.

    Args:
        signal_stats_df [pd.DataFrame]:
            A DataFrame taken directly from a call to pd.read_html(filename)[3]

    Returns:
        A list of 16 values. 
    """
    
    signal_stats_values = signal_stats_df.iloc[1:,1:].astype(int).values.flatten()

    return signal_stats_values


downstream_columns = [ 
    "Downstream: " + col_group + " " + let for let in [
        "A", 
        "B", 
        "C", 
        "D",
    ] for col_group in [
        "Channel ID", 
        "Frequency",
        "Signal to Noise Ratio",
        "Downstream Modulation",
        "Power Level",
    ]
]

upstream_columns = [ 
    "Upstream: " + col_group + " " + let for let in [
        "A", 
        "B", 
        "C", 
        "D",
    ] for col_group in [
        "Channel ID", 
        "Frequency",
        "Ranging Service ID",
        "Symbol Rate",
        "Power Level",
        "Upstream Modulation",
        "Ranging Status",
    ]
]

signal_stat_columns = [ 
    "Signal Stat: " + col_group + " " + let for let in [
        "A", 
        "B", 
        "C", 
        "D",
    ] for col_group in [
        "Channel ID", 
        "Total Unerrored Codewords",
        "Total Correctable Codewords",
        "Total Uncorrectable Codewords",
    ]
]

new_data = OrderedDict()
for column_name in (
        ["timestamp"] + 
        downstream_columns + 
        upstream_columns + 
        signal_stat_columns
):
    new_data[column_name] = []


for i, filename in enumerate(modem_files):
    try:
        with open(filename, "r") as f:
            df_list = pd.read_html(f)
    except Exception as e:
        print("Failed to parse {}: filename".format(filename))
        print(e)
        continue

    if list(map(lambda x: x.shape, df_list)) not in [
        [(7, 6), (1, 1), (8, 2), (5, 5)],  # Old style htm files
        [(7, 6), (1, 1), (8, 5), (5, 5)],  # New style htm files
    ]:
        # df_list is not parseable
        continue

    downstream_df = df_list[0]
    downstream_row = convert_downstream_df_to_list_of_values(downstream_df)

    upstream_df = df_list[2]
    upstream_row = convert_upstream_df_to_list_of_values(upstream_df)
    if len(upstream_row) == 7:
        upstream_row.extend([None]*21)

    signal_stats_df = df_list[3]
    signal_stats_row = convert_signal_stats_df_to_list_of_values(signal_stats_df)

    new_data["timestamp"].append(get_timestamp_from_filename(filename))
    for (val, name) in zip(downstream_row, downstream_columns):
        new_data[name].append(val)

    for (val, name) in zip(upstream_row, upstream_columns):
        new_data[name].append(val)

    for (val, name) in zip(signal_stats_row, signal_stat_columns):
        new_data[name].append(val)

# Would be nice to build the DataFrame only from new files, and only append at the *very* end
full_df = pd.DataFrame(new_data)
full_df.to_csv(full_df_filename, mode="a", index=False, header=False, quoting=csv.QUOTE_NONE, quotechar="",  escapechar="\\", na_rep="n/a")

    # full_df = full_df.append(pd.Series(row, index=list(range(len(row)))), ignore_index=True)
    # files_to_remove.append(filename)
    # if (i+1) % 10 == 0:
    #     quit()
    #     # for rm_file in files_to_remove:
    #     #     os.remove(rm_file)
    #     files_to_remove = []

quit()
