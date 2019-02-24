#!/usr/bin/env python3
"""
A script to take files that are currently on disk, extract the relevant
content, and append as new rows in a CSV. After successful extraction,
the files are then removed.
"""

# This script successfully handles both old and new *.htm file formats
# and appends their contents to OrderedDicts of lists. Once these lists
# reach 1,000 elements each, the OrderedDict is converted to a pandas
# DataFrame (shape: (1000, 65)). This is then converted to CSV and
# appended to the pre-existing CSV.
#
# NOTE: changes to versions in pandas, lxml, etc may cause changes in
# the behavior of pd.read_html(). Be sure to sync a new virtualenv to
# the included Pipfile.loc before using.

import csv
import os
import re
import shutil

from collections import OrderedDict
from glob import glob

import pandas as pd

full_df_filename = (
    "/home/ed/Documents/CableModemStats/modemOutput/modemData.converted.csv"
)

modem_files = glob("/home/ed/Documents/CableModemStats/modemOutput/*.htm")
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
    for i in range(1, 5):
        downstream_df.iloc[5, i] = downstream_df.iloc[5, i + 1]

    downstream_values = downstream_df.iloc[1:6, 1:5].values.flatten()

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

    upstream_values = upstream_df.iloc[1:, 1:].values.flatten("F")

    # Remove units by dropping anything after a space
    # Avoid dropping anything from the Upstream Modulation row, though.
    upstream_values = [
        val.split(" ") if "QAM" not in val else [val]
        for val in upstream_values
    ]
    upstream_values = [item for sublist in upstream_values for item in sublist]
    upstream_values = [
        val for val in upstream_values if val not in ["Hz", "Msym/sec", "dBmV"]
    ]

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

    signal_stats_values = (
        signal_stats_df.iloc[1:, 1:].astype(int).values.flatten()
    )

    return signal_stats_values


downstream_columns = [
    "Downstream: " + col_group + " " + let
    for col_group in [
        "Channel ID",
        "Frequency",
        "Signal to Noise Ratio",
        "Downstream Modulation",
        "Power Level",
    ]
    for let in ["A", "B", "C", "D"]
]

upstream_columns = [
    "Upstream: " + col_group + " " + let
    for let in ["A", "B", "C", "D"]
    for col_group in [
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
    "Signal Stat: " + col_group + " " + let
    for col_group in [
        "Channel ID",
        "Total Unerrored Codewords",
        "Total Correctable Codewords",
        "Total Uncorrectable Codewords",
    ]
    for let in ["A", "B", "C", "D"]
]

# Do something where I chunk by 1000 files at a time, for example.
chunk_size = 1_000
for chunk_start in range(0, len(modem_files), chunk_size):
    new_data = OrderedDict()
    for column_name in (
        ["timestamp"]
        + downstream_columns
        + upstream_columns
        + signal_stat_columns
    ):
        new_data[column_name] = []

    file_names_ready_for_removal = []
    for file_name in modem_files[chunk_start : chunk_start + chunk_size]:
        try:
            with open(file_name, "r") as f:
                df_list = pd.read_html(f)
        except Exception as e:
            print(f"WARN: Failed to parse {file_name}")
            print(e)
            continue

        if list(map(lambda x: x.shape, df_list)) not in [
            [(8, 6), (1, 1), (8, 2), (5, 5)],  # Old style htm files
            [(8, 6), (1, 1), (8, 5), (5, 5)],  # New style htm files
        ]:
            # df_list is not parseable
            continue

        downstream_df = df_list[0]
        downstream_row = convert_downstream_df_to_list_of_values(downstream_df)

        upstream_df = df_list[2]
        upstream_row = convert_upstream_df_to_list_of_values(upstream_df)
        if len(upstream_row) == 7:
            upstream_row.extend([None] * 21)

        signal_stats_df = df_list[3]
        signal_stats_row = convert_signal_stats_df_to_list_of_values(
            signal_stats_df
        )

        new_data["timestamp"].append(get_timestamp_from_filename(file_name))
        for (val, name) in zip(downstream_row, downstream_columns):
            new_data[name].append(val)

        for (val, name) in zip(upstream_row, upstream_columns):
            new_data[name].append(val)

        for (val, name) in zip(signal_stats_row, signal_stat_columns):
            new_data[name].append(val)

        file_names_ready_for_removal.append(file_name)

    chunk_df = pd.DataFrame(new_data)
    chunk_df.to_csv(
        full_df_filename,
        mode="a",
        index=False,
        header=False,
        quoting=csv.QUOTE_NONE,
        quotechar="",
        escapechar="\\",
        na_rep="n/a",
    )

    for rm_file in file_names_ready_for_removal:
        os.remove(rm_file)

quit()
