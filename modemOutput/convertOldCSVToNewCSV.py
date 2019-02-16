#!/usr/bin/env python3
"""
A script for one-time use to convert the old CSV of modemOutput data
to a new format, with more columns.
"""
import csv
from collections import OrderedDict

import pandas as pd

testCSV_old_file_name = "/home/ed/Documents/CableModemStats/testCSV.oldformat.csv"
testCSV_old_conv_file_name = "/home/ed/Documents/CableModemStats/testCSV.oldformat.converted.csv"

downstream_columns = [
    "Downstream: " + col_group + " " + let for col_group in [
        "Channel ID",
        "Frequency",
        "Signal to Noise Ratio",
        "Downstream Modulation",
        "Power Level",
    ] for let in [
        "A",
        "B",
        "C",
        "D",
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
    "Signal Stat: " + col_group + " " + let for col_group in [
        "Channel ID",
        "Total Unerrored Codewords",
        "Total Correctable Codewords",
        "Total Uncorrectable Codewords",
    ] for let in [
        "A",
        "B",
        "C",
        "D",
    ]
]

test_df = pd.read_csv(testCSV_old_file_name, dtype=str, quotechar='\0',keep_default_na=False, header=None, low_memory=False)

# Columns are named numerically by default. 

# Gather columns 26-29 (inclusive) into a single column
test_df["Upstream_Modulation_A"] = test_df.loc[:,26:29].apply(" ".join, axis=1)

# Now give each column a meaningful name
column_renaming_mapping = {0: "timestamp"}
#   Downstream columns are columns 1-20
column_renaming_mapping.update(
    {k+1:v for (k, v) in enumerate(downstream_columns)}
)

#   Upstream columns are columns 21-30, but 26-29 will be merged
#   Columns 21-25 before UpstreamModulation
column_renaming_mapping.update(
    {k+21:v for (k, v) in enumerate(upstream_columns) if k < 5}
)
#   UpstreamModulation column and 1 column after
column_renaming_mapping.update(
    OrderedDict([
        ("Upstream_Modulation_A", "Upstream: Upstream Modulation A"),
        (30, "Upstream: Ranging Status A"),
    ])
)

#   Signal Stat Columns are columns 31-46
column_renaming_mapping.update(
    {k+31:v for (k, v) in enumerate(signal_stat_columns)}
)

test_df.rename(columns=column_renaming_mapping, inplace=True)

# Reindex too add missing columns
test_df = test_df.reindex(
    columns=["timestamp"]
            +downstream_columns
            +upstream_columns
            +signal_stat_columns
)

test_df.to_csv(testCSV_old_conv_file_name, mode="w", index=False, header=False, quoting=csv.QUOTE_NONE, quotechar="",  escapechar="\\", na_rep="n/a")

quit()

