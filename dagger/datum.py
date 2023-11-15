"""
    datum.py
    David Merrell (c) 2023

    A Datum is used to represent an individual input or output.
    Inheritors may wrap, e.g., file paths, URLs, or 
    arbitrary python objects.

    A Datum must have the following attributes:
        * uid (a unique identifier)
        * (whatever information is necessary to 
           represent or locate the data.
           E.g., a file path. Tasks will use this
           to access the data as input.)
"""


