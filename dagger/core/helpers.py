"""
    core/helpers.py
    (c) David Merrell 2026

    Helper functions for the core workflow implementation.
"""

from dagger.core.datum import MemoryDatum, FileDatum
from dagger.core.task import FunctionTask, PklTask, ScriptTask
from pathvalidate import is_valid_filepath
from pathlib import Path
import platform
import pickle

#########################################
# Task helpers
#########################################
