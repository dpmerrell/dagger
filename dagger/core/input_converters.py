"""
    core/input_converters.py
    (c) David Merrell 2026

    Register input converters for the core Datum types
    (MemoryDatum, FileDatum) and the core InputForm values
    (OBJECT, FILEPATH).
"""

from dagger.abstract.input_converters import InputForm, converter_registry
from dagger.core.datum import MemoryDatum, FileDatum

from pathlib import Path
import pickle


def memory_to_object(datum):
    """Convert a MemoryDatum to a Python object."""
    return datum.pointer


def file_to_object(datum):
    """
    Convert a FileDatum to a Python object.

    If the file is a .pkl file, deserialize it.
    Otherwise, return the filepath string.
    """
    if Path(datum.pointer).suffix.lower() == ".pkl":
        with open(datum.pointer, "rb") as f:
            return pickle.load(f)
    return datum.pointer


def file_to_filepath(datum):
    """Convert a FileDatum to a filepath string."""
    return datum.pointer


converter_registry.register(MemoryDatum, InputForm.OBJECT, memory_to_object)
converter_registry.register(FileDatum, InputForm.OBJECT, file_to_object)
converter_registry.register(FileDatum, InputForm.FILEPATH, file_to_filepath)
