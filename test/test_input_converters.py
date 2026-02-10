"""
    Tests for the input converter registry.
"""

import pickle
import pytest
from pathlib import Path

from dagger.abstract.input_converters import (
    InputForm, ConverterRegistry, converter_registry
)
from dagger.core.datum import MemoryDatum, FileDatum
from dagger.abstract.datum import AbstractDatum


# ====================================
# Tests for ConverterRegistry mechanics
# ====================================

def test_registry_register_and_convert():
    reg = ConverterRegistry()
    reg.register(MemoryDatum, InputForm.OBJECT, lambda d: d.pointer)
    datum = MemoryDatum(pointer=42)
    assert reg.convert(datum, InputForm.OBJECT) == 42


def test_registry_has_converter():
    reg = ConverterRegistry()
    assert not reg.has_converter(MemoryDatum, InputForm.OBJECT)
    reg.register(MemoryDatum, InputForm.OBJECT, lambda d: d.pointer)
    assert reg.has_converter(MemoryDatum, InputForm.OBJECT)


def test_registry_missing_converter_raises():
    reg = ConverterRegistry()
    datum = MemoryDatum(pointer=42)
    with pytest.raises(TypeError, match="No converter registered"):
        reg.convert(datum, InputForm.FILEPATH)


def test_registry_mro_fallback():
    """A converter registered for a base class serves subclasses."""
    reg = ConverterRegistry()
    reg.register(AbstractDatum, InputForm.OBJECT, lambda d: "fallback")
    datum = MemoryDatum(pointer=42)
    assert reg.convert(datum, InputForm.OBJECT) == "fallback"


def test_registry_specific_overrides_base():
    """A more specific registration takes priority over base class."""
    reg = ConverterRegistry()
    reg.register(AbstractDatum, InputForm.OBJECT, lambda d: "base")
    reg.register(MemoryDatum, InputForm.OBJECT, lambda d: "specific")
    datum = MemoryDatum(pointer=42)
    assert reg.convert(datum, InputForm.OBJECT) == "specific"


# ====================================
# Tests for core-registered converters
# ====================================

def test_memory_to_object():
    datum = MemoryDatum(pointer={"key": "value"})
    result = converter_registry.convert(datum, InputForm.OBJECT)
    assert result == {"key": "value"}


def test_file_to_filepath(tmp_path):
    fpath = str(tmp_path / "test.txt")
    Path(fpath).write_text("hello")
    datum = FileDatum(pointer=fpath)
    result = converter_registry.convert(datum, InputForm.FILEPATH)
    assert result == fpath


def test_file_pkl_to_object(tmp_path):
    """FileDatum pointing to a .pkl file is deserialized."""
    fpath = str(tmp_path / "test.pkl")
    with open(fpath, "wb") as f:
        pickle.dump({"x": 99}, f)
    datum = FileDatum(pointer=fpath)
    result = converter_registry.convert(datum, InputForm.OBJECT)
    assert result == {"x": 99}


def test_file_nonpkl_to_object(tmp_path):
    """FileDatum pointing to a non-.pkl file returns the filepath."""
    fpath = str(tmp_path / "test.txt")
    Path(fpath).write_text("hello")
    datum = FileDatum(pointer=fpath)
    result = converter_registry.convert(datum, InputForm.OBJECT)
    assert result == fpath


def test_memory_to_filepath_not_registered():
    """MemoryDatum + FILEPATH should raise TypeError."""
    datum = MemoryDatum(pointer=42)
    with pytest.raises(TypeError, match="No converter registered"):
        converter_registry.convert(datum, InputForm.FILEPATH)
