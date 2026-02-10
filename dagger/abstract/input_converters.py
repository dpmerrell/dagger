"""
    abstract/input_converters.py
    (c) David Merrell 2026

    A registry of converter functions that translate Datums
    into the form required by a given Task type.

    Converters are keyed by (datum_type, input_form).
    Each Task type declares an `input_form` attribute;
    `AbstractTask._collect_inputs()` uses it to look up
    the right converter for each input Datum.
"""

from enum import Enum


class InputForm(Enum):
    """
    The form that a Task expects its inputs in.

    OBJECT:   a Python object in memory
    FILEPATH: a filepath string
    """
    OBJECT = "object"
    FILEPATH = "filepath"


class ConverterRegistry:
    """
    A registry mapping (datum_type, input_form) pairs
    to converter callables.

    A converter callable has the signature:
        converter(datum) -> value

    where `datum` is an AbstractDatum instance and `value`
    is whatever the Task expects as input.
    """

    def __init__(self):
        self._converters = {}

    def register(self, datum_type, input_form, converter):
        self._converters[(datum_type, input_form)] = converter

    def convert(self, datum, input_form):
        """
        Look up and call the converter for the given datum
        and input_form.

        Uses the datum's MRO to find the most specific
        registered converter.
        """
        for cls in type(datum).__mro__:
            key = (cls, input_form)
            if key in self._converters:
                return self._converters[key](datum)
        raise TypeError(
            f"No converter registered for "
            f"({type(datum).__name__}, {input_form})"
        )

    def has_converter(self, datum_type, input_form):
        return (datum_type, input_form) in self._converters


converter_registry = ConverterRegistry()
