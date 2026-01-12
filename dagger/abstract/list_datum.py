"""
    abstract/list_datum.py
    (c) David Merrell 2026

    Implementation of DatumList, a subclass
    of AbstractDatum. It represents a *list* of 
    zero or more Datum objects. 

    Used to represent the *output* of a task with a
    variable number of outputs; or the *input* of a task
    with a variable number of inputs.

    It's used when the number of task outputs
    is *unknown* at construction. 
"""

from dagger.abstract.datum import AbstractDatum, DatumState


class DatumList(AbstractDatum):

    def __init__(self, datums: list = None, **kwargs):
       
        if not all((isinstance(d, AbstractDatum) for d in datums)):
            raise ValueError("`datums` must be a list of Datum objects")
        self.datum_list = datums

        self.state = DatumState.EMPTY
        self.parents = [p for d in datums for p in d.parents]
        self.pointer = None
        self.quickhash = None
        if "pointer" in kwargs.keys():
            self.populate(kwargs["pointer"])
            self.verify_available(update=True)

    @property
    def pointer(self):
        """
        We want to store the pointers in the actual Datums
        without redundancy. So we define a `property` method
        to access them from the datum_list.
        """
        return [d.pointer for d in self.datum_list]

    def populate(self, pointer: list):
        """
        In this case, `pointer` ought to be a list of values
        corresponding to the Datums in `self.datum_list`.
        This list of values is used to populate each Datum in
        `self.datum_list`.
        """
        for d, p in zip(self.datum_list, pointer):
            d.populate(p)
  
    def verify_available(self, update=True):
        # Need to verify every Datum in the list, 
        # and update if specified.
        available = True
        for d in self.datum_list:
            available &= d.verify_available(update=update)
        return available
    
    def _verify_available_logic(self):
        return all((d._verify_available_logic() for d in self.datum_list))

    def _validate_format_logic(self):
        return all((d._validate_format_logic() for d in self.datum_list))

    def _clear_logic(self):
        for d in self.datum_list:
            d._clear_logic()

    def _quickhash(self):
        hashes = tuple((d._quickhash() for d in self._datum_list))
        return hash(hashes)


