"""
    abstract/list_datum.py
    (c) David Merrell 2026

    Implementation of DatumList, a subclass
    of AbstractDatum. It represents a *list* of 
    zero or more Datum objects _of the same type_. 

    Used to represent the *output* of a task with a
    variable number of outputs; or the *input* of a task
    with a variable number of inputs.

    It's used when the number of task outputs
    is *unknown* at construction. 
"""

from dagger.abstract.datum import AbstractDatum, DatumState


class DatumList(AbstractDatum):

    def __init__(self, datum_type, datums: list = None, **kwargs):
      
        self.datum_type = datum_type

        # Validate the datums, if provided
        if datums is not None:
            if not all((isinstance(d, datum_type) for d in datums)):
                raise ValueError(f"`datums` must be a list of {datum_type} objects")

        self.state = DatumState.EMPTY
        self.pointer = None
        self.quickhash = None

        # Whether 'datums' or 'pointer' is provided, we need
        # to initialize in a consistent way. 'pointer' takes
        # priority if both are provided.
        self.datum_list = None
        if "pointer" in kwargs.keys():
            self.populate(kwargs["pointer"])
            self.verify_available(update=True)
            self.parents = [p for d in self.datum_list for p in d.parents]
        elif datums is not None:
            pointers = [d.pointer for d in datums]
            self.populate(pointers)
            self.verify_available(update=True)
            self.parents = [p for d in self.datum_list for p in d.parents]
        else:
            self.datum_list = None
            self.parents = []
        

    @property
    def pointer(self):
        """
        We want to store the pointers in the actual Datums
        without redundancy. So we define a `property` method
        to access them from the datum_list.
        """
        if self.datum_list is None:
            return None
        else:
            return [d.pointer for d in self.datum_list]

    @pointer.setter
    def pointer(self, value: list):
        """
        Set the pointer to a value
        """
        if value is not None:
            for d, v in zip(self.datum_list, value):
                d.pointer = v

    def populate(self, pointer: list):
        """
        In this case, `pointer` ought to be a list of values
        corresponding to the Datums in `self.datum_list`.
        This list of values is used to populate each Datum in
        `self.datum_list`.
        """
       
        if self.datum_list is None:
            self.datum_list = []
            for p in pointer:
                self.datum_list.append(self.datum_type(pointer=p))
        else:
            for d, p in zip(self.datum_list, pointer):
                d.populate(p)
        
        self.state = DatumState.POPULATED 
        # Don't need to validate -- the sub-datums'
        # `populate` calls already validate.
        return 
 
    def verify_available(self, update=True):
        # Need to verify every Datum in the list, 
        # and update if specified.
        available = True
        for d in self.datum_list:
            available &= d.verify_available(update=update)
    
        # Then we follow the same logic as for a normal Datum
        if (self.state != DatumState.EMPTY) and available:
            if update:
                self.state = DatumState.AVAILABLE
                self.quickhash = self._quickhash()
            return True
        else:
            return False
        
    def _verify_available_logic(self):
        return all((d._verify_available_logic() for d in self.datum_list))

    def _validate_format_logic(self):
        return all((d._validate_format_logic() for d in self.datum_list))

    def _clear_logic(self):
        for d in self.datum_list:
            d._clear_logic()

    def _quickhash(self):
        hashes = tuple((d._quickhash() for d in self.datum_list))
        return hash(hashes)


