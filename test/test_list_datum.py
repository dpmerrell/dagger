
from dagger.abstract.datum import AbstractDatum, DatumState
from dagger.abstract.list_datum import DatumList
from dagger.core.datum import MemoryDatum, FileDatum

def test_datumlist():

    cat_d = MemoryDatum(pointer="cat")
    dog_d = MemoryDatum(pointer="dog")

    # Construction with and without a pointer
    d1 = DatumList(MemoryDatum)
    d2 = DatumList(MemoryDatum, datums=[cat_d, dog_d])

    assert d1.state == DatumState.EMPTY
    assert d1.pointer is None
    assert d1.quickhash is None

    assert d2.state == DatumState.AVAILABLE
    assert d2.pointer == ["cat", "dog"]
    assert d2.quickhash == d2._quickhash()
   
    # populate
    # TODO wheels are going to come off here -- d1 does
    # not have a datum_list, and so `populate` will fail.
    # the `populate` logic isn't complete.
    d1.populate(["fish", "bird"])
    assert d1.state == DatumState.POPULATED
    assert d1.pointer == ["fish", "bird"]

    # validate_format
    d1._validate_format()
    d2._validate_format()

    # verify_available
    assert d1.verify_available()
    assert d2.verify_available()

    assert d1.state == DatumState.AVAILABLE
    assert d2.state == DatumState.AVAILABLE
    
    assert d1.quickhash is not None
    assert d2.quickhash is not None

    # verify_quickhash
    true_quickhash = d1.quickhash
    assert d1._verify_quickhash(update=True)
    d1.quickhash = "abc123"
    assert not d1._verify_quickhash(update=True)
    assert d1._verify_quickhash()
    assert d1.quickhash == true_quickhash

    # clear
    d1.clear()
    d2.clear()
    assert d1.state == DatumState.POPULATED
    assert d2.state == DatumState.POPULATED

    assert d1.quickhash is None
    assert d2.quickhash is None

    return


