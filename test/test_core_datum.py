
from dagger.core.datum import MemoryDatum, DiskDatum, DatumState

def test_memorydatum():

    ls = [1,2,3,4]
    st = "cat dog"

    # Construction with and without a pointer
    d1 = MemoryDatum()
    d2 = MemoryDatum(pointer=ls)
    d3 = MemoryDatum(pointer=None) # corner case

    assert d1.state == DatumState.EMPTY
    assert d1.pointer is None

    assert d2.state == DatumState.POPULATED
    assert d2.pointer is ls
   
    assert d3.state == DatumState.POPULATED
    assert d3.pointer is None

    # populate
    d1.populate(st)
    assert d1.state == DatumState.POPULATED
    assert d1.pointer is st

    # validate_format
    d1.validate_format()
    d2.validate_format()
    d3.validate_format()

    # check_available
    assert d1.check_available()
    assert d2.check_available()
    assert d3.check_available()

    assert d1.state == DatumState.AVAILABLE
    assert d2.state == DatumState.AVAILABLE
    assert d3.state == DatumState.AVAILABLE

    # clear
    d1.clear()
    d2.clear()
    d3.clear()
    assert d1.state == DatumState.POPULATED
    assert d2.state == DatumState.POPULATED
    assert d3.state == DatumState.POPULATED
    assert d1.pointer is st 
    assert d2.pointer is ls
    assert d3.pointer is None

    return


def test_diskdatum():

    from os import path

    # setup
    nonexistent_path = "nonexistent_file.txt"
    existing_path = "test/catdog.txt"
    with open(existing_path, "w") as f:
        f.write("catdog")
    not_a_path = 123

    # Construction with and without a pointer
    d1 = DiskDatum()
    d2 = DiskDatum(pointer=existing_path)

    assert d1.state == DatumState.EMPTY
    assert d2.state == DatumState.POPULATED
    assert d2.pointer is existing_path

    # populate
    d1.populate(nonexistent_path)
    assert d1.state == DatumState.POPULATED
    assert d1.pointer is nonexistent_path

    # validate_format
    d1.validate_format()
    d2.validate_format()
    
    try:
        # This should throw an exception on construction
        # since construction with pointer calls
        # `populate()`, which in turn calls `validate_format`
        d3 = DiskDatum(pointer=not_a_path)
    except ValueError:
        assert True
    else:
        assert False

    # check_available
    assert not d1.check_available()
    assert d1.state == DatumState.POPULATED
    assert d2.check_available()
    assert d2.state == DatumState.AVAILABLE

    # clear
    assert path.exists(existing_path)
    d2.clear()
    assert not path.exists(existing_path)
    assert d2.state == DatumState.POPULATED
    # Make sure calling d2.clear() again 
    # doesn't cause errors
    d2.clear()

    return

