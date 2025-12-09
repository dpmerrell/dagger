
from dagger.abstract.datum import DatumState
from dagger.core.datum import MemoryDatum, FileDatum

def test_memorydatum():

    ls = [1,2,3,4]
    st = "cat dog"

    # Construction with and without a pointer
    d1 = MemoryDatum()
    d2 = MemoryDatum(pointer=ls)
    d3 = MemoryDatum(pointer=None) # corner case

    assert d1.state == DatumState.EMPTY
    assert d1.pointer is None
    assert d1.quickhash is None

    assert d2.state == DatumState.POPULATED
    assert d2.pointer is ls
    assert d2.quickhash is None
   
    assert d3.state == DatumState.POPULATED
    assert d3.pointer is None
    assert d3.quickhash is None

    # populate
    d1.populate(st)
    assert d1.state == DatumState.POPULATED
    assert d1.pointer is st

    # validate_format
    d1._validate_format()
    d2._validate_format()
    d3._validate_format()

    # verify_available
    assert d1._verify_available()
    assert d2._verify_available()
    assert d3._verify_available()

    assert d1.state == DatumState.AVAILABLE
    assert d2.state == DatumState.AVAILABLE
    assert d3.state == DatumState.AVAILABLE
    
    assert d1.quickhash is not None
    assert d2.quickhash is not None
    assert d3.quickhash is not None

    # verify_quickhash
    true_quickhash = d1.quickhash
    assert d1._verify_quickhash()
    d1.quickhash = "abc123"
    assert not d1._verify_quickhash()
    assert d1._verify_quickhash
    assert d1.quickhash == true_quickhash

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

    assert d1.quickhash is None
    assert d2.quickhash is None
    assert d3.quickhash is None

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
    d1 = FileDatum()
    d2 = FileDatum(pointer=existing_path)

    assert d1.state == DatumState.EMPTY
    assert d2.state == DatumState.POPULATED
    assert d2.pointer is existing_path

    assert d1.quickhash is None
    assert d2.quickhash is None

    # populate
    d1.populate(nonexistent_path)
    assert d1.state == DatumState.POPULATED
    assert d1.pointer is nonexistent_path

    # validate_format
    d1._validate_format()
    d2._validate_format()
    
    try:
        # This should throw an exception on construction
        # since construction with pointer calls
        # `populate()`, which in turn calls `_validate_format()`,
        # which throws a ValueError.
        d3 = FileDatum(pointer=not_a_path)
    except ValueError:
        assert True
    else:
        assert False

    # verify_available
    assert not d1._verify_available()
    assert d1.state == DatumState.POPULATED
    assert d2._verify_available()
    assert d2.state == DatumState.AVAILABLE

    assert d1.quickhash is None
    assert d2.quickhash is not None

    # verify_quickhash
    true_quickhash = d2.quickhash
    assert d2._verify_quickhash()
    d2.quickhash = "abc123"
    assert not d2._verify_quickhash()
    assert d2.quickhash == true_quickhash
    assert d2._verify_quickhash()


    # clear
    assert path.exists(existing_path)
    d2.clear()
    assert not path.exists(existing_path)
    assert d2.state == DatumState.POPULATED
    assert d2.quickhash is None
    # Make sure calling d2.clear() again 
    # doesn't cause errors
    d2.clear()

    return


