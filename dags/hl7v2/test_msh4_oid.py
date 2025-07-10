import pytest

def test_oid():
    from msh4_oid import get_domain_oid_from_hl7v2_msh4_with_crosswalk_fallback
    
    oid1 = get_domain_oid_from_hl7v2_msh4_with_crosswalk_fallback('../../samples/hl7v2/ADT_A01_oid.txt')
    oid2 = get_domain_oid_from_hl7v2_msh4_with_crosswalk_fallback('../../samples/hl7v2/ADT_A01_name.txt')
    assert oid1 == oid2
    assert oid1 == "1.2.840.114350.1.13.409.2.7.3.688884.100"
    with pytest.raises(ValueError):
        oid = get_domain_oid_from_hl7v2_msh4_with_crosswalk_fallback('../../samples/hl7v2/ADT_A01.txt')
