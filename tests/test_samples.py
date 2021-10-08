import pytest

from mrtools.samples import SampleABC, Sample, SampleGroup, SampleFromDAS, SampleFromFS


def test_sampleabc():

    with pytest.raises(TypeError):
        SampleABC()
