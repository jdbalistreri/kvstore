import pytest

from kvstore.encoding import *

def test_bijection():
    en = BinaryEncoderDecoder()

    test_cases = [
        Get("asdkfasdfa"),
        Set("asdkfasdfa", "1234"),
    ]
    for command in test_cases:
        assert command == en.decode(en.encode(command))
