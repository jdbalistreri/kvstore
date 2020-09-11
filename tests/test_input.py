import pytest

from kvstore.input import parse_input, InputValidationError

ERROR = "error"

def test_parse_input():
    test_cases = [
        # nonsense commands
        {"input": "asdfasdf asdf", "expected": ERROR},
        {"input": "get hello=123", "expected": ERROR},
        {"input": "get ===", "expected": ERROR},
        {"input": "get abc adsfa", "expected": ERROR},

        # get command syntax
        {"input": "get hello", "expected": ("get", "hello", "")},
        {"input": "GET hello", "expected": ("get", "hello", "")},
        {"input": "g hello", "expected": ("get", "hello", "")},
        {"input": "hello", "expected": ("get", "hello", "")},
        {"input": "get    abc", "expected": ("get", "abc", "")},
        {"input": "gEt hello", "expected": ERROR},

        # set command syntax
        {"input": "set hello=1", "expected": ("set", "hello", "1")},
        {"input": "SET hello=1", "expected": ("set", "hello", "1")},
        {"input": "s hello=1", "expected": ("set", "hello", "1")},
        {"input": "hello=1", "expected": ("set", "hello", "1")},
        {"input": "sEt hello=1", "expected": ERROR},
        {"input": "get=1", "expected": ("set", "get", "1")},

        # get target edge test_cases
        {"input": "get HEL121231DHFLSDF(()*&Y)", "expected": ("get", "HEL121231DHFLSDF(()*&Y)", "")},
        {"input": "get HEL121231DHFLSDF(()*&Y)=", "expected": ERROR},
        {"input": "get ", "expected": ERROR},
        {"input": "get set", "expected": ("get", "set", "")},
        {"input": "set", "expected": ("get", "set", "")},

        # set target edge test_cases
        {"input": "set 123=", "expected": ("set", "123", "")},
        {"input": "set 123=123", "expected": ("set", "123", "123")},
        {"input": "set 123=====///", "expected": ("set", "123", "====///")},
        {"input": "set =1", "expected": ERROR},
    ]
    for case in test_cases:
        expected = case["expected"]
        if expected == ERROR:
            with pytest.raises(InputValidationError):
                parse_input(case["input"])
        else:
            assert expected == parse_input(case["input"])
