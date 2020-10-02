import pytest

from kvstore.encoding import *

def test_bijection():
    en = BinaryEncoderDecoder()

    test_cases = [
        Get("asdkfasdfa"),
        Set("asdkfasdfa", "1234"),
        Set("asdkfasdfa😋😋😋", "1234"),
        WriteLog("asdkfasdfa😋😋😋", "1234", 121),
        RegisterFollower(12121, 121),
        Snapshot({}),
        Snapshot({'a': 'cats'}),
        StringResponse("catburglar"),
        WriteLogs(
            [
                WriteLog("asdfasdf", "1231", 11),
                WriteLog("adfd", "bsfsdf", 12),
            ]
        ),
        Shutdown(),
        EmptyResponse(),
        LBRegistrationInfo([(11123123123, 1), (12553242423, 2)]),
        LBRegistrationInfo([]),
        AddNode(2),
        RemoveNode(1212),
    ]
    for command in test_cases:
        assert command == en.decode(en.encode(command))
