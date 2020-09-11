import re
from kvstore.constants import GET_OP, SET_OP

COMMAND_REGEX = re.compile(r"^(?:set |SET |s |)\s*([^=\s]+)=(.*)$|^(?:get |GET |g |)\s*([^=\s]+)$")

class InputValidationError(Exception):
    pass

def parse_input(command):
    groups = COMMAND_REGEX.findall(command)
    if len(groups) == 0:
        raise InputValidationError("invalid input\n")

    setkey, setvalue, getkey  = groups[0]
    if getkey == "":
        return SET_OP, setkey, setvalue
    else:
        return GET_OP, getkey, ""
