import atexit
import re
import readline
from kvstore.constants import GET_OP, SET_OP
from kvstore.encoding import *

COMMAND_REGEX = re.compile(r"^(?:set |SET |s |)\s*([^=\s]+)=(.*)$|^(?:get |GET |g |)\s*([^=\s]+)$")

class InputValidationError(Exception):
    pass

def input_to_command(input):
    op, key, value = parse_input(input)

    if op == GET_OP:
        return Get(key)
    elif op == SET_OP:
        return Set(key, value)

    raise InputValidationError("unknown command")

def parse_input(command):
    groups = COMMAND_REGEX.findall(command)
    if len(groups) == 0:
        raise InputValidationError("invalid input\n")

    setkey, setvalue, getkey  = groups[0]
    if getkey == "":
        return SET_OP, setkey, setvalue
    else:
        return GET_OP, getkey, ""

def configure_readline():
    histfile = ".python_history"
    try:
        readline.read_history_file(histfile)
        readline.set_history_length(1000)
    except FileNotFoundError:
        pass

    atexit.register(readline.write_history_file, histfile)
