import atexit
import re
import readline
from kvstore.constants import GET_OP, SET_OP, ADD_NODE, REMOVE_NODE, LIST_NODES
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
    elif op == ADD_NODE:
        return AddNode(key)
    elif op == REMOVE_NODE:
        return RemoveNode(key)
    elif op == LIST_NODES:
        return ListNodes()

    raise InputValidationError("unknown command")

def parse_node(command):
    pieces = command.lower().split(" ")
    if len(pieces) != 2:
        raise InputValidationError("only one space allowed in node command\n")
    return int(pieces[1])

def parse_input(command):
    if command.lower().startswith("add_node"):
        node_num = parse_node(command)
        return ADD_NODE, node_num, None
    elif command.lower().startswith("remove_node"):
        node_num = parse_node(command)
        return REMOVE_NODE, node_num, None
    elif command.lower().startswith("list_nodes"):
        return LIST_NODES, None, None
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
