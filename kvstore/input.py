import re
from kvstore.constants import GET_OP, SET_OP

COMMAND_REGEX = re.compile(r"^(?:set |SET |s |)([^=\s]+)=(.*)$|^(?:get |GET |g |)([^=\s]+)$")

INPUT_PROMPT = "| "

def get_input():
    i = input(INPUT_PROMPT)
    return parse_input(i)

def parse_input(command):
    groups = COMMAND_REGEX.findall(command)
    if len(groups) == 0:
        raise ValueError("invalid input\n")

    setkey, setvalue, getkey  = groups[0]
    if getkey == "":
        return SET_OP, setkey, setvalue
    else:
        return GET_OP, getkey, ""
