import os
import sys


def print_override(value_to_print):
    stdout = sys.stdout
    sys.stdout = open(os.devnull, "w")
    stdout.write(str(value_to_print))
    stdout.flush()
    sys.stdout = stdout
