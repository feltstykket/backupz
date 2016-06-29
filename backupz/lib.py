import subprocess
import collections

import collections

Result = collections.namedtuple('Result', 'out err rc')

def run_command(command, cwd='/tmp', debug=False):
    p = subprocess.Popen(command, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    out, err = p.communicate()
    r = Result(out, err, p.returncode)
    if debug:
        print(r)

    if 'permission denied' in err.lower():
        raise PermissionError(err.rstrip())

    return r


def flatten(l):
    for el in l:
        if isinstance(el, collections.Iterable) and not isinstance(el, (str, bytes)):
            for sub in flatten(el):
                yield sub
        else:
            yield el
