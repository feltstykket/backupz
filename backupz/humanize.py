import re

from django.core.exceptions import ValidationError

import math

units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB']

rx = re.compile(r'^\s*([\d.]+)\s*([a-z]{1,2})?\s*$', re.IGNORECASE)

# Originally based on: http://stackoverflow.com/questions/1094841/reusable-library-to-get-human-readable-version-of-file-size
def humanize(size):
    # print('humanize(%s)' % size)

    size = abs(size)

    if size <= 0:
        return "0 B"

    p = math.floor(math.log(size, 2) / 10)

    return "%.1f %s" % (size / math.pow(1024, p), units[int(p)])


def dehumanize(size):
    # print('dehumanize(%s)' % size)

    if not size or size == '':
        return None

    unit = units[0]

    r = rx.match(size)
    if r:
        f = r.group(1)
        if r.group(2):
            unit = r.group(2)
    else:
        raise ValidationError('Please enter a value in the format: 123.45 [ %s ]' % ' | '.join(units))

    try:
        f = float(f)
    except:
        raise ValidationError("Unable to convert `%s' to float" % f)

    if f < 0:
        raise ValidationError('Must not be negative')

    try:
        p = units.index(unit.upper())
        return int(math.pow(1024, p) * f)
    except ValueError:
        raise ValidationError('Not a recognized suffix, please use one of: ' + ", ".join(units))


if __name__ == "__main__":
    import random

    random.seed(2)

    s = 256
    while s < 1e12:
        h = humanize(s)
        d = dehumanize(h)

        print("%12s => %9s => %12s => %12d <=> %s" % (s, h, d, s - d, humanize(s - d)))

        s = int(s * (random.random() + 2))
