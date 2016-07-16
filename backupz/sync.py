"""
The beginning of a module to do two way sync between the BackupZ database objects and the file-system.
"""

import datetime
import operator
import os.path

from .models import *


class Sync(object):
    def __init__(self):
        pass


    def all(self):
        # prefetch_related
        self.sync(DefaultOption.objects.get())

        for area in Area.objects.all():
            self.sync(area)

        for host in Host.objects.all():
            self.sync(host)


    def sync(self, what):
        config = what.config_file()

        if not os.path.exists(config) or what.mtime > datetime.datetime.fromtimestamp(os.path.getmtime(config)):
            return self.db2file(what)
        elif what.mtime < datetime.datetime.fromtimestamp(os.path.getmtime(config)):
            return self.file2db(what)


    def write_fields(self, what, file):
        fields = sorted(what._meta.get_fields(include_parents=False), key=operator.attrgetter('name'))

        for field in fields:
            if field.name in ['id']:
                # Internal fields to skip
                continue

            try:
                if isinstance(field, models.ManyToManyField):
                    # Have to special case these as the value isn't readily available
                    for x in getattr(what, field.name).all():
                        file.write("%s=%s\n" % (field.name, x.value))
                else:
                    value = getattr(what, field.name)
                    if value and value is not None:
                        file.write("%s=%s\n" % (field.name, value))
            except AttributeError:
                pass


    def db2file(self, what):
        with open(what.config_file(), 'w') as config:
            config.write("[defaults]\n")
            self.write_fields(what, config)

            if isinstance(what, Host):
                for job in Job.objects.filter(host=what):
                    config.write("\n[%s]\n" % job.name)
                    self.write_fields(job, config)

        # Finally, update the mtime of the file to match the database so it does not sync next time.
        os.utime(what.config_file(), times=(what.mtime.timestamp(), what.mtime.timestamp()))


    def file2db(self, what):
        # TODO: implement
        print('TODO sync file2db: ', what.config_file())
