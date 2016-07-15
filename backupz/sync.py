"""
The beginning of a module to do two way sync between the BackupZ database objects and the file-system.
"""

import datetime
import os.path

from .models import *

class Sync(object):
    def __init__(self):
        pass


    def all(self):
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


    def db2file(self, what):
        # TODO: implement
        print('Sync db2file: ', what.config_file())

    def file2db(self, what):
        # TODO: implement
        print('Sync file2db: ', what.config_file())

