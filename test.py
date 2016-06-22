#! /usr/bin/python3

import os

import time
import datetime

os.environ['DJANGO_SETTINGS_MODULE'] = 'settings'

import django

django.setup()

start = datetime.datetime.now()

from backupz.models import *

jobs = Job.objects.filter(host__name='phys-solid')

for j in jobs:
    print("vvvvvvvvvv %s vvvvvvvvvv" % j)
    print('config_file: ', j.host.config_file())
    if j.host.backup_area:
        print('area:config_file: ', j.host.backup_area.config_file())
    print('fs_path: ', j.fs_path())
    print('zfs_path: ', j.zfs_path())


    print()
    print('partial_okay: ', j.config.partial_okay)
    print('frequency: ', j.config.frequency)
    print('email_to: ', j.config.email_to)
    print('email_from:', j.config.email_from)
    print('allowed_days: ', j.config.allowed_days)
    print('schedule: ', j.config.schedule)
    print()

    print('should_start: ', j.should_start(stamp=start))
    print()

    b = Backup(job=j, start=start)
    print(b)
    cli = b.cli()
    print(cli[0])
    for c in cli[1:]:
        print(" ", c)

    print()
    time.sleep(1.6)
