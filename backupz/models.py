from __future__ import unicode_literals

from django.db import models
from django.db.models import Q

import solo.models
import multiselectfield

import django_extras.db.models
import os

import django

from . import lib
from .zfs import ZFS

import settings

zfs = ZFS()

DAYS = ((0, 'Monday'),
        (1, 'Tuesday'),
        (2, 'Wednesday'),
        (3, 'Thursday'),
        (4, 'Friday'),
        (5, 'Saturday'),
        (6, 'Sunday'),
        )


def _config_file(what, conf):
    p = os.path.join(settings.config_path, what)
    if not os.path.exists(p):
        os.makedirs(p, 0o750)
    return os.path.join(p, '%s.conf' % conf)


class Schedule(models.Model):
    name = models.SlugField(unique=True)
    start_time = models.TimeField()
    end_time = models.TimeField()

    def __str__(self):  # __unicode__ on Python 2
        return '%s (%s-%s)' % (self.name, self.start_time, self.end_time)

    class Meta:
        ordering = ('name',)


class PostBackupScript(models.Model):
    name = models.CharField(max_length=50, unique=True)
    description = models.TextField()

    def path(self):
        return os.path.join(settings.config_path, 'scripts', self.name)

    def __str__(self):  # __unicode__ on Python 2
        return '%s: %s' % (self.name, self.description)

    class Meta:
        ordering = ('name',)


class SnapshotsToKeep(models.Model):
    daily = models.PositiveSmallIntegerField()
    weekly = models.PositiveSmallIntegerField()
    monthly = models.PositiveSmallIntegerField()
    yearly = models.PositiveSmallIntegerField()

    def __str__(self):
        return 'Daily: %d, Weekly: %d, Monthly: %d, Yearly: %d' % (self.daily, self.weekly, self.monthly, self.yearly)

    class Meta:
        unique_together = ('daily', 'weekly', 'monthly', 'yearly')
        verbose_name_plural = "Snapshots to Keep"


class RsyncOption(models.Model):
    name = models.CharField(max_length=50, unique=True)
    value = models.CharField(max_length=50, unique=True)

    # "--archive --one-file-system --delete"

    def __str__(self):  # __unicode__ on Python 2
        return "%s (%s)" % (self.name, self.value)

    class Meta:
        ordering = ('name',)


class Transport(models.Model):
    rsync_options = models.ManyToManyField('RsyncOption')
    command = models.CharField(max_length=200)
    port = models.PositiveSmallIntegerField(unique=True)
    separator = models.CharField(max_length=2, default=':')

    def __str__(self):  # __unicode__ on Python 2
        return '%s (%s)' % (self.command, self.port)

    class Meta:
        unique_together = ('command', 'port',)
        ordering = ('command',)


class Owner(models.Model):
    name = models.CharField(max_length=50, unique=True)

    def __str__(self):
        return self.name

    class Meta:
        ordering = ('name',)
        verbose_name = 'Owner'


class BackupZOption(models.Model):
    enabled = models.BooleanField(default=True)

    email_to = models.CharField(max_length=200, null=True, blank=True)
    email_from = models.CharField(max_length=200, null=True, blank=True)

    quota = models.PositiveIntegerField(null=True, blank=True)

    ssh_key = models.CharField(max_length=200, null=True, blank=True)
    ssh_command = models.CharField(max_length=200, null=True, blank=True)
    user = models.CharField(max_length=20, null=True, blank=True)
    transport = models.ForeignKey(Transport, null=True, blank=True)
    password = models.CharField(max_length=200, null=True, blank=True)

    max_jobs = models.PositiveSmallIntegerField(null=True, blank=True)
    priority = models.PositiveSmallIntegerField(null=True, blank=True)

    frequency = models.DurationField(null=True, blank=True)
    use_max = django_extras.db.models.PercentField(null=True, blank=True)
    use_warn = django_extras.db.models.PercentField(null=True, blank=True)

    rsync_options = models.ManyToManyField('RsyncOption', blank=True)

    snapshots_to_keep = models.ForeignKey(SnapshotsToKeep, null=True, blank=True)

    schedule = models.ForeignKey(Schedule, null=True, blank=True)

    post_backup_script = models.ForeignKey(PostBackupScript, null=True, blank=True)

    allowed_days = multiselectfield.MultiSelectField(choices=DAYS, null=True, blank=True)

    partial_okay = models.NullBooleanField()

    EXPIRATION_ALG = (
        (0, 'Linear'),
        (1, 'Exponential'),
    )
    expiration_algorithm = models.SmallIntegerField(choices=EXPIRATION_ALG, default=EXPIRATION_ALG[0][0],
                                                    null=True, blank=True)

    owner = models.ForeignKey(Owner, null=True, blank=True)

    class Meta:
        abstract = True


class DefaultOption(solo.models.SingletonModel, BackupZOption):
    def config_file(self):
        return _config_file('areas', 'default')

    def __str__(self):
        return 'Default BackupZ Options'

    class Meta:
        verbose_name = 'Default BackupZ Options'


class Area(BackupZOption):
    zpool = models.CharField(max_length=200, unique=True)
    mountpoint = models.CharField(max_length=200, default='/', unique=True)
    display_name = models.CharField(max_length=200, unique=True)

    file_timestamp = models.DateTimeField(auto_now=True)

    def fs_path(self, which, what='backups'):
        p = os.path.join('/', self.mountpoint)
        return self._path(p, which, what)

    def zfs_path(self, which, what='backups'):
        p = self.zpool
        return self._path(p, which, what)

    def _path(self, p, which, what):

        if what:
            p = os.path.join(p, what)

        if which is not None:
            if isinstance(which, Job):
                p = os.path.join(p, which.host.name, which.name)
            elif isinstance(which, Host):
                p = os.path.join(p, which.name)
            else:
                raise AttributeError

        return p

    def config_file(self):
        return _config_file('areas', self.display_name)

    class Meta:
        ordering = ('zpool',)

    def __str__(self):  # __unicode__ on Python 2
        return self.fs_path(None)


class Host(BackupZOption):
    name = models.SlugField(unique=True)
    ip = models.GenericIPAddressField(protocol='IPv4', unique=True)
    backup_area = models.ForeignKey('Area', null=True, blank=True)
    allow_concurrent = models.BooleanField(default=False)

    file_timestamp = models.DateTimeField(auto_now=True)

    def list_backups(self):
        return Backup.objects.filter(job__host=self)

    def config_file(self):
        return _config_file('hosts', self.name)

    def __str__(self):  # __unicode__ on Python 2
        return "%s (%s)" % (self.name, self.owner)

    class Meta:
        ordering = ('name',)


class Config():
    def __init__(self, job):
        self.job = job

    def __getattr__(self, name):
        # print('Config: __getattr__(%s)' % name)
        for t in [self.job, self.job.backup_area, self.job.host, self.job.host.backup_area,
                  DefaultOption.objects.get()]:
            if t is not None:
                a = getattr(t, name)
                if a is not None and a != '':
                    return a

        return None


class JobManager(models.Manager):
    def to_run(self, stamp=django.utils.timezone.now()):
        run = []
        not_run = []

        # TODO: order by priority and distance from last successful backup, taking into account duration.
        # Cannot do simple .order_by() as we need to check job/host/area/defaults.
        for j in Job.objects.all():
            if j.should_start(stamp) is True:
                run.append(j)
            else:
                not_run.append(j)

        return run, not_run


class Job(BackupZOption):
    host = models.ForeignKey(Host)
    name = models.SlugField()
    path = models.CharField(max_length=200)
    backup_area = models.ForeignKey('Area', null=True, blank=True)

    def _enabled(self):
        if not DefaultOption.objects.get().enabled:
            return 'Not enabled in default options'
        elif not self._active_backup_area().enabled:
            return 'Not enabled in Area'
        elif not self.host.enabled:
            return 'Not enabled in Host'
        elif not self.enabled:
            return 'Not enabled in Job'

        return True


    def list_backups(self):
        return Backup.objects.filter(job=self)

    def _active_backup_area(self):
        if self.backup_area:
            return self.backup_area
        else:
            return self.host.backup_area

    def fs_path(self):
        return self._active_backup_area().fs_path(self)

    def zfs_path(self):
        return self._active_backup_area().zfs_path(self)

    def __init__(self, *args, **kwargs):
        self.config = Config(self)
        super(Job, self).__init__(*args, **kwargs)

    def __str__(self):  # __unicode__ on Python 2
        return "%s/%s (%s)" % (self.host.name, self.name, self.config.owner)

    class Meta:
        unique_together = (('host', 'name'), ('host', 'path'))
        ordering = ('host', 'name')

    objects = JobManager()

    def should_start(self, stamp=django.utils.timezone.now(), create_zfs_filesystems=True):
        """
        CAUTION: you MUST check the return value against `is [not] True' as this returns a string for False.
        """
        schedule = self.config.schedule

        e = self._enabled()
        if e is not True:
            return e

        # TODO: check if the area is mounted: https://docs.python.org/3/library/os.path.html#os.path.ismount
        zfs_filesystems = (self._active_backup_area().zfs_path(None, None),
                           self._active_backup_area().zfs_path(None),
                           self._active_backup_area().zfs_path(self.host),
                           self._active_backup_area().zfs_path(self),
                           )
        for z in zfs_filesystems:
            if not zfs.isfilesystem(z, create=create_zfs_filesystems):
                return 'ZFS file-system does not exist: %s' % z

        mountpoints = ((self._active_backup_area().fs_path(None, None), 'Area'),
                       (self._active_backup_area().fs_path(None), 'Backups'),
                       (self._active_backup_area().fs_path(self.host), 'Host'),
                       (self.fs_path(), 'Job'),
                       )
        for mp in mountpoints:
            if not os.path.ismount(mp[0]):
                return '%s mount point is not mounted: %s' % (mp[1], mp[0])

        # TODO: check quota here
        # TODO: check max_jobs against host/area/defaults

        try:
            backups = Backup.objects.filter(job=self).latest(field_name='start')
            if backups.status < 0 or backups.end is None:
                return 'Job currently in progress'

            if backups.start + self.config.frequency > stamp:
                return 'Not yet time for another job, last was %s' % backups.start
        except Backup.DoesNotExist:
            pass

        try:
            backups = Backup.objects.filter(Q(status__lt=0) | Q(end__isnull=True), job__host=self.host)
            if len(backups) > 0 and not self.host.allow_concurrent:
                return 'Concurrent backups to host not allowed (%s)' % ", ".join([str(x) for x in backups])
        except Backup.DoesNotExist:
            pass

        if str(stamp.weekday()) not in self.config.allowed_days:
            return 'Today not an allowed day'

        if schedule.start_time <= schedule.end_time:
            if schedule.start_time <= stamp.time() <= schedule.end_time:
                return True
            else:
                return 'Not within window'
        else:
            if schedule.start_time <= stamp.time() or stamp.time() <= schedule.end_time:
                return True
            else:
                return 'Not within window'


class Backup(models.Model):
    STATUS = (
        (-1, 'In Progress'),
        (0, 'Success'),
        (1, 'Success (with minor errors)'),
        (2, 'Failure'),
    )

    job = models.ForeignKey(Job)
    start = models.DateTimeField(default=django.utils.timezone.now)
    end = models.DateTimeField(null=True, blank=True)
    status = models.SmallIntegerField(choices=STATUS, default=STATUS[0][0])

    def timestamp(self):
        return self.start.strftime("%Y-%d-%m_%H-%M.%S")

    def log_file(self):
        p = os.path.join(settings.log_path, self.job.host.name)
        if not os.path.exists(p):
            os.makedirs(p, 0o750)

        s = "%s-%s.log" % (self.job.name, self.timestamp())
        return os.path.join(p, s)

    def rsync_arguments(self):
        args = []

        for t in [DefaultOption.objects.get(),
                  self.job.config.transport,
                  self.job.backup_area,
                  self.job.host,
                  self.job]:
            if t and t.rsync_options:
                args.append([x.value for x in t.rsync_options.all()])

        fname = os.path.join(settings.config_path, 'exclude', self.job.host.name)
        if os.path.isfile(fname):
            args.append("--exclude-from='%s'" % fname)

        return args

    def cli(self):
        """
        ORIG
        rsync
        --archive --one-file-system --delete
        -e '/usr/bin/ssh -i /root/.ssh/default.key'
        --log-file=/backupz/log/monitoring/etc-2016-06-20_22-30.53.log
        --exclude=".gvfs"
        --timeout=3600
        --exclude-from='/backupz/extras/global.excludes'
        root@monitoring.metro.ucdavis.edu:"/etc" .

        NEW
        /usr/bin/rsync
          --exclude-from='/backupz/exclude/linux'
          --archive
          --delete
          --one-file-system
          --timeout=3600
          --timeout=36000
          --exclude-from='/etc/backupz/exclude/phys-solid'
          --logfile='/var/log/backupz/phys-solid/var-spool-mail-2016-22-06_11-52.13.log'
          --rsh='/opt/csw/bin/ssh -i /root/.ssh/default.key'
          root@169.237.42.75:'/var/spool/mail'
          /physics/backups/phys-solid/var-spool-mail
        """

        transport = self.job.config.transport

        command = [transport.command,
                   self.rsync_arguments(),
                   "--logfile='%s'" % self.log_file(),
                   "--rsh='%s -i %s'" % (self.job.config.ssh_command, self.job.config.ssh_key),
                   "%s@%s%s'%s'" % (
                       self.job.config.user, self.job.host.ip, transport.separator, self.job.path.rstrip('/')),
                   self.job.fs_path().rstrip('/')
                   ]

        pw = self.job.config.password
        if pw:
            os.environ['RSYNC_PASSWORD'] = pw

        return list(lib.flatten(command))

    def __str__(self):  # __unicode__ on Python 2
        return "%s/%s @ %s" % (self.job.host.name, self.job.name, self.timestamp())

    class Meta:
        ordering = ('job__host__name', 'job__name', 'start',)
