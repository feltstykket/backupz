from __future__ import unicode_literals

from django.db import models
import solo.models
import multiselectfield

import django_extras.db.models
import os

import django

from .utils import *
import settings

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


class PI(models.Model):
    name = models.CharField(max_length=50, unique=True)

    def __str__(self):
        return self.name

    class Meta:
        ordering = ('name', )
        verbose_name = 'PI'


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
    expiration_algorithm = models.SmallIntegerField(choices=EXPIRATION_ALG, default=EXPIRATION_ALG[0][0], null=True, blank=True)

    PI = models.ForeignKey(PI, null=True, blank=True)

    class Meta:
        abstract = True



class DefaultOption(solo.models.SingletonModel, BackupZOption):
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

    file_timestamp = models.DateTimeField(auto_now=True)

    def config_file(self):
        return _config_file('hosts', self.name)

    def __str__(self):  # __unicode__ on Python 2
        return "%s (%s)" % (self.name, self.PI)

    class Meta:
        ordering = ('name',)


class Config():
    def __init__(self, job):
        self.job = job

    def __getattr__(self, name):
        #print('Config: __getattr__(%s)' % name)
        for t in [self.job, self.job.backup_area, self.job.host, self.job.host.backup_area, DefaultOption.objects.get()]:
            if t is not None:
                a = getattr(t, name)
                if a is not None and a != '':
                    return a

        return None


class Job(BackupZOption):
    host = models.ForeignKey(Host)
    name = models.SlugField()
    path = models.CharField(max_length=200)
    backup_area = models.ForeignKey('Area', null=True, blank=True)

    def active_backup_area(self):
        if self.backup_area:
            return self.backup_area
        else:
            return self.host.backup_area

    def fs_path(self):
        return self.active_backup_area().fs_path(self)

    def zfs_path(self):
        return self.active_backup_area().zfs_path(self)

    def __init__(self, *args, **kwargs):
        self.config = Config(self)
        super(Job, self).__init__(*args, **kwargs)

    def __str__(self):  # __unicode__ on Python 2
        return "%s/%s (%s)" % (self.host.name, self.name, self.config.PI)

    class Meta:
        unique_together = (('host', 'name'), ('host', 'path'))
        ordering = ('host', 'name')

    def should_start(self, stamp=django.utils.timezone.now()):
        schedule = self.config.schedule

        if str(stamp.weekday()) not in self.config.allowed_days:
            return False, 'Today not an allowed day'

        try:
            backup = Backup.objects.filter(job=self).latest(field_name='start')
            if backup.status < 0 or backup.end is None:
                return False, 'Job currently in progress'

            if backup.start + self.config.frequency > stamp:
                return False, 'Not yet time for another job, last was %s' % backup.start
        except Backup.DoesNotExist:
            pass

        if schedule.start_time <= schedule.end_time:
            if schedule.start_time <= stamp.time() <= schedule.end_time:
                return True, 'Within window'
            else:
                return False, 'Not within window'
        else:
            if schedule.start_time <= stamp.time() or stamp.time() <= schedule.end_time:
                return True, 'Within window'
            else:
                return False, 'Not within window'


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

        for t in [DefaultOption.objects.get(), self.job.config.transport, self.job.backup_area, self.job.host, self.job]:
            if t and t.rsync_options:
                args.append([x.value for x in t.rsync_options.all()])

        return args

    def cli(self):
        # ORIG
        # rsync
        # --archive --one-file-system --delete
        # -e '/usr/bin/ssh -i /root/.ssh/default.key'
        # --log-file=/backupz/log/monitoring/etc-2016-06-20_22-30.53.log
        # --exclude=".gvfs"
        # --timeout=3600
        # --exclude-from='/backupz/extras/global.excludes'
        # root@monitoring.metro.ucdavis.edu:"/etc" .

        # NEW
        # /usr/bin/rsync
        # --exclude-from='/backupz/extras/global.excludes'
        # --archive --delete --one-file-system --timeout=3600 --timeout=36000
        # --logfile='/var/log/backupz/phys-solid/var-spool-mail-2016-21-06_16-05.49.log'
        # --rsh='/opt/csw/bin/ssh -i /root/.ssh/default.key'
        # root@169.237.42.75:'/var/spool/mail'
        # /physics/backups/phys-solid/var-spool-mail
        transport = self.job.config.transport

        command = []
        command.append(transport.command)
        command.append(self.rsync_arguments())
        command.append("--logfile='%s'" % self.log_file())
        command.append("--rsh='%s -i %s'" % (self.job.config.ssh_command, self.job.config.ssh_key))
        command.append("%s@%s%s'%s'" % (self.job.config.user, self.job.host.ip, transport.separator, self.job.path.rstrip('/')))
        command.append(self.job.fs_path())

        pw = self.job.config.password
        if pw:
            os.environ['RSYNC_PASSWORD'] = pw

        return list(flatten(command))


    def __str__(self):  # __unicode__ on Python 2
        return "%s: %s @ %s" % (self.job.host.name, self.job.name, self.timestamp())

    class Meta:
        ordering = ('job__host__name', 'job__name', 'start',)
