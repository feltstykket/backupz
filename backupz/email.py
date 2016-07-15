"""
The beginning of a module to send emails.

Note, the module just gathers messages to send. No email is generated until the send() method is called.
"""

from django.core.mail import EmailMessage

from backupz.humanize import *

from . import lib


class Addrs(object):
    def __init__(self, fromaddr, toaddr, ccaddrs=None):
        self.fromaddr = fromaddr
        self.toaddr = toaddr
        self.ccaddrs = ccaddrs


    def __hash__(self):
        return hash(self.fromaddr + self.toaddr + (self.ccaddrs or ''))


    def __eq__(self, other):
        return self.fromaddr == other.fromaddr and self.toaddr == other.toaddr and self.ccaddrs == other.ccaddrs


    def __str__(self):
        return 'From: %s To: %s CC: %s' % (self.fromaddr, self.toaddr, self.ccaddrs)


class Email(object):
    def __init__(self):
        self.data = {}


    def quota(self, job, zfsquota, fromaddr, toaddr, ccaddrs=None, extra=None):
        # TODO: store this in the database to eliminate duplicate email every time the scheduler runs, or only let run once a day.

        if ccaddrs:
            # Eliminate the To: from the CC: list and make a string
            ccaddrs = ', '.join(sorted([x for x in ccaddrs if x != toaddr]))

        # Key around the combination of all email addresses to group into a single email to each client.
        key = Addrs(fromaddr, toaddr, ccaddrs)
        if key not in self.data:
            self.data[key] = {}

        if job.host not in self.data[key]:
            self.data[key][job.host] = {}

        if job not in self.data[key][job.host]:
            self.data[key][job.host][job] = {}

        if 'quota' not in self.data[key][job.host][job]:
            self.data[key][job.host][job]['quota'] = (zfsquota, extra)


    def format_quota(self, key, host, job):
        body = ''
        if not 'quota' in self.data[key][host][job]:
            return body

        zfsquota = self.data[key][host][job]['quota'][0]
        extra = self.data[key][host][job]['quota'][1]
        body += "\t%s is over quota" % job.name
        if extra:
            body += extra
        body += "\n"
        body += "\t\t%s >= %s in %s (%s)\n" % (humanize(zfsquota.used), zfsquota.quota, zfsquota.where, zfsquota.zfs_path)

        return body


    def send(self):
        for key in self.data.keys():
            body = ''
            hosts = []
            for host in self.data[key].keys():
                hosts.append(str(host))
                body += "%s:\n" % host
                for job in self.data[key][host].keys():
                    body += self.format_quota(key, host, job)

            hosts = ', '.join(hosts)

            ccaddrs = None
            if key.ccaddrs:
                ccaddrs = [key.ccaddrs]

            email = EmailMessage(subject='BackupZ report for: %s' % hosts, body=body, to=[key.toaddr], from_email=key.fromaddr, cc=ccaddrs)
            email.send()

        return lib.Status(True)
