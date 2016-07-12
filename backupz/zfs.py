"""
The beginning of a module to handle the BackupZ ZFS calls.
"""

import collections
import functools

from . import lib


class ZFS(object):
    commands = {'zfs': '/sbin/zfs',
                'zpool': '/sbin/zpool'}

    def __init__(self):
        pass

    @functools.lru_cache(maxsize=128, typed=False)
    def isfilesystem(self, path):
        # cannot open 'a/b/c/d': dataset does not exist
        #
        # NAME                        USED  AVAIL  REFER  MOUNTPOINT
        # descolada/backups/backupz   480K   417G    96K  /physics

        r = lib.run_command([ZFS.commands['zfs'], 'list', '-o', 'name', '-H', path])

        if r.rc != 0 or r.out.rstrip() != path:
            return False
        else:
            return True

    def create_filesystem(self, path):
        # cannot create 'z3/omen': dataset already exists
        # cannot create 'descolada/backups/backupz/backupsNONONO': dataset already exists

        if self.isfilesystem(path):
            return True

        r = lib.run_command([ZFS.commands['zfs'], 'create', path])

        if r.rc != 0 and r.err.rstrip() != "cannot create '%s': dataset already exists" % path:
            # All errors but dataset already exist is terminal
            raise RuntimeError(r.err)

        return True

    @functools.lru_cache(maxsize=128, typed=False)
    def used(self, path):
        # zfs list -o used -H -p physics/backups/phys-solid/etc

        r = lib.run_command([ZFS.commands['zfs'], 'list', '-o', 'used', '-H', '-p', path])
        return int(r.out)
