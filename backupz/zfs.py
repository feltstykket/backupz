"""
The beginning of a module to handle the BackupZ ZFS calls.
"""

import collections
import functools

from . import lib

class ZFS(object):
    zfs_command = '/sbin/zfs'
    zpool_command = '/sbin/zpool'

    def __init__(self):
        pass

    @functools.lru_cache(maxsize=128, typed=False)
    def isfilesystem(self, path, create=True):
        r = lib.run_command([ZFS.zfs_command, 'list', '-o', 'name', '-H', path])
        if create and r.rc == 1 and r.err.rstrip().endswith('dataset does not exist'):
            return self.create_filesystem(path)

        if r.rc != 0 or r.out.rstrip() != path:
            return False
        else:
            return True

    def create_filesystem(self, path):
        # cannot create 'z3/omen': dataset already exists
        # cannot create 'descolada/backups/backupz/backupsNONONO': dataset already exists
        r = lib.run_command([ZFS.zfs_command, 'create', path])
        if r.rc != 0 and r.err.rstrip() != "cannot create '%s': dataset already exists" % path:
            raise RuntimeError(r.err)

        return True
