from django.contrib import admin
import solo.admin
from .models import *

import django.forms

# Register your models here.
admin.site.register(Area)
admin.site.register(Host)
admin.site.register(Job)

admin.site.register(Schedule)
admin.site.register(PostBackupScript)
admin.site.register(SnapshotsToKeep)
admin.site.register(RsyncOption)
admin.site.register(Transport)
admin.site.register(Backup)
admin.site.register(Owner)

admin.site.register(DefaultOption, solo.admin.SingletonModelAdmin)
