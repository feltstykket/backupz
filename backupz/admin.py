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
admin.site.register(PI)

admin.site.register(DefaultOption, solo.admin.SingletonModelAdmin)

# @admin.register(Area)
# @admin.register(Host)
# @admin.register(Job)
# class WeekdaysAdmin(admin.ModelAdmin):
#     formfield_overrides = {
#         WeekdayField: {'widget': django.forms.CheckboxSelectMultiple},
#     }
# @admin.register(DefaultOption)
# class AreaAdmin(solo.admin.SingletonModelAdmin):
#     formfield_overrides = {
#         WeekdayField: {'widget': django.forms.CheckboxSelectMultiple},
#     }
