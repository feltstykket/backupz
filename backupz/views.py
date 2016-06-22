import traceback
from django.template.response import TemplateResponse
from django.shortcuts import render, get_object_or_404, redirect, HttpResponse
import django.forms
from django.contrib import messages
import django.core.exceptions
import django.http
from django.db import transaction

import datetime

from .models import *
from .lib import *

def messages_traceback(request, comment=None):
    base = 'Sorry, a serious error occurred, please copy this box and send it to <tt>ucd-puppet-admins@ucdavis.edu</tt>'

    if comment:
        base = base + '<br/>' + comment

    messages.error(request, base + '<br/><pre>' + traceback.format_exc() + '</pre>', extra_tags='safe')

def get_loginid(request):
    if request.user.is_superuser:
        return request.user.username
    else:
        return request.META['REMOTE_USER']


def get_user_info(request):
    previous_login = None
    previous_ip = None
    user = None

    try:
        loginid = get_loginid(request)
        user = User.objects.get(loginid=loginid)
    except KeyError as e:
        if e.args[0] == 'REMOTE_USER':
            return (None, 
                    TemplateResponse(request, 'backupz/403.html',
                                     {'error': 'You must login via CAS to access this site.'}, status=403))
        else:
            return (None, 
                    TemplateResponse(request, 'backupz/401.html', {'error': '%s: %s' % (type(e), e)}, status=401))
    except Exception as e:
        messages_traceback(request)
        return (None, 
                TemplateResponse(request, 'backupz/401.html', {'error': 'Generic Error, the very best kind'},
                                 status=401))

    return (user, None)


def index(request):
    user, err = get_user_info(request)
    if err:
        return err

    return render(request,
                  'backupz/index.html',
                  {
                   }
                  )
