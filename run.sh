#! /bin/zsh -eu
# -*-ksh-*-

setopt warn_create_global

PROGRAM=${0:t}

./manage.py makemigrations

./manage.py migrate backupz

./manage.py runserver "$@"
