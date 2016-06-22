#! /bin/zsh -eu
# -*-ksh-*-

setopt warn_create_global

PROGRAM=${0:t}

sudo ./database-clear.sh

rm backupz/migrations/*.py || true

./manage.py makemigrations

./manage.py makemigrations backupz

./manage.py migrate

./manage.py migrate backupz

./manage.py loaddata backupz_base

./manage.py createsuperuser --username $USER --email $USER@ucdavis.edu

./run.sh
