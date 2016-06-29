#! /bin/zsh -eu
# -*-ksh-*-

setopt warn_create_global

PROGRAM=${0:t}

cp -a ./database-clear.sh /tmp/
/tmp/database-clear.sh

rm backupz/migrations/*.py || true

find backupz/ -name \*.pyc -delete

./manage.py makemigrations

./manage.py makemigrations backupz

./manage.py migrate

./manage.py migrate backupz

./manage.py loaddata backupz_base

./manage.py createsuperuser --username $USER --email $USER@ucdavis.edu

./run.sh
