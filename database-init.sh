#! /bin/zsh -eu

if [[ $USER != "postgres" ]]
then
  eval exec sudo -u postgres $0 "$@"
fi

export USER=django
export DB=$USER

createuser --echo --encrypted --pwprompt $USER
createdb --echo --template=template0 --encoding UTF8 --owner $USER $DB
