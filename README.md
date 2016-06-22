# backupz

A reimplementation in Python/Django of the Perl based BackupZ used by
a couple departments at UC Davis.

Models looks mostly right, but will still need tuneup.
    
Use ./database-init.sh to initialize the Postgres database.
    
Use ./INIT.sh to blow away everything in the DB and start from scratch.
    
Use ./run.sh to update the DB models and run the debug server.
