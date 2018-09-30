### Content Pipeline

This repo contains the code for creating a data pipeline to migrate data to new content management system:

* `gcms_git_repos.py` -- clones or pulls required repos.
* `gcms_load_db.py` -- parses, cleans and transforms the text segments and loads them to the Oracle database.
