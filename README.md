[![Build Status](https://travis-ci.org/adsabs/myADSPipeline.svg)](https://travis-ci.org/adsabs/myADSPipeline)
[![Coverage Status](https://coveralls.io/repos/adsabs/myADSPipeline/badge.svg)](https://coveralls.io/r/adsabs/myADSPipeline)

# myADSPipeline

## Short summary

This pipeline processes daily and weekly myADS notifications and sends customized emails to registered users.

### Config options for users
* active = **true**/**false**
* frequency = **daily**/**weekly** (traditionally used for daily arXiv notifications and weekly astronomy database updates)
* type = **templated**/**query** (**templated** refers to notifications set up using guided Classic-style notifications -
user supplies keywords or authors of interest and a query is constructed for them; **query** refers to free-form
notifications that allow users to construct the query and select desired options)
* stateful = **true**/**false** (**true** returns only new results, **false** returns all results)
* name = string (for **query** setups, this is user-defined; for **templated** setups, this is pipeline-defined)
* qid = string (pointer to entry in either Query table (**query** notifications) or myADS table (**templated** notifications))

## Queues
* process: processes notifications of the given frequency for a single user (fetches myADS setup, executes queries for notifications
of the given frequency, processes stateful results if necessary, builds and sends HTML email)

## Setup (recommended)

    `$ virtualenv python`
    `$ source python/bin/activate`
    `$ pip install -r requirements.txt`
    `$ pip install -r dev-requirements.txt`
    `$ vim local_config.py` # edit, edit
    `$ alembic upgrade head` # initialize database

## Note
Two cron jobs are needed, one with the daily flag turned on (processes M-F), one with the weekly flag turned on (processes after weekly ingest is complete)
