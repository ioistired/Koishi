# Koishi

A personal discord bot

## Running

Discord verification requirements has made it far too cumbersome to run.
The source is only provided here as a reference.

## Installation

```
psql -c 'CREATE DATABASE koishi;'
psql koishi -c 'CREATE SCHEMA koi_test;'
PGOPTIONS=--search_path=koi_test,public psql koishi -f schema.sql
cp config.example.json config.json
# edit config.json accordingly
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
./bot.py
```

Also make sure you have the Arial font installed. This is included on Windows, and on Linux you can install
it by getting the "ms core fonts" package for your distribution.
