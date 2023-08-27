from contextlib import contextmanager
from datetime import datetime
import sqlite3


things_table = 'things'

@contextmanager
def iota_db(path):
    db = sqlite3.connect(path)
    try:
        yield db
    finally:
        db.commit()
        db.close()


def initialize(db):
    """Initialize iota database non-destructively."""
    cmd = f'CREATE TABLE IF NOT EXISTS {things_table} (name TEXT)'

    execute(db, cmd)


def get_things(db):
    """Get things in iota."""
    cmd = f'SELECT name FROM {things_table}'

    res = execute(db, cmd)
    things = sorted((item[0] for item in res.fetchall()))

    return things


def add_thing(db, descriptor):
    """Add thing to iota."""
    new_thing = descriptor['id']
    if new_thing in get_things(db):
        # thing already exists.
        return False

    # Register thing.    
    cmd = f'INSERT INTO {things_table} VALUES ("{new_thing}")'

    execute(db, cmd)

    # Create table for thing.
    columns = sorted((k for k in descriptor.keys() if k != 'id'))
    cmd = f'CREATE TABLE {new_thing} (timestamp INTEGER, '
    cmd += ', '.join((f'{k} {descriptor[k]}' for k in columns))
    cmd += ')'

    execute(db, cmd)
    

    return True
    

def get_latest_thing_entry(db, thing):
    """Get most recent thing entry."""
    columns = table_columns(db, thing)

    cmd = f'SELECT * FROM {thing} WHERE ROWID IN (SELECT max(ROWID) FROM {thing})'

    res = execute(db, cmd)
    entries = [dict(zip(columns, e)) for e in res]

    return entries[-1] if len(entries) > 0 else []


def get_most_recent_thing_entries(db, thing, n):
    """Get `n` most recent thing data entries"""
    columns = table_columns(db, thing)
    
    cmd = f'SELECT * from {thing} WHERE ROWID > (SELECT max(ROWID) FROM {thing}) - {n}'

    res = execute(db, cmd)
    entries = [dict(zip(columns, e)) for e in res]

    return entries


def get_thing_entries(db, thing, start, finish):
    """Get range of thing data entries based on timestamp."""
    columns = table_columns(db, thing)
    
    key = 'timestamp'
    start_date = datetime.fromisoformat(start)
    finish_date = datetime.fromisoformat(finish)
    cmd = f'SELECT * FROM {thing} WHERE {key} >= {start_date.timestamp()} AND {key} <= {finish_date.timestamp()}'

    res = execute(db, cmd)
    entries = [dict(zip(columns, e)) for e in res]

    return entries


def add_to_thing(db, thing, datapoint):
    """Add datapoint to thing."""
    timestamp = datetime.now().timestamp()
    columns = sorted((k for k in datapoint.keys() if k != 'id'))
    cmd = f'INSERT INTO {thing} VALUES ({timestamp}, '
    cmd += ', '.join((f'{datapoint[k]}' for k in columns))
    cmd += ')'

    execute(db, cmd)


def table_columns(db, table):
    """Get column names for table"""
    cmd = f'SELECT name FROM pragma_table_info("{table}")'

    res = execute(db, cmd)
    entries = [r[0] for r in res]

    return entries


def execute(db, cmd):
    """Execute SQL command"""
    print(f'SQL: {cmd}')

    cur = db.cursor()
    return cur.execute(cmd)
