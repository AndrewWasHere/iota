import datetime

import iota

def test_iota_db():
    with iota.iota_db(':memory:') as db:
        # Initialization creates a things table
        iota.initialize(db)

        c = db.cursor()
        assert 'things' in [r[0] for r in c.execute('SELECT name FROM sqlite_master')]

        # Add a thing to iota.
        thing = 'thing_1'
        d = {
            'id': thing,
            'temperature': 'FLOAT'
        }
        iota.add_thing(db, d)

        assert thing in iota.get_things(db)
        assert iota.get_latest_thing_entry(db, thing) == []

        # Add entry from thing.
        d = { 'temperature': 42.0 }
        iota.add_to_thing(db, thing, d)

        entry = iota.get_latest_thing_entry(db, thing)
        assert d['temperature'] == entry['temperature']
        assert entry['timestamp'] >= datetime.datetime.fromisoformat(datetime.date.today().isoformat()).timestamp()

        entries = iota.get_most_recent_thing_entries(db, thing, 2)
        assert len(entries) == 1
        assert d['temperature'] in [e['temperature'] for e in entries]

        missing_temp = d['temperature']
        new_temps = [ { 'temperature': 52.0 }, { 'temperature': 62.0 }]
        for d in new_temps:
            iota.add_to_thing(db, thing, d)

        entries = iota.get_most_recent_thing_entries(db, thing, 2)
        entry_temps = [e['temperature'] for e in entries]
        assert len(entries) == 2
        for d in new_temps:
            assert d['temperature'] in entry_temps

        assert missing_temp not in entry_temps

        today = f'{datetime.date.today()}'
        tomorrow = f'{datetime.date.today() + datetime.timedelta(days=1)}'
        entries = iota.get_thing_entries(db, thing, today, tomorrow)
        entry_temps = [e['temperature'] for e in entries]
        assert len(entries) == 3
        for t in ([missing_temp] + [e['temperature'] for e in new_temps]):
            assert t in entry_temps
