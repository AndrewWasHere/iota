import datetime

from flask import Flask, request

import iota

def create_app():
    """Create web server"""
    db_path = 'iota.sqlite'
    with iota.iota_db(db_path) as db:
        iota.initialize(db)

    app = Flask('iota')

    ##########
    # Browser endpoints
    @app.get('/')
    def get_dashboard():
        return 'TODO'

    @app.get('/things/<thing_id>')
    def get_thing_info(thing_id):
        return 'TODO'

    ##########
    # API endpoints

    @app.get('/api/things')
    def get_things():
        with iota.iota_db(db_path) as db:
            result = iota.get_things(db)

        return result
    
    @app.post('/api/things')
    def add_thing():
        with iota.iota_db(db_path) as db:
            thing = request.get_json()
            result = iota.add_thing(db, thing)

        return ('', 201) if result else (f'{thing} does not exist', 400)

    @app.get('/api/things/<thing_id>')
    def get_thing(thing_id):
        p = request.args
        if 'last' in p.keys():
            try:
                last = int(p['last'])
            except ValueError:
                return '`last` query value must be an integer', 400
            
            try:
                with iota.iota_db(db_path) as db:
                    result = iota.get_most_recent_thing_entries(db, thing_id, last)

                return result
            except Exception as e:
                print(f'*** Iota Error: {e}')
                return 'You broke my database.', 500

        elif 'from' in p.keys() or 'to' in p.keys():
            if not 'from' in p.keys() and 'to' in p.keys():
                return '`from` and `to` query parameters must both be present', 400
            
            try:
                start = datetime.date.fromisoformat(p['from'])
                finish = datetime.date.fromisoformat(p['to'])
            except Exception as e:
                return '`from` and `to` query values must be dates of the form y-m-d', 400
            
            try:
                with iota.iota_db(db_path) as db:
                    results = iota.get_thing_entries(db, thing_id, start, finish)

                return results
            except Exception as e:
                print(f'*** Iota Error: {e}')
                return 'You broke my database.', 500
        else:
            try:
                with iota.iota_db(db_path) as db:
                    results = iota.get_latest_thing_entry(db, thing_id)

                return results
            except Exception as e:
                print(f'***Iota Error: {e}')
                return 'You broke my database.', 500

    @app.put('/api/things/<thing_id>')
    def update_thing(thing_id):
        try:
            with iota.iota_db(db_path) as db:
                result = iota.get_things(db)
            
            if thing_id not in result:
                return f'{thing_id} not found', 400
            
            datapoint = request.get_json()
            with iota.iota_db(db_path) as db:
                iota.add_to_thing(db, thing_id, datapoint)
        except Exception as e:
            print(f'*** Iota Error: {e}')
            return 'You broke my database.', 500
        
        return '', 204

    return app
