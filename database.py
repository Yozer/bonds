from bonds import fetch_bonds, min_ytd, max_ytd, min_maturity_days
import pymongo, traceback
from pymongo import MongoClient
import datetime, os

db_host = os.environ['MONGO_DATABASE_HOST'] if 'MONGO_DATABASE_HOST' in os.environ else 'localhost'
client = MongoClient(db_host, 27018, username=os.environ['MONGO_INITDB_ROOT_USERNAME'], password=os.environ['MONGO_INITDB_ROOT_PASSWORD'])
db = client.main.bonds

def init(db):
    db.create_index(
        [('ticker', pymongo.DESCENDING),
        ('price', pymongo.DESCENDING),
        ('ytm_net', pymongo.DESCENDING)], 
    unique=True)

def mark_notified(bond):
    db.update_one({'ticker': bond['ticker'], 'price': bond['price'], 'ytm_net': bond['ytm_net']}, {'$set': {'notified': True}})

def get_dict(snapshot):
    flatten = set([s['ticker'] for s in snapshot])
    if len(flatten) != len(snapshot):
        raise Exception(f"Found duplicates in snapshot {snapshot['fetch_date']}")

    return dict([(s['ticker'], s) for s in snapshot])

def get_new_bonds(last, prev):
    for bond in last.values():
        if bond['ticker'] in prev:
            continue

        yield bond

def get_betters_bonds(last, prev):
    for bond in last.values():
        if bond['ticker'] not in prev:
            continue

        prev_bond = prev[bond['ticker']]
        if prev_bond['ytm_net'].to_decimal() < bond['ytm_net'].to_decimal():
            bond['prev_ytm_net'] = prev_bond['ytm_net']
            bond['prev_price'] = prev_bond['price']
            yield bond

def filter_bonds(bonds):
    d = (datetime.datetime.utcnow() + datetime.timedelta(days=min_maturity_days))
    tmp = [b for b in bonds if b['buyback_date'] <= d and not b['notified']]
    tmp = [b for b in tmp if b['ytm_net'].to_decimal() >= min_ytd and b['ytm_net'].to_decimal() <= max_ytd]
    # tmp = [b for b in tmp if 'CB' in b['type']]
    return tmp

def get_interesting_bonds():
    last_date = list(db.find().sort('fetch_date', -1).limit(1))[0]['fetch_date']
    prev_date = list(db.find({'fetch_date': {'$ne': last_date}}).sort('fetch_date', -1).limit(1))
    if len(prev_date) == 0:
        prev_date = None
    else:
        prev_date = prev_date[0]['fetch_date']

    last_snapshot = get_dict(list(db.find({'fetch_date': last_date})))
    prev_snapshot = get_dict(list(db.find({'fetch_date': prev_date})))

    new_bonds = list(get_new_bonds(last_snapshot, prev_snapshot))
    better_bonds = list(get_betters_bonds(last_snapshot, prev_snapshot))

    return (filter_bonds(new_bonds), filter_bonds(better_bonds))

def update_bonds():
    init(db)
    try:
        bonds = list(fetch_bonds())
    except Exception as e:
        print(traceback.format_exc())
        return False
    
    for bond in bonds:
        if 'success' in bond:
            del bond['success']
        
        bond['notified'] = False
        exists = list(db.find({'ticker': bond['ticker'], 'price': bond['price'], 'ytm_net': bond['ytm_net']}))
        if len(exists) != 0:
            db.update_one({'ticker': bond['ticker'], 'price': bond['price'], 'ytm_net': bond['ytm_net']}, {'$set': {'fetch_date': bond['fetch_date']}})
        else:
            db.insert_one(bond)

    return True
