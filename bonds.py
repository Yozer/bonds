import requests, datetime, pymongo, pytz, time, asyncio, aiohttp, traceback, os
from bson.decimal128 import Decimal128
from pymongo import MongoClient
from threading import Thread
from queue import Queue

min_ytd = 2
max_ytd = 15
min_maturity_days = 360
per_page = 10
base_url = 'https://gpw.notoria.pl/widgets/bonds/screener.php?'
concurrent = 4

search_params = f'&yf={min_ytd}&yt={max_ytd}&sort_name=name&sort_dir=up&blenf=0&blent=10&market[]=243&type[]=TB&type[]=CB&type[]=SB&type[]=MB&type[]=MG'

db_host = os.environ['MONGO_DATABASE_HOST'] if 'MONGO_DATABASE_HOST' in os.environ else 'localhost'
client = MongoClient(db_host, 27018, username=os.environ['MONGO_INITDB_ROOT_USERNAME'], password=os.environ['MONGO_INITDB_ROOT_PASSWORD'])
db = client.main.bonds

def try_get_bond(ticker):
    result = list(db.find({'ticker': ticker}).sort('fetch_date', -1).limit(1))
    return result[0] if len(result) == 1 else None

def get_gpw_headers():
    return {
        'Accept-Language': 'pl,en-US;q=0.9,en-GB;q=0.8,en;q=0.7', 
        'Referer': 'https://gpwcatalyst.pl/',
        'Origin': 'https://gpwcatalyst.pl',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'cross-site',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.69 Safari/537.36 Edg/89.0.774.39'}

def create_bos_session():
    s = requests.Session()
    s.headers.update({
        'Accept-Language': 'pl,en-US;q=0.9,en-GB;q=0.8,en;q=0.7', 
        'Referer': 'https://bossa.pl/',
        'Origin': 'https://bossa.pl',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'cross-site',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.69 Safari/537.36 Edg/89.0.774.39'})

    return s

def get_bos_bonds(fetch_date):
    s = create_bos_session()
    url = 'https://api.30.bossa.pl/API/GPW/v2/Q/C/_cat_bonds'

    response = s.get(url).json()['_d'][0]['_t']
    active_bonds = [b for b in response if b['_ask_size'] is not None]

    for b in active_bonds:
        try:
            item = {
                    'ticker': b['_symbol'],
                    'issuer': None,
                    'buyback_date': None,
                    'fetch_date': fetch_date,
                    'price': Decimal128(b['_ask_size']),
                    'ytm_net': None}
            yield item
        except:
            pass

async def call_calculator(bond, s):
    if bond['ytm_net'] is not None:
        return

    url_format = 'https://gpw.notoria.pl/widgets/bonds/calculator.php?id={0}&price={1}&type={2}&b_fee=0.0019&t_fee=0.19'
    async with s.get(url_format.format(bond['ticker'], str(bond['price']), bond['type'])) as response:
        response = await response.json()
        if 'calculator' not in response:
            return False

        response = response['calculator']
        bond['ytm_net'] = Decimal128(str(response['ytm_net']))

    print(f"Calculated YTD for {bond['ticker']}")
    return True

async def call_details(bond, s):
    url_format = 'https://gpw.notoria.pl/widgets/bonds/profile.php?id=' + bond['ticker']
    async with s.get(url_format) as response:
        response = await response.json()
        response = response['bonds']
        bond['issuer'] = response['issuer']
        bond['buyback_date'] = datetime.datetime.strptime(response['info']['maturity_date'], '%Y-%m-%d')
        bond['type'] = response['info']['code']
    print(f"Fetched details for {bond['ticker']}")

async def calculate_details_bond_async(q):
    async with aiohttp.ClientSession(headers=get_gpw_headers()) as session:
        while True:
            bond = q.get()
            if bond is None:
                return

            tries = 5
            success = False
            bond_db = try_get_bond(bond['ticker'])

            while tries >= 0:
                try:
                    if bond_db is None:
                        await call_details(bond, session)
                    else:
                        bond['issuer'] = bond_db['issuer']
                        bond['buyback_date'] = bond_db['buyback_date']
                        bond['type'] = bond_db['type']

                    if bond_db is None or bond['price'].to_decimal() != bond_db['price'].to_decimal():
                        calced = await call_calculator(bond, session)
                        if not calced:
                            bond['ytm_net'] = Decimal128("0")
                    else:
                        bond['ytm_net'] = bond_db['ytm_net']
                    success = True
                    break
                except Exception as e:
                    print(f"Exception for {bond['ticker']} " + traceback.format_exc())
                    await asyncio.sleep(2)
                    tries -= 1

            bond['success'] = success
            # print(f"Processed {bond['ticker']}")

def calculate_details_bond(q):

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    loop.run_until_complete(calculate_details_bond_async(q))
    loop.close()

def calculate_details(bonds_bos):
    q = Queue(len(bonds_bos) + concurrent + 5)
    threads = []
    for i in range(concurrent):
        t = Thread(target=calculate_details_bond, args=(q,))
        t.start()
        threads.append(t)

    for bond in bonds_bos:
        q.put(bond)

    for i in range(concurrent):
        q.put(None)

    for t in threads:
        t.join()

def fetch_bonds():
    fetch_date = datetime.datetime.utcnow()
    bonds_bos = list(get_bos_bonds(fetch_date))

    calculate_details(bonds_bos)
    return [b for b in bonds_bos if b['success']]
