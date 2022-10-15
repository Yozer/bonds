import asyncio, discord, random, traceback, datetime, pytz, os
from database import update_bonds, get_interesting_bonds, mark_notified

client = discord.Client()
delay_min = 3 * 60
delay_max = 5 * 60
publish = True
check_dates = True
running = False

def enrich_embed(embed, bond):
    embed.description = f"[Szczegóły {bond['ticker']}](https://obligacje.pl/pl/obligacja/{bond['ticker']})"
    embed.add_field(name='Data wykupu', value= bond['buyback_date'].strftime("%Y-%m-%d"))
    embed.add_field(name='Emitent', value= bond['issuer'])

async def send_notifications(channel):
    try:
        new_bonds, better_bonds = get_interesting_bonds()
    except Exception as e:
        print(traceback.format_exc())
        if publish:
            await channel.send("Error when calculating bonds. Possibly an unexpected duplicate")
        return

    for bond in new_bonds:
        embed=discord.Embed(title=f"New bond {bond['ticker']}", color=0x0040ff)
        embed.add_field(name='YTM Netto', value=str(bond['ytm_net'])+ '%')
        embed.add_field(name='Cena', value= bond['price'])
        enrich_embed(embed, bond)
        if publish:
            await channel.send(embed=embed)
        print(f"New bond {bond['ticker']}")
        mark_notified(bond)

    for bond in better_bonds:
        embed=discord.Embed(title=f"{bond['ticker']} YTM {bond['prev_ytm_net']}% -> {bond['ytm_net']}%", color=0x00FF00)
        embed.add_field(name='Cena', value= f"{bond['prev_price']} -> {bond['price']}")
        enrich_embed(embed, bond)
        print(f"{bond['ticker']} YTM {bond['prev_ytm_net']}% -> {bond['ytm_net']}%")
        if publish:
            await channel.send(embed=embed)
        mark_notified(bond)

async def send_binance_notif(channel, binance):

    for asset in binance:
        avail = 'not available' if asset['sellOut'] else 'available'
        color = 0xCC0000 if asset['sellOut'] else 0x0040ff

        interest = ''
        if 'prev_interest' in asset:
            interest = ('%.2f' % (float(asset['prev_interest']) * 100))+ '% -> '
        interest += ('%.2f' % (float(asset['interest']) * 100))+ '%'
        embed=discord.Embed(title=f"Staking {asset['asset']}", color=color, description=avail)
        embed.add_field(name='Interest', value=interest)
        embed.add_field(name='Duration', value=asset['duration'] + ' days')
        embed.add_field(name='Type', value=asset['type'])
        if publish:
            await channel.send(embed=embed)
        print(f"New asset {asset['asset']}")

def should_fetch_bonds():
    if not check_dates:
        return True

    warsaw = pytz.timezone("Europe/Warsaw")
    dt = datetime.datetime.now(warsaw)
    start = datetime.time(8, 50)
    end = datetime.time(17, 10)

    if dt.weekday() >= 5:
        return False
    if start <= dt.time() <= end:
        return True
    return False

async def fetch_bonds_task():
    print("fetch_bonds_task running")
    await client.wait_until_ready()
    channel_bonds = discord.utils.get(client.get_all_channels(), name='obligacje')

    while not client.is_closed():
        try:
            if should_fetch_bonds():
                success = update_bonds()
                if not success and publish:
                    await channel_bonds.send('Failed to fetch bonds. Please check the logs.')
                    
                if success:
                    await send_notifications(channel_bonds)

        except Exception as e:
            print(f"Exception " + traceback.format_exc())

        await asyncio.sleep(random.randint(delay_min, delay_max))

@client.event
async def on_ready():
    global running

    print('Logged in as')
    print(client.user.name)
    print(client.user.id)
    print(f"Already running {running}")
    print('------')
    if not running:
        print("Starting loop")
        running = True
        client.loop.create_task(fetch_bonds_task()) # best to put it in here
        print("Started loop")

client.run(os.environ['DISCORD_KEY'])