import aiohttp
import asyncio
import json
import random
import sqlite3
conn = sqlite3.connect('demo_arbeitsagentur.db')

# Create a cursor object
c = conn.cursor()

# Create a new table
c.execute('''
    CREATE TABLE IF NOT EXISTS data (
        id TEXT PRIMARY KEY,
        title TEXT,
        type TEXT,
        company TEXT,
        plz TEXT,
        ort TEXT,
        strasse TEXT,
        region TEXT,
        land TEXT,
        time_up NUMERIC,
        link TEXT,
        time_end NUMERIC,
        salary INTEGER
    )
''')
cookies = {
    'cookie_consent': 'accepted',
    'personalization_consent': 'accepted',
    'marketing_consent': 'accepted',
    '_pk_id.35.cfae': '766ae640a443e040.1714126276.',
    '_pk_ses.35.cfae': '1',
    'rest': '024dae17b4-4300-45RMraRjeRco-n6uHyjZohm3xzppT_5OmZSlWhuOt0OFfdKGYl6RWPhAhUolxMheS9grY',
}

headers = {
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
    'Connection': 'keep-alive',
    'Origin': 'https://www.arbeitsagentur.de',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-site',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'X-API-Key': 'jobboerse-jobsuche',
    'correlation-id': '90995c40-848b-99ec-3bfb-2d038319a8a5',
    'sec-ch-ua': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
}
 
params = {
    'angebotsart': '1',
    'page': '1',
    'size': '100',
    'pav': 'false',
    'facetten': 'false',
}

num_pages = 10

async def fetch(session, url):
    params['page'] = str(url)
    async with session.get('https://rest.arbeitsagentur.de/jobboerse/jobsuche-service/pc/v4/jobs', params=params, cookies=cookies, headers=headers) as response:
        return await response.text()

async def main():
    urls = [i for i in range(1, num_pages + 1)]
    tasks = []
    async with aiohttp.ClientSession() as session:
        for url in urls:
            tasks.append(fetch(session, url))
        pages_content = await asyncio.gather(*tasks)
        for page_content in pages_content:
            data = json.loads(page_content)
            for key, value in data.items():
                if isinstance(value, list):
                    for item in value:
                            obj = {}
                            obj['id'] = item.get('refnr', '')
                            obj['title'] = item.get('titel', '')
                            obj['type'] = item.get('beruf', '')
                            obj['company'] = item.get('arbeitgeber', '')
                            obj['plz'] = item['arbeitsort'].get('plz', '') 
                            obj['ort'] = item['arbeitsort'].get('ort', '')
                            obj['strasse'] = item['arbeitsort'].get('strasse', '')  
                            obj['region'] = item['arbeitsort'].get('region', '')
                            obj['land'] = item['arbeitsort'].get('land', '')
                            obj['time_up'] = item.get('modifikationsTimestamp','')
                            obj['time_end'] = item.get('eintrittsdatum', '')  
                            obj['link'] = 'https://www.arbeitsagentur.de/jobsuche/jobdetail/' + item.get('refnr', '')
                            obj['salary'] = random.randint(1000, 10000)
                            c.execute("SELECT * FROM data WHERE id = ?", (obj['id'],))
                            result = c.fetchone()
                            if result is None:
                                c.execute('''INSERT INTO data (id, title, type, company, plz, ort, strasse, region,land, time_up, link, time_end, salary )VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', (obj['id'], obj['title'], obj['type'], obj['company'], obj['plz'], obj['ort'], obj['strasse'], obj['region'], obj['land'], obj['time_up'], obj['link'], obj['time_end'], obj['salary']))
                                conn.commit()
    conn.close()

asyncio.run(main())
