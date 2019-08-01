import aiohttp
import asyncio
import requests
from datetime import date
ETAG = []
ETAG_IDS = []


def main():
    biosample_ids = fetch_biosample_ids()
    asyncio.get_event_loop().run_until_complete(fetch_all_etags(biosample_ids))
    if len(biosample_ids) != len(ETAG_IDS):
        for my_id in biosample_ids:
            if my_id not in ETAG_IDS:
                resp = requests.get("http://www.ebi.ac.uk/biosamples/samples/{}".format(my_id)).headers
                if 'ETag' in resp and resp['ETag']:
                    ETAG.append("{}\t{}".format(my_id, resp['ETag']))


async def fetch_all_etags(ids):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for my_id in ids:
            task = asyncio.ensure_future(fetch_etag(session, my_id))
            tasks.append(task)
        await asyncio.gather(*tasks, return_exceptions=True)


async def fetch_etag(session, my_id):
    url = "http://www.ebi.ac.uk/biosamples/samples/{}".format(my_id)
    resp = await session.get(url)
    etag_value = resp.headers.get('ETag')
    if etag_value:
        ETAG.append("{}\t{}".format(my_id, etag_value))
    ETAG_IDS.append(my_id)


def fetch_biosample_ids():
    return requests.get("https://www.ebi.ac.uk/biosamples/accessions?project=FAANG&limit=100000").json()


if __name__ == "__main__":
    main()
    today = date.today().strftime('%Y-%m-%d')
    with open("etag_list_{}.txt".format(today), 'w') as w:
        for item in sorted(ETAG):
            w.write(item + "\n")
