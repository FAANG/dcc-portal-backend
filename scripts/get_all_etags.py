import aiohttp
import asyncio
import requests
from datetime import date
ETAG = []


def main():
    biosample_ids = fetch_biosample_ids()
    print(len(biosample_ids))
    asyncio.get_event_loop().run_until_complete(fetch_all_etags(biosample_ids))


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
    ETAG.append("{}\t{}".format(my_id, resp.headers.get('ETag')))


def fetch_biosample_ids():
    return requests.get("https://www.ebi.ac.uk/biosamples/accessions?project=FAANG&limit=100000").json()


if __name__ == "__main__":
    main()
    today = date.today().strftime('%Y-%m-%d')
    with open("etag_list_{}.txt".format(today), 'w') as w:
        for item in ETAG:
            w.write(item + "\n")
