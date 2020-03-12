import aiohttp
import asyncio
import requests
from datetime import date
ETAG = []
ETAG_IDS = []
ACCESSION_API = 'https://www.ebi.ac.uk/biosamples/accessions?filter=attr:project:FAANG&size=100000'

def main():
    biosample_ids = fetch_biosample_ids()
    if len(biosample_ids) < 5000:
        print(f"The number of returned BioSamples accessions is {len(biosample_ids)}, "
              f"less than 5000 and very suspicious.\n"
              f"Please manually check {ACCESSION_API}")
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
    result = requests.get(ACCESSION_API).json()
    return result['_embedded']['accessions']


if __name__ == "__main__":
    main()
    today = date.today().strftime('%Y-%m-%d')
    with open("etag_list_{}.txt".format(today), 'w') as w:
        for item in sorted(ETAG):
            w.write(item + "\n")
