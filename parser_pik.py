import requests
from sqlalchemy.dialects.postgresql import insert

from datetime import datetime

import scrape_postgres as pg

headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7,ja;q=0.6,zh-CN;q=0.5,zh;q=0.4',
    'Connection': 'keep-alive',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"'
}

def scrape_full():
    next_page = "https://flat.pik-service.ru/api/v1/filter/flat?type=1,2&location=2,3&flatLimit=50&sortBy=price&orderBy=asc&allFlats=1&flatPage=1"
    pagenum = 1
    offset = 0
    total_cnt = 0
    errors = []
    while True:
        page = requests.get(next_page, headers=headers)
        res = page.json()
        print(f"pik {offset+1}..{offset+20}/{res['data']['stats']['count']}")
        next_page = f"https://flat.pik-service.ru/api/v1/filter/flat?type=1,2&location=2,3&flatLimit=50&sortBy=price&orderBy=asc&allFlats=1&flatPage={pagenum}"
        offset += 20
        pagenum += 1

        cnt, error_list = parse_json(res)
        total_cnt += cnt
        errors.extend(error_list)

        if len(res['data']['items']) == 0:
            break
    return f"Inserted: {total_cnt}, errors: {str(errors)[:1000]}"


def parse_json(raw):
    errors_list = []
    cnt = 0
    for item in raw['data']['items']:
        try:
            insert_stmt = insert(pg.table_flats).values(
                estate=item['blockSlug'],
                city="msk",
                article=item['guid'],
                website_id=item['id'],
                booked=(item['status'] != "free"),
                area=item['area'],
                price=item['price'],
                ppm=item['meterPrice'],
                realty_class=item['bulkIsPremium'] == True,
                furnish=convert_furnish(item['finishType']).name,
                rooms=item['rooms'],
                floor_number=item['floor'],
                finish_quarter=None,
                finish_year=datetime.strptime(item['settlementDate'], "%Y-%m-%dT%H:%M:%S%z").year if item['settlementDate'] else None,
                link=f"https://www.pik.ru/flat/{item['id']}",
                raw=item
            )
            pg.connection.execute(insert_stmt)
            pg.connection.commit()
            cnt += 1
        except Exception as e:
            errors_list.append(e)
    return (cnt, errors_list)


def convert_class(bulk_is_premium):
    match bulk_is_premium:
        case True:
            return pg.RealtyClass.COMFORT_PLUS
        case False:
            return pg.RealtyClass.COMFORT
        case _:
            return pg.RealtyClass.UNKNOWN


def convert_furnish(finish):
    match finish:
        case 0:
            return pg.Furnish.NOTHING
        case 1:
            return pg.Furnish.WHITEBOX_PLUS
        case 2:
            return pg.Furnish.WHITEBOX
        case 3:
            return pg.Furnish.FURNITURE
        case _:
            return pg.Furnish.UNKNOWN
