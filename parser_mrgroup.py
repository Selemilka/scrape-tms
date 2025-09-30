import requests
from sqlalchemy.dialects.postgresql import insert

from datetime import datetime

import scrape_postgres as pg

# WARNING!! NEED TO UPDATE COOKIE TODO
headers = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7,ja;q=0.6,zh-CN;q=0.5,zh;q=0.4",
    "authorization": "",
    "baggage": "sentry-environment=production,sentry-public_key=64d42d1ec99f4044ff0df570a905dbca,sentry-trace_id=098335f0bee64424b3ad883c4f02ad2f,sentry-sample_rate=0.3,sentry-transaction=%2Fflats%2F*,sentry-sampled=true",
    "cookie": "spid=1737226624293_93c20a02a8ec8773db78f4c00e3aa98c_2idw1h9dv47ubxqf; _ga=GA1.1.715374469.1737226627; _ym_uid=1737226630629593499; _ym_d=1737226630; _ym_isad=1; _gcl_au=1.1.1780867920.1737226631; mindboxDeviceUUID=52618eb9-930b-4a69-9d24-abe38b5fb032; directCrm-session=%7B%22deviceGuid%22%3A%2252618eb9-930b-4a69-9d24-abe38b5fb032%22%7D; uxs_uid=063831a0-d5ce-11ef-acef-415588940a8a; popmechanic_sbjs_migrations=popmechanic_1418474375998%3D1%7C%7C%7C1471519752600%3D1%7C%7C%7C1471519752605%3D1; USE_COOKIE_CONSENT_STATE={%22session%22:true%2C%22persistent%22:true%2C%22necessary%22:true%2C%22preferences%22:true%2C%22statistics%22:true%2C%22marketing%22:true%2C%22firstParty%22:true%2C%22thirdParty%22:true}; spsc=1737297705152_f0023f1474b0378e23f231178b8abe89_e6cfb3ea8f0a0fa28cc6ebefdcae8ea5; PHPSESSID=33r1ndiej3on0tfji1j5vq1ilq; _cmg_csstvfLiQ=1737297709; _comagic_idvfLiQ=9698776203.13846381473.1737297709; sessionId=17372977099805560852; _ym_visorc=b; _ga_H5S7YBLWM3=GS1.1.17372977099805560852.2.0.1737297736.0.0.0; _ga_70ZZHDSCR6=GS1.1.1737297710.2.0.1737297736.34.0.0",
    "priority": "u=1, i",
    "referer": "https://www.mr-group.ru/flats/page-2/",
    "sec-ch-ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "sentry-trace": "098335f0bee64424b3ad883c4f02ad2f-aa3ca2b28a963a1a-1",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
}


def scrape_full():
    next_page = "https://www.mr-group.ru/api/sale/products?category=flats&page=1&limit=500"
    pagenum = 1
    offset = 0
    total_cnt = 0
    errors = []
    while True:
        page = requests.get(next_page, headers=headers)
        res = page.json()
        print(f"mrgroup {offset+1}..{offset+500} page {pagenum}/{res['page_count']}")
        next_page = f"https://www.mr-group.ru/api/sale/products?category=flats&page={pagenum}&limit=500"
        offset += 500
        pagenum += 1

        cnt, error_list = parse_json(res)
        total_cnt += cnt
        errors.extend(error_list)

        if len(res['items']) == 0:
            break
    return f"Inserted: {total_cnt}, errors: {str(errors)[:1000]}"


def parse_json(raw):
    errors_list = []
    cnt = 0
    for item in raw['items']:
        try:
            insert_stmt = insert(pg.table_flats).values(
                estate=item['project']['code'],
                city="msk",
                article=item['code'],
                website_id=item['id'],
                booked=(item['booking_type'] != "Free"),
                area=item['area'],
                price=item['price'], # !!! TODO need to create discount_price
                ppm=item['meter_price'],
                realty_class=convert_class(item['project']['is_private']).name if item['project']['is_private'] else pg.RealtyClass.BUSINESS.name,
                furnish=item['decoration']['code'], # TODO: изучить что значат
                rooms=item['rooms_number'],
                floor_number=item['floor'],
                finish_quarter=None,
                finish_year=datetime.strptime(item['building']['deadline'], "%Y-%m-%dT%H:%M:%S%z").year if item['building']['deadline'] else None,
                link=f"https://www.mr-group.ru/catalog/apartments/{item['code']}",
                raw=item
            )
            pg.connection.execute(insert_stmt)
            pg.connection.commit()
            cnt += 1
        except Exception as e:
            errors_list.append(e)
    return (cnt, errors_list)


def convert_class(is_private):
    match is_private:
        case True:
            return pg.RealtyClass.PREMIUM
        case False:
            return pg.RealtyClass.BUSINESS
        case _:
            return pg.RealtyClass.UNKNOWN

# TODO
# def convert_furnish(finish):
#     match finish:
#         case 0:
#             return pg.Furnish.NOTHING
#         case 1:
#             return pg.Furnish.WHITEBOX_PLUS
#         case 2:
#             return pg.Furnish.WHITEBOX
#         case 3:
#             return pg.Furnish.FURNITURE
#         case _:
#             return pg.Furnish.UNKNOWN
