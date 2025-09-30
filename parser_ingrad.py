import requests
from sqlalchemy.dialects.postgresql import insert

from datetime import datetime

import scrape_postgres as pg

# TODO refresh cookies if api returns html+text 403
headers = {
    'accept': '*/*',
    'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7,ja;q=0.6,zh-CN;q=0.5,zh;q=0.4',
    'baggage': 'sentry-environment=PROD,sentry-release=release-fast-track-250117,sentry-public_key=e92b18b752b5a57fcab9400337321152,sentry-trace_id=6692be736a9a4e8b8ccac83c662581af,sentry-sample_rate=0.1,sentry-sampled=false',
    'cookie': '_smt=24fdec87-3928-40f6-aa20-e3c899218536; mindboxDeviceUUID=52618eb9-930b-4a69-9d24-abe38b5fb032; directCrm-session=%7B%22deviceGuid%22%3A%2252618eb9-930b-4a69-9d24-abe38b5fb032%22%7D; _ymab_param=WlLV9wxvwE4UWNGUv7w-kCuYnJJyf5Frza56lwvb2LDBLf7md4R-9O0BEJbXsdk-1kZAPq02aVRtM11HwG99vs_iU_M; _ct_site_id=36409; _ct=1300000000497989442; _ct_client_global_id=fed8f9d1-bdee-5433-8e7e-07e097008be8; _ym_uid=1737230723230336960; _ym_d=1737230723; suggested_city=1; sessionid=0d5ywtvks95asf9m79k9abg8s0wd54my; _ga=GA1.1.863638089.1737230733; FPID=FPID2.2.Z0zg7yDuvtpAK3jV4vwqdcIBLtOYKKaw%2BUsq4yV655w%3D.1737230733; cted=modId%3Dhtlowve6%3Bya_client_id%3D1737230723230336960%3Bclient_id%3D863638089.1737230733; cookies_accepted=1; FPLC=bv8omVSkE1VDg57widtT5aQ9UL7XQ2SkebnUnDy0gM2peZeaZQc%2B6T2v15Dr5wO9GANn53TQqLMaTDt0CmggFpudJXDB1x6qcsDRLdDDFw9UXs%2BwZU88exo7C4%2B1Tg%3D%3D; _ct_ids=htlowve6%3A36409%3A804195973; _ct_session_id=804195973; _ym_isad=1; helperBlockWatch=header; nxt-city=%7B%22dep%22%3A%7B%22version%22%3A1%2C%22sc%22%3A0%7D%2C%22__v_isRef%22%3Atrue%2C%22__v_isShallow%22%3Afalse%2C%22_rawValue%22%3A%7B%22key%22%3A%22moscow%22%2C%22name%22%3A%22%D0%9C%D0%BE%D1%81%D0%BA%D0%B2%D0%B0%22%2C%22url_prefix%22%3A%22%22%2C%22contact_number%22%3A%22%2B7%20495%20292-31-31%22%7D%2C%22_value%22%3A%7B%22key%22%3A%22moscow%22%2C%22name%22%3A%22%D0%9C%D0%BE%D1%81%D0%BA%D0%B2%D0%B0%22%2C%22url_prefix%22%3A%22%22%2C%22contact_number%22%3A%22%2B7%20495%20292-31-31%22%7D%7D; csrftoken=7GqdCbAl2VrTWw1sLcJR1cEJP2YuJMLpKSQATCooIEm7gQ74fglTfnxN8TNvoiHf; pageviewUrlProjectGorkiPark=true; pageviewUrlProjectEgorovoPark=true; qrator_ssid=1737315414.591.P9d79wGNUHfhk7qY-lps2devmr7mna7cdvvaruv46etv6ljcl; undefined=4172.427; pageviewTimerAll=4172.427; pageviewTimerMSK=4172.427; pageviewTimerAllFired1min=true; pageviewTimerAllFired2min=true; pageviewTimerAllFired5min=true; pageviewTimerAllFired10min=true; pageviewTimerAllFired15sec=true; pageviewTimerAllFired15min=true; pageviewTimerAllFired45min=true; pageviewTimerAllFired30min=true; pageviewTimerMSKFired1min=true; pageviewTimerMSKFired2min=true; pageviewTimerMSKFired5min=true; pageviewTimerMSKFired10min=true; pageviewTimerMSKFired15min=true; pageviewTimerMSKFired45min=true; _ga_2WZB3B8QT0=GS1.1.1737315423.4.0.1737315423.0.0.1433683168; qrator_jsr=1737315423.448.WOhHAvN7awID96n9-kkupi2lgkuvplatjetrjrmilv05ggv8k-00; qrator_jsid=1737315423.448.WOhHAvN7awID96n9-hluu1udainv381vd0r0rn9kb213e3qoo',
    'priority': 'u=1, i',
    'referer': 'https://samolet.ru/flats/?from=project&ordering=-order_manual,filter_price_package,pk&free=1&type=100000000&nameType=s',
    'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'sentry-trace': '6692be736a9a4e8b8ccac83c662581af-8a6972a9650b9758-0',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
}


def scrape_full():
    next_page = 'https://new-api.ingrad.ru/api/flats/search?numberElementsPage=500&currentPage=1&sortBy=price&sortOrder=asc&ignoreFilterAdvantagesFlatsAliases=0&ignoreFilterRooms=0&ignoreFilterHouses=0'
    pagenum = 1
    offset = 0
    total_cnt = 0
    errors = []
    while True:
        page = requests.get(next_page, headers=headers)
        res = page.json()
        print(f"ingrad {offset+1}..{offset+500}/{res['data']['numberFlats']}")
        next_page = f'https://new-api.ingrad.ru/api/flats/search?numberElementsPage=500&currentPage={pagenum}&sortBy=price&sortOrder=asc&ignoreFilterAdvantagesFlatsAliases=0&ignoreFilterRooms=0&ignoreFilterHouses=0'
        offset += 500
        pagenum += 1

        cnt, error_list = parse_json(res)
        total_cnt += cnt
        errors.extend(error_list)

        if len(res['data']['flats']) == 0:
            break
    return f"Inserted: {total_cnt}, errors: {str(errors)[:1000]}"


def parse_json(raw):
    errors_list = []
    cnt = 0
    for item in raw['data']['flats']:
        # try:
            insert_stmt = insert(pg.table_flats).values(
                estate=item['estateData']['code'],
                city="msk",
                article=item['externalCode'],
                website_id=item['id'],
                booked=(item['status'] != "free"),
                area=item['square'],
                price=item['price'][0],
                ppm=item['squareCost'][0],
                realty_class=convert_class(item['estateData']['type']).name,
                furnish=item['finish'],
                rooms=item['rooms'],
                floor_number=item['floorNum'],
                finish_quarter=item['houseData']['settlement_quarter'],
                finish_year=item['houseData']['settlement_year'],
                link=f"https://www.mr-group.ru{item['link']}",
                raw=item
            )
            pg.connection.execute(insert_stmt)
            pg.connection.commit()
            cnt += 1
        # except Exception as e:
        #     errors_list.append(e)
    return (cnt, errors_list)


def convert_class(type):
    match type:
        case "comfort":
            return pg.RealtyClass.COMFORT
        case "business":
            return pg.RealtyClass.BUSINESS
        case _:
            return pg.RealtyClass.UNKNOWN # TODO get all other


def convert_furnish(finish):
    match finish:
        case "Без отделки":
            return pg.Furnish.NOTHING
        case "White Box":
            return pg.Furnish.WHITEBOX
        case "Чистовая отделка":
            return pg.Furnish.WHITEBOX_PLUS
        case "Бизнес":
            return pg.Furnish.WHITEBOX_PLUS # TODO check
        case "Премиальная отделка":
            return pg.Furnish.WHITEBOX_PLUS # TODO check
        case _:
            return pg.Furnish.UNKNOWN

