import requests
from sqlalchemy.dialects.postgresql import insert

from datetime import datetime

import scrape_postgres as pg

# TODO refresh cookies if api returns html+text 403
headers = {
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7,ja;q=0.6,zh-CN;q=0.5,zh;q=0.4',
    'Connection': 'keep-alive',
    'Origin': 'https://samolet.ru',
    'Referer': 'https://samolet.ru/',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-site',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Google Chrome";v="137", "Chromium";v="137", "Not/A)Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'x-session-token': '578f372f-9362-4a52-b08c-0ac1e950d04d',
    'cookie': '_smt=3e3f5158-1575-480c-98bb-5d21fb9f4b8e; mindboxDeviceUUID=7504e02e-dfc2-42e9-b14a-6fdf0eaa08c8; directCrm-session=%7B%22deviceGuid%22%3A%227504e02e-dfc2-42e9-b14a-6fdf0eaa08c8%22%7D; suggested_city=1; _ct=1300000000506987472; _ct_client_global_id=7a2fa218-c9af-58fc-86ea-4126d2ecf008; _ga=GA1.1.1167193313.1739643003; FPID=FPID2.2.1yhQLnRJ5FzTMeQ2LV%2Bf9Y0vNeSqvaAqvpR29X1QqrY%3D.1739643003; _ymab_param=-xgHcWkWPJpMHNK2NHbF88B7vga99IX-CITHN1M0ccjRY_Bkeai1DZYdcxC6WKUTueGEDYn2y6v9HCZtPad_-EsxngA; _ym_uid=1739643010409496612; cookies_accepted=1; _ga_2WZB3B8QT0=GS1.1.1739794260.2.0.1739794260.0.0.1524228599; qrator_ssid=1756124970.135.DGfuXTfSyMk7Gl1Q-o75gl4msirpetrcmjj4cjsn0a5lr36m3; qrator_jsr=1756124973.399.IoCSbNpBd7mGy7Q3-t9b3v007pv7kvv7h9qppjrgenr441gh4-00; cted=modId%3Dhtlowve6%3Bclient_id%3D1167193313.1739643003%3Bya_client_id%3D1739643010409496612; popmechanic_sbjs_migrations=popmechanic_1418474375998%3D1%7C%7C%7C1471519752600%3D1%7C%7C%7C1471519752605%3D1; _ym_d=1756124975; _ym_isad=1; _ym_visorc=b; vp_width=1440; _ct_ids=htlowve6%3A36409%3A907824141; _ct_session_id=907824141; _ct_site_id=36409; sessionid=yqg4swyvt18jrb0u6fv8b3lrk7cydy2q; session_timer_104054=1; city_was_suggested=1; nxt-city=%7B%22key%22%3A%22moscow%22%2C%22name%22%3A%22%D0%9C%D0%BE%D1%81%D0%BA%D0%B2%D0%B0%22%2C%22url_prefix%22%3A%22%22%2C%22contact_number%22%3A%22%2B7%20495%20292-31-31%22%7D; qrator_jsid=1756124973.399.IoCSbNpBd7mGy7Q3-o43p7sub074nau7ak10jaq73l5eej83p; undefined=20.975; pageviewTimerAll=20.975; pageviewTimerMSK=20.975; pageviewTimerAllFired15sec=true; seconds_on_page_104054=18; call_s=___htlowve6.1756126817.907824141.143945:445562|2___; user_account_return_url_session=%2Fflats%2F%3Ffree%3D1%26project%3D44%26rooms%3D2; csrftoken=nX9DtI0e3tvKTjVhIVqkb2TuJtaVaN3n8YJhQ32AMmXJVuSLyl7tlvTBekBzBvY9; pageviewCount=3; pageviewCountMSK=3; PageNumber=3'
}


def scrape_full():
    next_page = 'https://samolet.ru/api_redesign/flats/?from=project&ordering=-order_manual,filter_price_package,pk&free=1&type=100000000&nameType=s&offset=0&limit=12'
    pagenum = 1
    offset = 0
    total_cnt = 0
    errors = []
    while True:
        page = requests.get(next_page, headers=headers)
        res = page.json()
        print(f"samolet {offset+1}..{offset+12}/{res['count']}")
        next_page = f'https://samolet.ru/api_redesign/flats/?from=project&ordering=-order_manual,filter_price_package,pk&free=1&type=100000000&nameType=s&offset={offset}&limit=12'
        offset += 12
        pagenum += 1

        cnt, error_list = parse_json(res)
        total_cnt += cnt
        errors.extend(error_list)

        if len(res['results']) == 0:
            break
    return f"Inserted: {total_cnt}, errors: {str(errors)[:1000]}"


def parse_json(raw):
    errors_list = []
    cnt = 0
    for item in raw['results']:
        # try:
            insert_stmt = insert(pg.table_flats).values(
                estate=item['project'],
                city="msk",
                article=item['article'],
                website_id=item['id'],
                booked=(not item['booking_available']), # TODO ??
                area=item['area'],
                price=item['price'],
                ppm=item['ppm'],
                realty_class=pg.RealtyClass.BUSINESS.name if item.get('business_class') else pg.RealtyClass.COMFORT.name,
                furnish=pg.RealtyClass.UNKNOWN.name, # TODO has_individual_decoration, has_included_improved_decoration, default_decoration, has_pre_finish_decoration
                rooms=item['rooms'],
                floor_number=item['floor_number'],
                finish_quarter=None,
                finish_year=parse_date_safe(item),
                link=item['url'],
                raw=item
            )

            pg.connection.execute(insert_stmt)
            pg.connection.commit()
            cnt += 1
        # except Exception as e:
        #     errors_list.append(e)
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



# GPT
def parse_date_safe(item):
    """Parses an item safely, handling potential missing keys or invalid data formats."""

    finish_year = None

    if item is not None and isinstance(item, dict) and 'settling_date_formatted' in item:
        settling_date_formatted = item['settling_date_formatted']

        if settling_date_formatted and isinstance(settling_date_formatted, str): # Check if it's a non-empty string
            parts = settling_date_formatted.split(' ')
            if len(parts) > 2: # Ensure we have at least three parts
                finish_year = parts[2]

    return finish_year
