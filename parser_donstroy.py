import requests
from sqlalchemy.dialects.postgresql import insert
import json

from datetime import datetime

import scrape_postgres as pg


headers = {
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7,ja;q=0.6,zh-CN;q=0.5,zh;q=0.4',
    'content-type': 'application/json',
    'cookie': 'PHPSESSID=zfcpl4tXII9VwzmpLdotKgW9cPlHBnGK; BITRIX_CONVERSION_CONTEXT_sm=%7B%22ID%22%3A86%2C%22EXPIRE%22%3A1737233940%2C%22UNIQUE%22%3A%5B%22conversion_visit_day%22%5D%7D; BX_USER_ID=3bcc8a61a4fef669fab20a91639c1378; _gid=GA1.2.380172496.1737230244; sbjs_migrations=1418474375998%3D1; sbjs_current_add=fd%3D2025-01-18%2022%3A57%3A24%7C%7C%7Cep%3Dhttps%3A%2F%2Fdonstroy.moscow%2Ffull-search%2F%7C%7C%7Crf%3Dhttps%3A%2F%2Fdonstroy.moscow%2F; sbjs_first_add=fd%3D2025-01-18%2022%3A57%3A24%7C%7C%7Cep%3Dhttps%3A%2F%2Fdonstroy.moscow%2Ffull-search%2F%7C%7C%7Crf%3Dhttps%3A%2F%2Fdonstroy.moscow%2F; sbjs_current=typ%3Dtypein%7C%7C%7Csrc%3D%28direct%29%7C%7C%7Cmdm%3D%28none%29%7C%7C%7Ccmp%3D%28none%29%7C%7C%7Ccnt%3D%28none%29%7C%7C%7Ctrm%3D%28none%29; sbjs_first=typ%3Dtypein%7C%7C%7Csrc%3D%28direct%29%7C%7C%7Cmdm%3D%28none%29%7C%7C%7Ccmp%3D%28none%29%7C%7C%7Ccnt%3D%28none%29%7C%7C%7Ctrm%3D%28none%29; sbjs_udata=vst%3D1%7C%7C%7Cuip%3D%28none%29%7C%7C%7Cuag%3DMozilla%2F5.0%20%28Macintosh%3B%20Intel%20Mac%20OS%20X%2010_15_7%29%20AppleWebKit%2F537.36%20%28KHTML%2C%20like%20Gecko%29%20Chrome%2F131.0.0.0%20Safari%2F537.36; _ym_uid=1737230245894577991; _ym_d=1737230245; cted=modId%3Ddde5d0a5%3Bclient_id%3D58024530.1737230244%3Bya_client_id%3D1737230245894577991%7CmodId%3D36b04ec4%3Bclient_id%3D58024530.1737230244%3Bya_client_id%3D1737230245894577991; _ct=500000001185212917; _ct_ids=36b04ec4%3A13517%3A1102238429_dde5d0a5%3A7883%3A4762339330; _ct_session_id=4762339330; _ct_site_id=7883; _ct_client_global_id=fed8f9d1-bdee-5433-8e7e-07e097008be8; agree-cookie=1; _ga=GA1.2.58024530.1737230244; _ga_F522MVXW9K=GS1.2.1737230244.1.1.1737230259.45.0.0; _ga_LJV2D2Z2D2=GS1.1.1737230244.1.1.1737230260.0.0.0; _ga_H36T4JN56M=GS1.1.1737230244.1.1.1737230260.44.0.0; d_session_start_time=1737293544428',
    'origin': 'https://donstroy.moscow',
    'priority': 'u=1, i',
    'referer': 'https://donstroy.moscow/full-search/?price%5B%5D=13.8&price%5B%5D=910.2&area%5B%5D=21&area%5B%5D=392&floor_number%5B%5D=2&floor_number%5B%5D=50&floor_first_last=false&discount=false&furnish=false&apartments=false&secondary=false&sort=price-asc&view_type=flats&page=3&view=card',
    'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
}


def scrape_full():
    url = 'https://donstroy.moscow/api/v1/flatssearch/choose_params_api_flats/'
    offset = 0
    total_cnt = 0
    errors = []
    data = {
        "price": [1, 99999],
        "area": [21, 392],
        "floor_number": [1, 999],
        "rooms": [],
        "projects": [],
        "quarters": [],
        "buildings": [],
        "advantages": [],
        "floor_first_last": False,
        "discount": False,
        "furnish": False,
        "apartments": False,
        "secondary": False,
        "category": None,
        "sort": "price-asc",
        "view_type": "flats",
        "page": 1
    }
    while True:
        page = requests.post(url, headers=headers, data=json.dumps(data))
        res = page.json()
        print(f"donstroy {total_cnt+1}..??/{res['total_flats']}") # TODO ??? somnitelno
        offset += 20
        total_cnt += 1
        data["page"] += 1

        cnt, error_list = parse_json(res)
        total_cnt += cnt
        errors.extend(error_list)

        if len(res['flats']) == 1:
            break
    return f"Inserted: {total_cnt}, errors: {str(errors)[:1000]}"


def parse_json(raw):
    errors_list = []
    cnt = 0
    for item in raw['flats']:
        try:
            if (item['isUtp']):
                continue
            insert_stmt = insert(pg.table_flats).values(
                estate=item['project'],
                city="msk",
                article=item['id'],
                website_id=item['number'],
                booked=False,
                area=float(item['area']),
                price=float(item['price']),
                ppm=round(float(item['price'])/float(item['area'])),
                realty_class=pg.RealtyClass.BUSINESS.name,
                furnish=convert_furnish(item['furnish']).name,
                rooms=item['rooms'],
                floor_number=item['floor'],
                finish_quarter=None,
                finish_year=None,
                link=f"https://www.mr-group.ru/objects/sobytie/plans/quarter4/korpus2/section1/floor19/flat300402231/",
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


def convert_furnish(finish):
    match finish:
        case 0:
            return pg.Furnish.NOTHING
        case 1:
            return pg.Furnish.WHITEBOX_PLUS
        case _:
            return pg.Furnish.UNKNOWN
