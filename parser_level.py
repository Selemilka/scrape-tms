import requests
from sqlalchemy.dialects.postgresql import insert

import scrape_postgres as pg


headers = {
    'accept': '*/*',
    'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7,ja;q=0.6,zh-CN;q=0.5,zh;q=0.4',
    'cookie': 'booking-blocking=false; menu=%7B%22isFavorite%22%3Afalse%7D; growthbook-attr-id=1b9033bd-6449-4e88-9816-a6fbbc942ef8; _gcl_au=1.1.1528953693.1736268543; _ym_uid=1736268540313674296; _ym_d=1736268540; mindboxDeviceUUID=52618eb9-930b-4a69-9d24-abe38b5fb032; directCrm-session=%7B%22deviceGuid%22%3A%2252618eb9-930b-4a69-9d24-abe38b5fb032%22%7D; scbsid_old=2586284637; carrotquest_device_guid=11fcc048-6477-4c1b-af7c-194006633857; carrotquest_uid=1880394950315606708; carrotquest_auth_token=user.1880394950315606708.50549-b9906febe2aaab4d349cf1594e.5505d13d82abc821127fd3102f42930fa8e15d07ef9851f7; carrotquest_realtime_services_transport=wss; csrftoken=Wrjp1qybk9HjIACTRlxHMvIjbVAfk9qX; activity=0|-1; qrator_jsr=v2.0.1737317910.794.b222966bOdkpAKuw|eCNi1IffXe0iIjWQ|9Lu3FnkKa/MDyx57AsT4eZyZi+JCQx9L2WklWB3zO7McymitvDeGtb+MOJNMKfqZD9QFspFbLb3wK8GEa/lDUA==-hOqXK5EMtD258GKTqMglkvAOvuw=-00; qrator_jsid2=v2.0.1737317910.794.b222966bOdkpAKuw|axgrzOa2ddW7DOku|84GbaVmdvio9/O6zLHxSzNEDe4SrJNfMgMO+zIxeDeAE3OpW6Ci8GbMQZtIWws+uckpxlFvMAoQtYUTRwSMvI9zjJtRw5IMRS2Bhm1lJyfRw0IAOVX7gLWSN9p98eKqHZvbdKm8PO0U3vsmx9v1A4g==-LWi325/7uxpoj+iHTy1riVCpGWo=; ya_visit_init=1737317913236; _cmg_csstvg3wT=1737317915; _comagic_idvg3wT=10035550009.14099502374.1737317915; carrotquest_jwt_access=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdHQiOiJhY2Nlc3MiLCJleHAiOjE3MzczMjE1MTUsImlhdCI6MTczNzMxNzkxNSwianRpIjoiNTI3ZThmM2JlMzNiNDVhNzllOGE3OGYyYzg3ZDcxOGEiLCJhY3QiOiJ3ZWJfdXNlciIsImN0cyI6MTczNzMxNzkxNSwicm9sZXMiOlsidXNlci4kYXBwX2lkOjUwNTQ5LiR1c2VyX2lkOjE4ODAzOTQ5NTAzMTU2MDY3MDgiXSwiYXBwX2lkIjo1MDU0OSwidXNlcl9pZCI6MTg4MDM5NDk1MDMxNTYwNjcwOH0.2F2HFxAAqjez8evDYWoiq9UEu7mjr7iAPzr_ifaKYYk; _ym_isad=1; _gid=GA1.2.635518753.1737317923; _gat_UA-79793340-2=1; _ym_visorc=w; _ga=GA1.1.869278910.1736268542; carrotquest_session=fuhk3gcstxyon5yr75qdbdpl9awkf2hq; carrotquest_session_started=1; pageCount=14; sma_session_id=2157751620; SCBfrom=https%3A%2F%2Flevel.ru%2Ffilter%2F; SCBnotShow=-1; SCBporogAct=5000; smFpId_old_values=%5B%22cdbca1cf6cd1661edcc62e427695e396%22%2C%22b8cb5d3cb6ddd7c6020ac17a17faaf56%22%5D; SCBstart=1737317936672; SCBFormsAlreadyPulled=true; ya_visit_total=4; ya_visit_total_session=4; ya_visit_page=%2Ffilter; _ga_M5QHFCMEFC=GS1.1.1737317912.6.1.1737317970.2.0.0; sma_index_activity=4732; SCBindexAct=4232',
    'priority': 'u=1, i',
    'referer': 'https://level.ru/filter?cardType=vertical',
    'sec-ch-ua': '"Not A(Brand";v="8", "Chromium";v="132", "Google Chrome";v="132"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36',
    'x-forwarded-host': '',
}


def scrape_full():
    url = 'https://level.ru/api/filter/'
    params = {
        'ordering': "price,pk,mode",
        'limit': 200,
        'offset': 0
    }
    total_cnt = 0
    errors = []
    while True:
        page = requests.get(url, headers=headers, params=params)
        res = page.json()
        print(f"level {params['offset']+1}..{params['offset']+params['limit']}/{res['count']}")
        params['offset'] += params['limit']

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
                estate=item['project_long_slug'],
                city="msk",
                article=item['realty_pk'],
                website_id=item['flat_id'],
                booked=(item['status'] == 2),
                area=item['area'],
                price=item['price'],
                ppm=item['ppm'],
                realty_class=convert_class(item['category_class']).name,
                furnish=convert_furnish(item['renovation']).name,
                rooms=item['room'],
                floor_number=item['floor'],
                finish_quarter=item['completion_quarter'],
                finish_year=item['completion_year'],
                link=f"https://www.level.ru{item['url']}",
                raw=item
            )

            pg.connection.execute(insert_stmt)
            pg.connection.commit()
            cnt += 1
        # except Exception as e:
        #     errors_list.append(e)
    return (cnt, errors_list)


def convert_class(category_class):
    match category_class:
        case 2:
            return pg.RealtyClass.BUSINESS
        case 4:
            return pg.RealtyClass.COMFORT
        case _:
            return pg.RealtyClass.UNKNOWN # TODO get all other


def convert_furnish(renovation):
    match renovation:
        case 0:
            return pg.Furnish.NOTHING
        case 2:
            return pg.Furnish.WHITEBOX
        case _:
            return pg.Furnish.UNKNOWN
