import requests
from sqlalchemy.dialects.postgresql import insert

import scrape_postgres as pg


def scrape_full():
    webpage = "https://manager.forma.ru/api/v2/marketplace"
    pagenum = 1
    limit = 100
    total_cnt = 0
    current_cnt = 0
    errors = []
    while True:
        attrs = {"price":{},"area":{},"floor":{},"attributes":[],"page":pagenum,"limit":limit,"order":{"key":"price","type":"asc"},"ceilingHeight":{}}
        page = requests.post(webpage, attrs)
        res = page.json()
        total_cnt = res['total']
        pagenum += 1

        cnt, error_list = parse_json(res)
        current_cnt += cnt
        errors.extend(error_list)

        print(f"FORMA: {current_cnt}/{total_cnt}")
        if len(res['flats']) == 0:
            break
    return f"Inserted: {current_cnt}, errors: {str(errors)[:1000]}"


def parse_json(raw):
    errors_list = []
    cnt = 0
    for item in raw['flats']:
        try:
            insert_stmt = insert(pg.table_flats).values(
                estate=str.lower(item['ProjectName']),
                city="msk",
                article=item['name'],
                website_id=item['id'],
                booked=False,
                area=item['area'],
                price=item['currentPrice'],
                ppm=item['meterPrice'],
                realty_class=convert_class(item['projectId']).name,
                furnish=convert_furnish(item['finish']['name']).name,
                rooms=item['rooms'],
                floor_number=item['floor'],
                finish_quarter=item['bulk']['settlement_quarter'],
                finish_year=item['bulk']['settlement_year'],
                link=f"https://forma.ru/market/flat/{item['name']}",
                raw=item
            )
            pg.connection.execute(insert_stmt)
            pg.connection.commit()
            cnt += 1
        except Exception as e:
            errors_list.append(e)
    return (cnt, errors_list)


def convert_class(project_id):
    if project_id in [427]:
        return pg.RealtyClass.PREMIUM
    elif project_id != None:
        return pg.RealtyClass.BUSINESS
    else:
        return pg.RealtyClass.UNKNOWN


def convert_furnish(furnish):
    match furnish:
        case 'unfinished':
            return pg.Furnish.NOTHING
        case 'whiteBox':
            return pg.Furnish.WHITEBOX
        case 'dizajnerskayaOtdelkaForma':
            return pg.Furnish.WHITEBOX_PLUS
        case _:
            return pg.Furnish.UNKNOWN
