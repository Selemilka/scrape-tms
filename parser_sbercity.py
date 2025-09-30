import requests
from sqlalchemy.dialects.postgresql import insert

import scrape_postgres as pg


def scrape_full(page_link = 'https://sbercity.ru/api/v1/api/flats/?limit=100&offset=0'):
    offset = 0
    next_page = page_link
    total_cnt = 0
    errors = []
    while True:
        page = requests.get(next_page)
        res = page.json()
        offset += 100
        next_page = f'https://sbercity.ru/api/v1/api/flats/?limit=100&offset={offset}'

        cnt, error_list = parse_json(res)
        total_cnt += cnt
        errors.extend(error_list)

        print(len(res))
        if res['next'] is None:
            break
    return f"Inserted: {total_cnt}, errors: {str(errors)[:1000]}"


def parse_json(raw):
    errors_list = []
    cnt = 0
    for item in raw['results']:
        try:
            insert_stmt = insert(pg.table_flats).values(
                estate="sbercity",
                city="msk",
                article=item['alias'],
                website_id=item['id'],
                booked=(item['price'] == None),
                area=item['area'],
                price=item['price'],
                ppm=item['price_per_meter'],
                realty_class=convert_class(item['standard']).name,
                furnish=convert_furnish(item['finish']).name,
                rooms=item['rooms'],
                floor_number=item['number_on_floor'],
                finish_quarter=item['complex_finish_quarter'],
                finish_year=item['complex_finish_year'],
                link=f"https://sbercity.ru/property/flats/{item['alias']}",
                raw=item
            )
            pg.connection.execute(insert_stmt)
            pg.connection.commit()
            cnt += 1
        except Exception as e:
            errors_list.append(e)
    return (cnt, errors_list)


def convert_class(standard):
    match standard:
        case "Balanced":
            return pg.RealtyClass.COMFORT
        case "Optimum":
            return pg.RealtyClass.COMFORT_PLUS
        case "Advanced":
            return pg.RealtyClass.BUSINESS
        case "Superb":
            return pg.RealtyClass.PREMIUM
        case _:
            return pg.RealtyClass.UNKNOWN


def convert_furnish(finish):
    match finish:
        case False:
            return pg.Furnish.NOTHING
        case True:
            return pg.Furnish.WHITEBOX
        case _:
            return pg.Furnish.UNKNOWN