import requests
from sqlalchemy.dialects.postgresql import insert

import scrape_postgres as pg


def scrape_link(page_link, city):
    next_page = page_link
    total_cnt = 0
    offset = 0
    errors = []
    while True:
        page = requests.get(next_page)
        res = page.json()
        next_page = res['next']
        print(f"a101 {offset+1}..{offset+100}/{res['count']}")

        cnt, error_list = parse_json(res, city)
        total_cnt += cnt
        offset += 100
        errors.extend(error_list)

        if res['next'] is None:
            break
    return f"Inserted: {total_cnt}, errors: {str(errors)[:4000]}"


def scrape_full():
    print(scrape_msk())
    print(scrape_spb())


def scrape_msk():
    return scrape_link("https://a101.ru/api/v2/flat/?ab_test=false&city=msk&filter_type=price&limit=100&offset=0&ordering=actual_price&view=card", city="msk")


def scrape_spb():
    return scrape_link("https://spb.a101.ru/api/v2/flat/?ordering=actual_price&view=card&filter_type=price&limit=100&offset=0&city=spb&ab_test=false", city="spb")


# small return: 119 flats
def scrape_zorge_house():
    return scrape_link("https://a101.ru/api/v2/flat/?ab_test=false&city=msk&complex=139&filter_type=price&limit=100&offset=0&ordering=actual_price&view=card", city="msk")


def parse_json(raw, city):
    errors_list = []
    cnt = 0
    for item in raw['results']:
        try:
            insert_stmt = insert(pg.table_flats).values(
                estate=item['complex_slug'],
                city=city,
                article=item['article'],
                website_id=item['id'],
                booked=(item['status'] != 1),
                area=float(item['area']),
                price=float(item['actual_price']),
                ppm=float(item['actual_ppm']),
                realty_class=convert_realty_class(item['realty_class']).name,
                furnish=convert_furnish(item['whitebox']).name,
                rooms=item['room'],
                floor_number=item['floor'],
                finish_quarter=item['stage'].split(' ')[0],
                finish_year=item['stage'].split(' ')[2],
                link=f"https://a101.ru/kvartiry/{item['id']}/",
                raw=item
            )
            pg.connection.execute(insert_stmt)
            pg.connection.commit()
            cnt += 1
        except Exception as e:
            print(e)
            errors_list.append(e)
    return (cnt, errors_list)


def convert_realty_class(raw_class):
    match raw_class:
        case 2:
            return pg.RealtyClass.COMFORT
        case 3:
            return pg.RealtyClass.BUSINESS
        case 4:
            return pg.RealtyClass.STANDARD
        case "standard":
            return pg.RealtyClass.STANDARD
        case "comfort":
            return pg.RealtyClass.COMFORT
        case "business":
            return pg.RealtyClass.BUSINESS
        case _:
            return pg.RealtyClass.UNKNOWN

def convert_furnish(whitebox):
    match whitebox:
        case False:
            return pg.Furnish.NOTHING
        case True:
            return pg.Furnish.WHITEBOX
        case _:
            return pg.Furnish.UNKNOWN
