# scrape-tms

Изначально задумывался как тмс скрапер (скрап раз в заданный промежуток времени). На данный момент скрапит недвижку без тмс :)

## функционал

Сейчас парсит:
- а101 - https://a101.ru/kvartiry/
- донстрой - https://donstroy.moscow/full-search/
- форма - https://forma.ru/market/list
- инград - https://www.ingrad.ru/search/flats
- левел - https://level.ru/filter/
- мргруп - https://www.mr-group.ru/flats/
- пик - http://pik.ru/search
- самолет - https://samolet.ru/flats/
- сберсити - https://sbercity.ru/property/flats

## TODO 

парсить:

- https://strana.com/msk/flats/?first=24&city=Q2l0eVR5cGU6MQ%3D%3D&features=55&orderMostExpensive=true&utm_referrer=https%3A%2F%2Fstrana.com%2Fmsk%2Fflats%2F%3Ffirst%3D24%26city%3DQ2l0eVR5cGU6MQ%253D%253D%26features%3D55%26orderMostExpensive%3Dtrue
- https://fsk.ru/flats?city-id=1
- https://moskva.brusnika.ru/flat/
- https://etalongroup.ru/choose/?group=false
- https://pioneer.ru/search?projectsType=business
- https://krost.ru/flats/?ordering=price

построить красивые графики:

1. текущее распределение объектов кол-во по проекту (мб за дату)
2. средняя цена за квадрат студии/1/2/3/4+ кв по каждому объекту, по домам в целом (в идеале мультивыбор объектов) + график изменения цен
3. мб карту получится сделать
4. ⭐️ посмотреть какие инструменты подходят для анализа временных рядов, наложить их на квартиры
