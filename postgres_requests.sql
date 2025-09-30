-- select * from flats where creation_time >= '2025-01-18 00:00:24.8643+03' ORDER BY id;

-- select booked, count(booked) from flats GROUP BY booked;

-- select * from flats where estate = 'sbercity';

-- delete from flats where estate != 'sbercity';

-- drop table flats;

-- delete from flats where 1=1;

-- select city, count(city) from flats GROUP BY city;

-- delete from flats where creation_time >= '2025-01-18 00:00:24.8643+03';

-- select * from flats 
-- where
-- -- flats.raw->>'bookingStatus' != 'reserve'
-- finish_year = '2024'
-- order by id desc;

select * from flats
    where 
        flats.raw->>'metroStationsServiceNew' is not NULL;


-- select * from (select *, row_number() OVER(partition by article order by id desc) as rn from flats);

-- SELECT article, count(article) FROM flats 
-- GROUP BY article
-- HAVING COUNT(*) > 2;

select 1;


select * from flats where article IN (select article from (select id, article, price, rank() OVER(partition by article order by price) as rnk from flats) where rnk > 1) order by article, creation_time;


select * from flats where article IN (select article from flats group by article HAVING count(article) = 6);


select * from flats where creation_time > '2025-08-07';


select ceil(EXTRACT(epoch from end_time - start_time) * 100000) as elapsed_ms, * from job_results;


select * from flats order by id desc limit 100;


select distinct estate from flats where estate ilike '%park%';
select * from flats where estate = 'mpark' order by price desc;



-- table for parsers:

-- create table scrape if not exists (
--     id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
--     creation_time TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
--     parser_name text,
--     website_name text,
--     raw_html text, -- seems ok but ?  ?? 
--     raw_json jsonb,
-- );
-- HERE SHOULD BE CONSTRAINT raw_html AND raw_json SHOULD CONTAIN ONLY ONE DATA AND ONE NULL
--


select * from flats where article = 'Soul-6(кв)-8/4/1(3)';


select article, string_agg(TO_CHAR(price, '999G999G999G990') || ' ' || creation_time::date, ', ' order by creation_time) as prices from (select * from flats where price BETWEEN 10_000_000 and 20_000_000 order by price desc) group by article;

select article, '[' || string_agg('{ "price": ' || price || ' "date": "' || creation_time::date || '" }', ', ' order by rnk) || ']' as prices from (select rank() over(PARTITION BY article ORDER BY creation_time) as rnk, * from flats) group by article;\
