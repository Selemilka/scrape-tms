CREATE TABLE IF NOT EXISTS flats(  
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    creation_time TIMESTAMP WITH TIME ZONE DEFAULT now(),
    zastroischik TEXT,
    estate TEXT,
    city TEXT,
    article TEXT,
    website_id TEXT,
    booked BOOLEAN,
    area NUMERIC,
    discount_price NUMERIC,
    base_price NUMERIC,
    discount_ppm NUMERIC,
    base_ppm NUMERIC,
    realty_class TEXT,
    furnish TEXT,
    rooms TEXT,
    floor_number TEXT,
    finish_quarter TEXT,
    finish_year TEXT,
    link TEXT,
    raw jsonb
);

-- TODO: create indexes

CREATE TABLE IF NOT EXISTS job_results(
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    func_name TEXT,
    start_time TIMESTAMP WITH TIME ZONE,
    end_time TIMESTAMP WITH TIME ZONE,
    status TEXT,
    result TEXT
)
