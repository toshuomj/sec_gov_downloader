-- Table to store the urls
--GRANT ALL PRIVILEGES ON DATABASE crawlers to postgres;
CREATE TABLE IF NOT EXISTS public.qtr_index (
        ID serial NOT NULL,
        YEAR INT NOT NULL,
        QUARTER TEXT NOT NULL,
        URL TEXT NOT NULL,
        CRAWLED INT NOT NULL,
        CONSTRAINT QTR_PK PRIMARY KEY (ID),
        CONSTRAINT QTR_UN UNIQUE (URL)
);