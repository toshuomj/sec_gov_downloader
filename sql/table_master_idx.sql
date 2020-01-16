CREATE TABLE IF NOT EXISTS public.master_idx (
        id serial NOT NULL,
        qtr_id int4 NOT NULL,
        cik int NOT NULL,
        cname text NOT NULL,
        form text NOT NULL,
        "date" text NOT NULL,
        link text NOT NULL,
        path text NULL,
        downloaded bool NOT NULL,
        CONSTRAINT master_pk PRIMARY KEY (id),
        CONSTRAINT master_fk FOREIGN KEY (qtr_id) REFERENCES qtr_index(id) ON UPDATE CASCADE ON DELETE CASCADE,
        CONSTRAINT MASTER_UN UNIQUE (LINK)
);

--CIK|Company Name|Form Type|Date