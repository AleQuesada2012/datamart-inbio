CREATE SCHEMA inbio;
CREATE SCHEMA specimen_datamart;

select * from inbio.inbio_data; -- esta es la tabla que nace del CSV

-- ******************* tablas de inbio *********************
CREATE TABLE TAXON(
	taxon_id SERIAL PRIMARY KEY,
	kingdom_name TEXT,
	phylum_division_name TEXT,
	class_name TEXT,
	order_name TEXT,
	family_name TEXT,
	genus_name TEXT,
	species_name TEXT,
	scientific_name TEXT
);


create table SITE(
                     site_id SERIAL PRIMARY KEY,
                     Latitude DOUBLE PRECISION NOT NULL, -- no sé por qué vienen con mayúsculas en el proyecto pero
                     Longitude DOUBLE PRECISION NULL,    -- lo puse igual jaja
                     site_description TEXT NOT NULL
);


create table GATHERING_RESPONSIBLE(
    gathering_responsible_ID SERIAL PRIMARY KEY,
    _name TEXT NOT NULL
);


create table GATHERING(
	gathering_id SERIAL PRIMARY KEY,
	gathering_date TEXT NOT NULL,
	gathering_responsible_ID INTEGER,
	site_id INTEGER,
	FOREIGN KEY(gathering_responsible_ID) REFERENCES inbio.gathering_responsible(gathering_responsible_ID),
	FOREIGN KEY(site_id) REFERENCES inbio.SITE(site_id)
);


create table SPECIMEN(
	specimen_id SERIAL PRIMARY KEY,
	taxon_id INTEGER,
	gathering_id INTEGER,
	specimen_description TEXT,
	specimen_cost DOUBLE PRECISION,
	FOREIGN KEY(taxon_id) REFERENCES inbio.TAXON(taxon_id),
	FOREIGN KEY(gathering_id) REFERENCES inbio.GATHERING(gathering_id)
);


-- ************** procedimientos de inbio ******************

CREATE OR REPLACE PROCEDURE insert_site(sit_id INTEGER, lat DOUBLE PRECISION, long DOUBLE PRECISION, des TEXT)
LANGUAGE plpgsql AS
$$
	BEGIN
		IF NOT EXISTS (SELECT 1 FROM inbio.SITE WHERE site_id = sit_id) AND lat IS NOT NULL AND long IS NOT NULL AND des IS NOT NULL THEN
			INSERT INTO inbio.SITE (site_id, Latitude, Longitude, site_description)
			VALUES (sit_id, lat, long, des);
		END IF;
	END;
$$;
