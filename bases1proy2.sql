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
	gathering_date DATE NOT NULL,
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
CREATE OR REPLACE PROCEDURE insert_site(id_site INTEGER, lat DOUBLE PRECISION, long DOUBLE PRECISION, description TEXT)
LANGUAGE plpgsql AS
$$
	BEGIN
		IF NOT EXISTS (SELECT 1 FROM inbio.SITE WHERE site_id = id_site) AND lat IS NOT NULL AND long IS NOT NULL AND description IS NOT NULL THEN
			INSERT INTO inbio.SITE (site_id, Latitude, Longitude, site_description)
			VALUES (id_site, lat, long, description);
		END IF;
	END;
$$;


CREATE OR REPLACE FUNCTION insert_gath_resp(name_to_insert TEXT)
RETURNS INTEGER
LANGUAGE plpgsql AS
$$
	DECLARE
		index INTEGER;
	BEGIN
		SELECT gathering_responsible_ID INTO index FROM inbio.gathering_responsible WHERE _name = name_to_insert;
		IF NOT FOUND AND name_to_insert IS NOT NULL THEN
			INSERT INTO inbio.gathering_responsible(_name) VALUES (name_to_insert) RETURNING gathering_responsible_ID INTO index;
		END IF;
		RETURN index;
	END;
$$;


CREATE OR REPLACE PROCEDURE insert_taxon(id_taxon INTEGER, kingdom TEXT, phylum TEXT, cclass TEXT, _order TEXT, family TEXT, genus TEXT, species TEXT, sci_name TEXT)
LANGUAGE plpgsql AS
$$
	BEGIN
		IF NOT EXISTS (SELECT 1 FROM inbio.TAXON WHERE taxon_id = id_taxon) THEN
			INSERT INTO inbio.TAXON (taxon_id, kingdom_name, phylum_division_name, class_name, order_name, family_name, genus_name, species_name, scientific_name)
			VALUES (id_taxon, kingdom, phylum, cclass, _order, family, genus, species, sci_name);
		END IF;
	END;
$$;

CREATE OR REPLACE FUNCTION insert_gathering(fechaxd DATE, gath_resp_id INTEGER, id_site INTEGER)
RETURNS INTEGER
LANGUAGE plpgsql AS
$$
	DECLARE
		indice INTEGER;
	BEGIN
		SELECT gathering_id INTO indice FROM inbio.GATHERING WHERE gathering_date = fechaxd AND gathering_responsible_id = gath_resp_id AND site_id = id_site;
		IF NOT FOUND AND fechaxd IS NOT NULL AND EXISTS (SELECT 1 FROM inbio.GATHERING_RESPONSIBLE WHERE gathering_responsible_ID = gath_resp_id) AND EXISTS (SELECT 1 FROM inbio.SITE WHERE site_id = id_site) THEN
			INSERT INTO inbio.GATHERING(gathering_date, gathering_responsible_ID, site_id)
			VALUES (fechaxd, gath_resp_id, id_site) RETURNING gathering_id INTO indice;
		END IF;
		RETURN indice;
	END;
$$;


CREATE OR REPLACE PROCEDURE insert_species(id_taxon INTEGER, gath_id INTEGER, description TEXT, spec_cost DOUBLE PRECISION)
LANGUAGE plpgsql AS
$$
	BEGIN
		IF EXISTS (SELECT 1 FROM inbio.GATHERING WHERE gathering_id = gath_id) THEN
			INSERT INTO inbio.SPECIMEN(taxon_id, gathering_id, specimen_description, specimen_cost)
			VALUES(id_taxon, gath_id, description, spec_cost);
		END IF;
	END;
$$;

CREATE OR REPLACE PROCEDURE normalizar_tablas()
LANGUAGE plpgsql AS
$$
DECLARE
    rec RECORD;
    gath_resp_id INTEGER;
    gath_id INTEGER;
BEGIN
    FOR rec IN SELECT * FROM inbio.inbio_data LOOP
        call insert_taxon(
            rec.taxon_id,
            rec.kingdom_name,
            rec.phylum_division_name,
            rec.class_name,
            rec.order_name,
            rec.family_name,
            rec.genus_name,
            rec.species_name,
            rec.default_name
        );

        call insert_site(
            rec.site_id,
            rec.latitude,
            rec.longitude,
            rec.description
        );

        -- Inserta el responsable del gathering y recupera su ID
        gath_resp_id := insert_gath_resp(rec.responsible_person_name);

        -- Inserta el registro en gathering y recupera su ID
        gath_id := insert_gathering(rec.initial_date_time, gath_resp_id, rec.site_id);

        call insert_species(
            rec.taxon_id,
            gath_id,
            rec.description2,
            rec.specimen_value::DOUBLE PRECISION
        );
    END LOOP;
END;
$$;

CALL normalizar_tablas();

-- revisando si la migración fue exitosa contando los registros
select count(*) from gathering_responsible;
select count(*) from site;
select count(*) from taxon;
select count(*) from specimen;
select count(*) from gathering;