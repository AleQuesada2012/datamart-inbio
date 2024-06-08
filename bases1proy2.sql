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

-- Procedure: insert_site
-- Descripción: Inserta un nuevo sitio a la tabla inbio.SITE si no existe y si los parámetros no son nulos.
-- Parámetros de entrada:
--   id_site: INTEGER - ID del sitio.
--   lat: DOUBLE PRECISION - Coordenada latitudinal del sitio.
--   long: DOUBLE PRECISION - Coordenada longitudinal del sitio.
--   description: TEXT - Descripción del sitio (algunos, su dirección).
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

-- Function: insert_gath_resp
-- Description: Inserta el nombre de un nuevo responsable si no existe y el nombre parámetro no es nulo.
-- Input Parameters:
--   name_to_insert: TEXT - Nombre del responsable de la recolección.
-- Return Value:
--   INTEGER - ID del responsable insertado según el sequence autogenerado para la llave primaria.
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

-- Procedure: insert_taxon
-- Description: Inserta un nuevo taxón a la tabla inbio.TAXON si aún no existe. Se destaca su uso de EXISTS y SELECT 1 para
-- descartar la existencia de un duplicado.
-- Input Parameters:
--   id_taxon: INTEGER - ID del taxón.
--   kingdom: TEXT - nombre del reino del taxón.
--   phylum: TEXT - nombre del Filo o División del taxón.
--   cclass: TEXT - nombre de la Clase del taxón. Se usa cclass con doble c por si diera problemas con palabras reservadas.
--   _order: TEXT - nombre del Orden al que pertenece el taxón.
--   family: TEXT - nombre de la Familia a la que pertenece el taxón.
--   genus: TEXT - nombre del Género del taxón.
--   species: TEXT - nombre común o nombre de especie
--   sci_name: TEXT - nombre científico.
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

-- Function: insert_gathering
-- Description: Inserta un nuevo registro a las recolecciones si no existe y todos los parámetros son válidos.
-- Input Parameters:
--   fechaxd: DATE - Fecha registrada de la recolección.
--   gath_resp_id: INTEGER - ID del responsable de la recolección.
--   id_site: INTEGER - ID del sitio en que se registró la recolección.
-- Return Value:
--   INTEGER - ID de la recolección.
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

-- Procedure: insert_species
-- Description: Inserta un nuevo espécimen a la tabla inbio.SPECIMEN si su ID de recolección ya existe.
-- Input Parameters:
--   id_taxon: INTEGER - ID del taxón
--   gath_id: INTEGER - ID de la recolección.
--   description: TEXT - Descripción del espécimen.
--   spec_cost: DOUBLE PRECISION - Costo asociado con recolectar el espécimen, importante para el datmart más adelante.
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

-- Procedure: normalizar_tablas
-- Description: Se llama para normalizar las tablas en el esquema inbio a partir de la tabla del CSV
-- Lo realiza insertando registros a varias tablas por medio de la invocación de los procedimientos y funciones definidos anteriormente.
-- No emplea los cursores o punteros, utiliza FOR-LOOPs y la variable RECORD que aloja completo un registro para cada fila.
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

-- terminan las tablas del esquema inbio, lo siguiente es del esquema specimen_datamart

-- ********* CREATE TABLES *************

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


CREATE TABLE SITE(
	site_id SERIAL PRIMARY KEY,
	Latitude DOUBLE PRECISION NOT NULL,
	Longitude DOUBLE PRECISION NOT NULL,
	site_description TEXT NOT NULL
);


CREATE TABLE specimen_datamart.GATHERING_RESPONSIBLE(
	gathering_responsible_ID SERIAL PRIMARY KEY,
	_name TEXT NOT NULL
);


CREATE TABLE specimen_datamart.GATHERING_DATE(
	gathering_id SERIAL PRIMARY KEY,
    month INTEGER NOT NULL,
    day INTEGER NOT NULL,
	year INTEGER NOT NULL
);
-- tratando de agilizar las consultas para los filtros
create index on GATHERING_DATE(month);
create index on GATHERING_DATE(year);

CREATE TABLE specimen_datamart.SPECIMEN_FACT(
	specimen_id SERIAL PRIMARY KEY,
	taxon_id INTEGER,
	gathering_id INTEGER,
	gathering_responsible_ID INTEGER,
	site_id INTEGER,
	specimen_cost DOUBLE PRECISION,
    -- llaves a las 4 dimensiones que conectan la estrella
	FOREIGN KEY(taxon_id) REFERENCES specimen_datamart.TAXON(taxon_id),
	FOREIGN KEY(gathering_id) REFERENCES specimen_datamart.GATHERING_DATE(gathering_id),
	FOREIGN KEY(gathering_responsible_ID) REFERENCES specimen_datamart.GATHERING_RESPONSIBLE(gathering_responsible_ID),
	FOREIGN KEY(site_id) REFERENCES specimen_datamart.SITE(site_id)
);


-- ********* procedimientos del datamart **************

-- Procedure: insert_site_star_dimension
-- Description: Inserta la información del sitio desde la tabla inbio.SPECIMEN a la de la dimensión en el patrón estrella
-- Input Parameters: ninguno
-- Se encarga de recorrer Site con un for y una variable de registro, trae toda la información de la tabla.
CREATE OR REPLACE PROCEDURE insert_site_star_dimension()
LANGUAGE plpgsql AS
$$
DECLARE
    rec RECORD;
BEGIN
    FOR rec IN SELECT * FROM inbio.site LOOP
        INSERT INTO specimen_datamart.site (site_id, latitude, longitude, site_description)
        VALUES (rec.site_id, rec.latitude, rec.longitude, rec.site_description);
    END LOOP;
END;
$$;

-- Procedure: insert_gathresp_star_dimension
-- Description: Inserta la información del responsable desde la tabla inbio.GATHERING_RESPONSIBLE a la de la dimensión en el patrón estrella
-- Input Parameters: ninguno
-- Se encarga de recorrer la tabla con un for y una variable de registro, trae toda la información de la tabla.
CREATE OR REPLACE PROCEDURE insert_gathresp_star_dimension()
LANGUAGE plpgsql AS
$$
DECLARE
    rec RECORD;
BEGIN
    FOR rec IN SELECT gathering_responsible_id, _name FROM inbio.gathering_responsible LOOP
        INSERT INTO specimen_datamart.gathering_responsible (gathering_responsible_id, _name)
        VALUES (rec.gathering_responsible_id, rec._name);
    END LOOP;
END;
$$;

-- Procedure: insert_gathering_star_dimension
-- Description: Inserta la información de la recolección desde la tabla inbio.GATHERING a la de la dimensión en el patrón estrella
-- Input Parameters: ninguno
-- Se encarga de recorrer la tabla con un for y una variable de registro, trae toda la información de la tabla.
-- utiliza la función EXTRACT para desacoplar la fecha y convertirla en día, mes y año por aparte, algo de utilidad para la estrella.
CREATE OR REPLACE PROCEDURE insert_gathering_star_dimension()
LANGUAGE plpgsql AS
$$
DECLARE
    rec RECORD;
    day_to_insert INTEGER;
    month_to_insert INTEGER;
    year_to_insert INTEGER;
BEGIN
    FOR rec IN SELECT gathering_id, gathering_date FROM inbio.gathering LOOP
        day_to_insert := EXTRACT(DAY FROM rec.gathering_date);
        month_to_insert := EXTRACT(MONTH FROM rec.gathering_date);
        year_to_insert := EXTRACT(YEAR FROM rec.gathering_date);

        IF NOT EXISTS (
            SELECT 1 FROM specimen_datamart.gathering_date
            WHERE month = month_to_insert AND day = day_to_insert AND year = year_to_insert
        ) THEN
            INSERT INTO specimen_datamart.gathering_date (gathering_id, month, day, year)
            VALUES (rec.gathering_id, month_to_insert, day_to_insert, year_to_insert);
        END IF;
    END LOOP;
END;
$$;

-- Procedure: insert_taxon_star_dimension
-- Description: Inserta la información del taxon desde la tabla inbio.TAXON a la de la dimensión en el patrón estrella
-- Input Parameters: ninguno
-- Se encarga de recorrer TAXON con un for y una variable de registro, trae toda la información de la tabla.
CREATE OR REPLACE PROCEDURE insert_taxon_star_dimension()
LANGUAGE plpgsql AS
$$
DECLARE
    rec RECORD;
BEGIN
    FOR rec IN SELECT * FROM inbio.taxon LOOP
        INSERT INTO specimen_datamart.taxon (taxon_id, kingdom_name, phylum_division_name, class_name,
            order_name, family_name, genus_name, species_name, scientific_name)
        VALUES (rec.taxon_id, rec.kingdom_name, rec.phylum_division_name, rec.class_name,
            rec.order_name, rec.family_name, rec.genus_name, rec.species_name, rec.scientific_name
        );
    END LOOP;
END;
$$;

-- Procedure: load_fact_group()
-- Description: Es el más complejo de los de este esquema. Se encarga de llenar los registros de la tabla de hechos a partir
-- de la información obtenida del modelo relacional.
-- Input Parameters: ninguno
-- Se encarga de recorrer una tabla generada por un SELECT a partir de una unión (join) y obtener los registros necesarios
-- para llenar la tabla de hechos.
CREATE OR REPLACE PROCEDURE load_fact_group()
LANGUAGE plpgsql AS
$$
DECLARE
    rec RECORD;
    ind INTEGER;
    day_to_insert INTEGER;
    month_to_insert INTEGER;
    year_to_insert INTEGER;
BEGIN
    FOR rec IN
        SELECT
            specimen_id, specimen_description, specimen_cost, inbio.specimen.taxon_id,
            inbio.gathering.gathering_date, inbio.gathering.site_id, inbio.gathering.gathering_responsible_id
        FROM inbio.specimen
        JOIN inbio.gathering ON inbio.specimen.gathering_id = inbio.gathering.gathering_id
    LOOP
        day_to_insert := EXTRACT(DAY FROM rec.gathering_date);
        month_to_insert := EXTRACT(MONTH FROM rec.gathering_date);
        year_to_insert := EXTRACT(YEAR FROM rec.gathering_date);

        SELECT gathering_id INTO ind
        FROM specimen_datamart.gathering_date
        WHERE month = month_to_insert AND day = day_to_insert AND year = year_to_insert;

        INSERT INTO specimen_datamart.specimen_fact (
            specimen_id, taxon_id, gathering_id,
            gathering_responsible_id, site_id, specimen_cost
        ) VALUES (rec.specimen_id,
                  rec.taxon_id,
                  ind,
                  rec.gathering_responsible_id,
                  rec.site_id,
                  rec.specimen_cost
        );
    END LOOP;
END;
$$;

-- llamada a los procedimientos necesarios para llenar las dimensiones y la tabla de hechos
BEGIN;
    call insert_site_star_dimension();
    call insert_gathresp_star_dimension();
    call insert_gathering_star_dimension();
    call insert_taxon_star_dimension();
    call fill_fact_group();
END;

select count(*) from SPECIMEN_FACT;

SELECT MIN(EXTRACT(YEAR FROM initial_date_time)) AS smallest_year
FROM inbio.inbio_data;

SELECT initial_date_time, EXTRACT(YEAR FROM initial_date_time) AS year
FROM inbio.inbio_data;
-- para encontrar cuántos había con el año 6 que me pareció super raro jaja
SELECT COUNT(*)
FROM inbio.inbio_data
WHERE EXTRACT(YEAR FROM initial_date_time) = 6;


-- esto es para exportar la base a LucidChart y diagramar
--SET enable_nestloop=0;SELECT 'postgresql' AS dbms,t.table_catalog,t.table_schema,t.table_name,c.column_name,c.ordinal_position,c.data_type,c.character_maximum_length,n.constraint_type,k2.table_schema,k2.table_name,k2.column_name FROM information_schema.tables t NATURAL LEFT JOIN information_schema.columns c LEFT JOIN(information_schema.key_column_usage k NATURAL JOIN information_schema.table_constraints n NATURAL LEFT JOIN information_schema.referential_constraints r)ON c.table_catalog=k.table_catalog AND c.table_schema=k.table_schema AND c.table_name=k.table_name AND c.column_name=k.column_name LEFT JOIN information_schema.key_column_usage k2 ON k.position_in_unique_constraint=k2.ordinal_position AND r.unique_constraint_catalog=k2.constraint_catalog AND r.unique_constraint_schema=k2.constraint_schema AND r.unique_constraint_name=k2.constraint_name WHERE t.TABLE_TYPE='BASE TABLE' AND t.table_schema NOT IN('information_schema','pg_catalog');