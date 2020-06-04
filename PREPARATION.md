To create a suitable database for running ```aggregate.py```, various preparatory steps are required. This file briefly lists the steps that I did:

You need a Linux box with PostgreSQL & PostGIS installed. The name of the DB is expected to be ```tt```.

1. Import the 250m grid of the whole country by Statistics Finland to the DB: ```shp2pgsql -s 3067 T11_grd_e_250m/T11_grd_e_250m.shp |psql -q tt```
2. Import the MetropAccess (= TTM-supplied) grid: ```shp2pgsql -s 3067 MetropAccess_YKR_grid/MetropAccess_YKR_grid_EurefFIN.shp |psql -q tt```
3. Download the subregions data over WFS as a shapefile:
        ```sh
        wget https://kartta.hel.fi/ws/geoserver/avoindata/wfs?version=2.0.0&request=GetFeature&typeName=avoindata:Seutukartta_aluejako_tilastoalue&outputformat=SHAPE-ZIP&srsName=EPSG:3067&format_options=charset:utf-8
        ```
4. Import the subregions of the extended HCR: ```shp2pgsql -s 3067 Seutukartta_aluejako_tilastoalue/Seutukartta_aluejako_tilastoaluePolygon.shp hcr_subregions |psql tt```
5. Create the following table structures:
    1. A table for all of the commuting data. For this, I created the table named ```T06_tma_e_TOL2008``` to have a name as close to the .MDB files as possible; this is useful! The table itself can be created as follows:
        ```sql
        CREATE TABLE "T06_tma_e_TOL2008"(
          id integer NOT NULL,
          "aTK_id" double precision,
          ax double precision,
          ay double precision,
          akunta text,
          "tTK_id" double precision,
          tx double precision,
          ty double precision,
          tkunta text,
          vuosi text,
          matka double precision,
          sp text,
          yht integer,
          a_alkut integer,
          b_kaivos integer,
          c_teoll integer,
          d_infra1 integer,
          e_infra2 integer,
          f_rakent integer,
          g_kauppa integer,
          h_kulj integer,
          i_majrav integer,
          j_info integer,
          k_raha integer,
          l_kiint integer,
          m_tekn integer,
          n_halpa integer,
          o_julk integer,
          p_koul integer,
          q_terv integer,
          r_taide integer,
          s_muupa integer,
          t_koti integer,
          u_kvjarj integer,
          x_tuntem integer,
          txyind text,
          axyind text
        );
        ```
    2. A table for each of the TTMs:
        ```sql
        CREATE TABLE ttm2013(
          from_id integer,
          to_id integer,
          walk_t integer,
          walk_d integer,
          pt_m_tt integer,
          pt_m_t integer,
          pt_m_d integer,
          car_m_t integer,
          car_m_d integer
        );
        CREATE TABLE ttm2015(
          from_id integer,
          to_id integer,
          walk_t integer,
          walk_d integer,
          pt_r_tt integer,
          pt_r_t integer,
          pt_r_d integer,
          pt_m_tt integer,
          pt_m_t integer,
          pt_m_d integer,
          car_r_t integer,
          car_r_d integer,
          car_m_t integer,
          car_m_d integer
        );
        CREATE TABLE ttm2018(
          from_id integer,
          to_id integer,
          walk_t integer,
          walk_d integer,
          bike_s_t integer,
          bike_f_t integer,
          bike_d integer,
          pt_r_tt integer,
          pt_r_t integer,
          pt_r_d integer,
          pt_m_tt integer,
          pt_m_t integer,
          pt_m_d integer,
          car_r_t integer,
          car_r_d integer,
          car_m_t integer,
          car_m_d integer,
          car_sl_t integer
        );
        ```
6. The files that should be imported might have characters in their filenames that are difficult to handle. Tips for fixing them:
    1. MDB file names:
        * ```find YKR/ -name '*Д*' -exec rename.ul -v 'Д' 'ä' {} \;```
        * ```find YKR/ -name '*Ф*' -exec rename.ul -v 'Ф' 'ö' {} \;```
    2. TTM file names: ```find . -type f -name 'travel_times_to_*.txt' -exec rename.ul ' ' '_' {} \;``` (use ```rename.ul```, sometimes shipped with the ```util-linux``` package).
7. Import the MDBs (prepare to wait for a day or so…):
    ```sh
    for i in `find . -name '*2008.mdb' |sort |xargs`; do echo $i; mdb-export -Q -I postgres $i `echo -n $i |sed 's/^\.\/[0-9]\{4\}\/\(.*\)\.mdb/\1/'` |sed 's/\T06_tma_e_[0-9]\{2\}_TOL2008/T06_tma_e_TOL2008/' |psql -q tt; done
    ```
8. Import each of the TTMs:
    ```sh
    for i in `find ./ -name 'travel_times_to*.txt'`; do echo $i; cat $i |psql tt -q -c "COPY ttm2013 FROM STDIN (FORMAT csv, DELIMITER ';', NULL '-1', HEADER)"; done
    for i in `find ./ -name 'travel_times_to*.txt'`; do echo $i; cat $i |psql tt -q -c "COPY ttm2015 FROM STDIN (FORMAT csv, DELIMITER ';', NULL '-1', HEADER)"; done
    for i in `find ./ -name 'travel_times_to*.txt'`; do echo $i; cat $i |psql tt -q -c "COPY ttm2018 FROM STDIN (FORMAT csv, DELIMITER ';', NULL '-1', HEADER)"; done
    ```
9. Run several queries to prepare the database for ```aggregate.py```:
    1. Create the grid:
        ```sql
        -- Tidy up the TTM supplied grid:
        ALTER TABLE metropaccess_ykr_grid_eureffin RENAME TO ttm_supplied_grid;
        ALTER SEQUENCE metropaccess_ykr_grid_eureffin_gid_seq RENAME TO ttm_supplied_grid;
        ALTER INDEX metropaccess_ykr_grid_eureffin_pkey RENAME TO ttm_supplied_grid;
        -- Tidy up the all-country grid:
        ALTER TABLE t11_grd_e_250m RENAME TO fin_msssuf_grid;
        ALTER SEQUENCE t11_grd_e_250m_gid_seq RENAME TO fin_msssuf_grid_gid_seq;
        ALTER INDEX t11_grd_e_250m_pkey RENAME TO fin_msssuf_grid_pkey;
        -- Create the actual research area grid:
        –– Use an anti-join on DELETE, or this is very slow!
        CREATE TABLE hcr_msssuf_grid AS TABLE fin_msssuf_grid;
        ALTER TABLE hcr_msssuf_grid ADD PRIMARY KEY (gid);
        CREATE INDEX ttm_supplied_grid_ykr_id_idx ON ttm_supplied_grid(ykr_id);
        DELETE FROM hcr_msssuf_grid h WHERE NOT EXISTS (SELECT NULL FROM ttm_supplied_grid t WHERE h.gid=t.ykr_id);
        ```
    3. Tune up the all-journey table and create geometries:
        ```sql
        ALTER TABLE "T06_tma_e_TOL2008" RENAME TO msssuf_journeys_ic2008;
        ALTER TABLE msssuf_journeys_ic2008 ADD PRIMARY KEY(id, vuosi);
        ALTER TABLE msssuf_journeys_ic2008 RENAME COLUMN "aTK_id" TO "atk_id";
        ALTER TABLE msssuf_journeys_ic2008 RENAME COLUMN "tTK_id" TO "ttk_id";
        ALTER TABLE msssuf_journeys_ic2008 ALTER COLUMN sp TYPE integer USING sp::integer;
        ALTER TABLE msssuf_journeys_ic2008 ALTER COLUMN vuosi TYPE integer USING vuosi::integer;
        ALTER TABLE msssuf_journeys_ic2008 ALTER COLUMN akunta TYPE integer USING akunta::integer;
        ALTER TABLE msssuf_journeys_ic2008 ALTER COLUMN tkunta TYPE integer USING tkunta::integer;
        AddGeometryColumn('msssuf_journeys_ic2008','a_geom',3067,'POINT',2);
        AddGeometryColumn('msssuf_journeys_ic2008','t_geom',3067,'POINT',2);
        UPDATE msssuf_journeys_ic2008 SET a_geom=(ST_SetSRID(ST_MakePoint(ax,ay),3067));
        UPDATE msssuf_journeys_ic2008 SET t_geom=(ST_SetSRID(ST_MakePoint(tx,ty),3067));
        AddGeometryColumn('msssuf_journeys_ic2008','at_geom',3067,'LINESTRING',2);
        UPDATE msssuf_journeys_ic2008 SET at_geom=(ST_MakeLine(a_geom,t_geom));
        ```
    4. Clean up those rows from 2013 & 2015 TTMs that have no ```to_id```s (the 2018 TTM should not have these), and add primary keys for all TTMs. Note: it would be more efficient to drop these rows on TTM import (use ```grep```!)):
        ```sqlTo create a suitable database for running ```aggregate.py```, various preparatory steps are required. This file briefly lists the steps that I did:

You need a Linux box with PostgreSQL & PostGIS installed. The name of the DB is expected to be ```tt```.

1. Import the 250m grid of the whole country by Statistics Finland to the DB: ```shp2pgsql -s 3067 T11_grd_e_250m/T11_grd_e_250m.shp |psql -q tt```
2. Import the MetropAccess (= TTM-supplied) grid: ```shp2pgsql -s 3067 MetropAccess_YKR_grid/MetropAccess_YKR_grid_EurefFIN.shp |psql -q tt```
3. Download the subregions data over WFS as a shapefile:
        ```sh
        wget https://kartta.hel.fi/ws/geoserver/avoindata/wfs?version=2.0.0&request=GetFeature&typeName=avoindata:Seutukartta_aluejako_tilastoalue&outputformat=SHAPE-ZIP&srsName=EPSG:3067&format_options=charset:utf-8
        ```
4. Import the subregions of the extended HCR: ```shp2pgsql -s 3067 Seutukartta_aluejako_tilastoalue/Seutukartta_aluejako_tilastoaluePolygon.shp hcr_subregions |psql tt```
5. Create the following table structures:
    1. A table for all of the commuting data. For this, I created the table named ```T06_tma_e_TOL2008``` to have a name as close to the .MDB files as possible; this is useful! The table itself can be created as follows:
        ```sql
        CREATE TABLE "T06_tma_e_TOL2008"(
          id integer NOT NULL,
          "aTK_id" double precision,
          ax double precision,
          ay double precision,
          akunta text,
          "tTK_id" double precision,
          tx double precision,
          ty double precision,
          tkunta text,
          vuosi text,
          matka double precision,
          sp text,
          yht integer,
          a_alkut integer,
          b_kaivos integer,
          c_teoll integer,
          d_infra1 integer,
          e_infra2 integer,
          f_rakent integer,
          g_kauppa integer,
          h_kulj integer,
          i_majrav integer,
          j_info integer,
          k_raha integer,
          l_kiint integer,
          m_tekn integer,
          n_halpa integer,
          o_julk integer,
          p_koul integer,
          q_terv integer,
          r_taide integer,
          s_muupa integer,
          t_koti integer,
          u_kvjarj integer,
          x_tuntem integer,
          txyind text,
          axyind text
        );
        ```
    2. A table for each of the TTMs:
        ```sql
        CREATE TABLE ttm2013(
          from_id integer,
          to_id integer,
          walk_t integer,
          walk_d integer,
          pt_m_tt integer,
          pt_m_t integer,
          pt_m_d integer,
          car_m_t integer,
          car_m_d integer
        );
        CREATE TABLE ttm2015(
          from_id integer,
          to_id integer,
          walk_t integer,
          walk_d integer,
          pt_r_tt integer,
          pt_r_t integer,
          pt_r_d integer,
          pt_m_tt integer,
          pt_m_t integer,
          pt_m_d integer,
          car_r_t integer,
          car_r_d integer,
          car_m_t integer,
          car_m_d integer
        );
        CREATE TABLE ttm2018(
          from_id integer,
          to_id integer,
          walk_t integer,
          walk_d integer,
          bike_s_t integer,
          bike_f_t integer,
          bike_d integer,
          pt_r_tt integer,
          pt_r_t integer,
          pt_r_d integer,
          pt_m_tt integer,
          pt_m_t integer,
          pt_m_d integer,
          car_r_t integer,
          car_r_d integer,
          car_m_t integer,
          car_m_d integer,
          car_sl_t integer
        );
        ```
6. The files that should be imported might have characters in their filenames that are difficult to handle. Tips for fixing them:
    1. MDB file names:
        * ```find YKR/ -name '*Д*' -exec rename.ul -v 'Д' 'ä' {} \;```
        * ```find YKR/ -name '*Ф*' -exec rename.ul -v 'Ф' 'ö' {} \;```
    2. TTM file names: ```find . -type f -name 'travel_times_to_*.txt' -exec rename.ul ' ' '_' {} \;``` (use ```rename.ul```, sometimes shipped with the ```util-linux``` package).
7. Import the MDBs (prepare to wait for a day or so…):
    ```sh
    for i in `find . -name '*2008.mdb' |sort |xargs`; do echo $i; mdb-export -Q -I postgres $i `echo -n $i |sed 's/^\.\/[0-9]\{4\}\/\(.*\)\.mdb/\1/'` |sed 's/\T06_tma_e_[0-9]\{2\}_TOL2008/T06_tma_e_TOL2008/' |psql -q tt; done
    ```
8. Import each of the TTMs:
    ```sh
    for i in `find ./ -name 'travel_times_to*.txt'`; do echo $i; cat $i |psql tt -q -c "COPY ttm2013 FROM STDIN (FORMAT csv, DELIMITER ';', NULL '-1', HEADER)"; done
    for i in `find ./ -name 'travel_times_to*.txt'`; do echo $i; cat $i |psql tt -q -c "COPY ttm2015 FROM STDIN (FORMAT csv, DELIMITER ';', NULL '-1', HEADER)"; done
    for i in `find ./ -name 'travel_times_to*.txt'`; do echo $i; cat $i |psql tt -q -c "COPY ttm2018 FROM STDIN (FORMAT csv, DELIMITER ';', NULL '-1', HEADER)"; done
    ```
9. Run several queries to prepare the database for ```aggregate.py```:
    1. Create the grid:
        ```sql
        -- Tidy up the TTM supplied grid:
        ALTER TABLE metropaccess_ykr_grid_eureffin RENAME TO ttm_supplied_grid;
        ALTER SEQUENCE metropaccess_ykr_grid_eureffin_gid_seq RENAME TO ttm_supplied_grid;
        ALTER INDEX metropaccess_ykr_grid_eureffin_pkey RENAME TO ttm_supplied_grid;
        -- Tidy up the all-country grid:
        ALTER TABLE t11_grd_e_250m RENAME TO fin_msssuf_grid;
        ALTER SEQUENCE t11_grd_e_250m_gid_seq RENAME TO fin_msssuf_grid_gid_seq;
        ALTER INDEX t11_grd_e_250m_pkey RENAME TO fin_msssuf_grid_pkey;
        -- Create the actual research area grid:
        –– Use an anti-join on DELETE, or this is very slow!
        CREATE TABLE hcr_msssuf_grid AS TABLE fin_msssuf_grid;
        ALTER TABLE hcr_msssuf_grid ADD PRIMARY KEY (gid);
        CREATE INDEX ttm_supplied_grid_ykr_id_idx ON ttm_supplied_grid(ykr_id);
        DELETE FROM hcr_msssuf_grid h WHERE NOT EXISTS (SELECT NULL FROM ttm_supplied_grid t WHERE h.gid=t.ykr_id);
        ```
    3. Tune up the all-journey table and create geometries:
        ```sql
        ALTER TABLE "T06_tma_e_TOL2008" RENAME TO msssuf_journeys_ic2008;
        ALTER TABLE msssuf_journeys_ic2008 ADD PRIMARY KEY(id, vuosi);
        ALTER TABLE msssuf_journeys_ic2008 RENAME COLUMN "aTK_id" TO "atk_id";
        ALTER TABLE msssuf_journeys_ic2008 RENAME COLUMN "tTK_id" TO "ttk_id";
        ALTER TABLE msssuf_journeys_ic2008 ALTER COLUMN sp TYPE integer USING sp::integer;
        ALTER TABLE msssuf_journeys_ic2008 ALTER COLUMN vuosi TYPE integer USING vuosi::integer;
        ALTER TABLE msssuf_journeys_ic2008 ALTER COLUMN akunta TYPE integer USING akunta::integer;
        ALTER TABLE msssuf_journeys_ic2008 ALTER COLUMN tkunta TYPE integer USING tkunta::integer;
        AddGeometryColumn('msssuf_journeys_ic2008','a_geom',3067,'POINT',2);
        AddGeometryColumn('msssuf_journeys_ic2008','t_geom',3067,'POINT',2);
        UPDATE msssuf_journeys_ic2008 SET a_geom=(ST_SetSRID(ST_MakePoint(ax,ay),3067));
        UPDATE msssuf_journeys_ic2008 SET t_geom=(ST_SetSRID(ST_MakePoint(tx,ty),3067));
        AddGeometryColumn('msssuf_journeys_ic2008','at_geom',3067,'LINESTRING',2);
        UPDATE msssuf_journeys_ic2008 SET at_geom=(ST_MakeLine(a_geom,t_geom));
        ```
    4. Clean up those rows from 2013 & 2015 TTMs that have no ```to_id```s (the 2018 TTM should not have these), and add primary keys for all TTMs. Note: this is what I did, but it would likely be more efficient to simply ```grep -v``` these rows while importing the TTMs):
        ```sql
        DELETE FROM ttm2013 WHERE to_id IS NULL;
        DELETE FROM ttm2015 WHERE to_id IS NULL;
        ALTER TABLE ttm2013 ADD PRIMARY KEY (from_id, to_id);
        ALTER TABLE ttm2015 ADD PRIMARY KEY (from_id, to_id);
        ALTER TABLE ttm2018 ADD PRIMARY KEY (from_id, to_id);
        ```
    5. Create a HCR-only journey table:
        ```sql
        CREATE TABLE hcr_msssuf_journeys AS TABLE msssuf_journeys_ic2008 WITH NO DATA;
        ALTER TABLE hcr_msssuf_journeys ADD PRIMARY KEY (id, vuosi);
        CREATE INDEX hcr_msssuf_journeys_axyind_vuosi_sp0_idx ON hcr_msssuf_journeys (axyind,vuosi) WHERE sp=0;
        CREATE INDEX hcr_msssuf_journeys_txyind_vuosi_sp0_idx ON hcr_msssuf_journeys (txyind,vuosi) WHERE sp=0;
        -- Note: this inserts all journeys that either start from
        -- or end up to the HCR. However, this is a larger subset
        -- than what is strictly required for analysis.
        INSERT INTO hcr_msssuf_journeys SELECT f.* FROM msssuf_journeys_ic2008 f INNER JOIN hcr_msssuf_grid g ON f.axyind=g.xyind;
        INSERT INTO hcr_msssuf_journeys SELECT f.* FROM msssuf_journeys_ic2008 f INNER JOIN hcr_msssuf_grid g ON f.txyind=g.xyind ON CONFLICT DO NOTHING;
        -- Add subregion fields to the table. Note: arguably ST_Within could
        -- be used instead;
        ALTER TABLE hcr_msssuf_journeys ADD COLUMN a_region text;
        ALTER TABLE hcr_msssuf_journeys ADD COLUMN t_region text;
        -- Create an index to hcr_subregions to speed up queries.
        CREATE INDEX hcr_subregions_kokotun_idx ON hcr_subregions (kokotun);
        UPDATE hcr_msssuf_journeys SET a_region = r.kokotun FROM hcr_subregions r WHERE ST_Contains(r.geom, hcr_msssuf_journeys.a_geom) AND r.kunta IN ('049','091','092','235');
        UPDATE hcr_msssuf_journeys SET t_region = r.kokotun FROM hcr_subregions r WHERE ST_Contains(r.geom, hcr_msssuf_journeys.t_geom) AND r.kunta IN ('049','091','092','235');
        CREATE INDEX hcr_msssuf_journeys_a_region_idx ON hcr_msssuf_journeys (a_region);
        CREATE INDEX hcr_msssuf_journeys_a_region_idx ON hcr_msssuf_journeys (a_region);
        --
        ```
    6. Turn missing values of the all-journey table to real NULLs (note: this is slow!):
        ```sql
        UPDATE hcr_msssuf_journeys SET b_kaivos = NULL WHERE b_kaivos = -1
        UPDATE hcr_msssuf_journeys SET c_teoll = NULL WHERE c_teoll = -1
        UPDATE hcr_msssuf_journeys SET d_infra1 = NULL WHERE d_infra1 = -1
        UPDATE hcr_msssuf_journeys SET e_infra2 = NULL WHERE e_infra2 = -1
        UPDATE hcr_msssuf_journeys SET f_rakent = NULL WHERE f_rakent = -1
        UPDATE hcr_msssuf_journeys SET g_kauppa = NULL WHERE g_kauppa = -1
        UPDATE hcr_msssuf_journeys SET h_kulj = NULL WHERE h_kulj = -1
        UPDATE hcr_msssuf_journeys SET i_majrav = NULL WHERE i_majrav = -1
        UPDATE hcr_msssuf_journeys SET j_info = NULL WHERE j_info = -1
        UPDATE hcr_msssuf_journeys SET k_raha = NULL WHERE k_raha = -1
        UPDATE hcr_msssuf_journeys SET l_kiint = NULL WHERE l_kiint = -1
        UPDATE hcr_msssuf_journeys SET m_tekn = NULL WHERE m_tekn = -1
        UPDATE hcr_msssuf_journeys SET n_halpa = NULL WHERE n_halpa = -1
        UPDATE hcr_msssuf_journeys SET o_julk = NULL WHERE o_julk = -1
        UPDATE hcr_msssuf_journeys SET p_koul = NULL WHERE p_koul = -1
        UPDATE hcr_msssuf_journeys SET q_terv = NULL WHERE q_terv = -1
        UPDATE hcr_msssuf_journeys SET r_taide = NULL WHERE r_taide = -1
        UPDATE hcr_msssuf_journeys SET s_muupa = NULL WHERE s_muupa = -1
        UPDATE hcr_msssuf_journeys SET t_koti = NULL WHERE t_koti = -1
        UPDATE hcr_msssuf_journeys SET u_kvjarj = NULL WHERE u_kvjarj = -1
        UPDATE hcr_msssuf_journeys SET x_tuntem = NULL WHERE x_tuntem = -1
        ```
    7. Create the journey tables for aggregate.py:
        ```sql
        CREATE TABLE hcr_journeys_t_d_2013 AS (
          SELECT j.id, j.vuosi, j,sp, m.*
          FROM hcr_msssuf_journeys j
          INNER JOIN hcr_msssuf_grid gf ON j.axyind=gf.xyind
          INNER JOIN hcr_msssuf_grid gt ON j.txyind=gt.xyind
          INNER JOIN ttm2013 m ON gf.gid=m.from_id AND gt.gid=m.to_id
        );
        ALTER TABLE hcr_journeys_t_d_2013 DROP COLUMN j;
        ALTER TABLE hcr_journeys_t_d_2013 ADD PRIMARY KEY(id, vuosi);
        CREATE TABLE hcr_journeys_t_d_2015 AS (
          SELECT j.id, j.vuosi, j,sp, m.*
          FROM hcr_msssuf_journeys j
          INNER JOIN hcr_msssuf_grid gf ON j.axyind=gf.xyind
          INNER JOIN hcr_msssuf_grid gt ON j.txyind=gt.xyind
          INNER JOIN ttm2015 m ON gf.gid=m.from_id AND gt.gid=m.to_id
        );
        ALTER TABLE hcr_journeys_t_d_2015 DROP COLUMN j;
        ALTER TABLE hcr_journeys_t_d_2015 ADD PRIMARY KEY(id, vuosi);
        CREATE TABLE hcr_journeys_t_d_2018 AS (
          SELECT j.id, j.vuosi, j,sp, m.*
          FROM hcr_msssuf_journeys j
          INNER JOIN hcr_msssuf_grid gf ON j.axyind=gf.xyind
          INNER JOIN hcr_msssuf_grid gt ON j.txyind=gt.xyind
          INNER JOIN ttm2018 m ON gf.gid=m.from_id AND gt.gid=m.to_id
        );
        ALTER TABLE hcr_journeys_t_d_2018 DROP COLUMN j;
        ALTER TABLE hcr_journeys_t_d_2018 ADD PRIMARY KEY(id, vuosi);
        ```

        DELETE FROM ttm2013 WHERE to_id IS NULL;
        DELETE FROM ttm2015 WHERE to_id IS NULL;
        ALTER TABLE ttm2013 ADD PRIMARY KEY (from_id, to_id);
        ALTER TABLE ttm2015 ADD PRIMARY KEY (from_id, to_id);
        ALTER TABLE ttm2018 ADD PRIMARY KEY (from_id, to_id);
        ```
    5. Create a HCR-only journey table:
        ```sql
        CREATE TABLE hcr_msssuf_journeys AS TABLE msssuf_journeys_ic2008 WITH NO DATA;
        ALTER TABLE hcr_msssuf_journeys ADD PRIMARY KEY (id, vuosi);
        CREATE INDEX hcr_msssuf_journeys_axyind_vuosi_sp0_idx ON hcr_msssuf_journeys (axyind,vuosi) WHERE sp=0;
        CREATE INDEX hcr_msssuf_journeys_txyind_vuosi_sp0_idx ON hcr_msssuf_journeys (txyind,vuosi) WHERE sp=0;
        -- Note: this inserts all journeys that either start from
        -- or end up to the HCR. However, this is a larger subset
        -- than what is strictly required for analysis.
        INSERT INTO hcr_msssuf_journeys SELECT f.* FROM msssuf_journeys_ic2008 f INNER JOIN hcr_msssuf_grid g ON f.axyind=g.xyind;
        INSERT INTO hcr_msssuf_journeys SELECT f.* FROM msssuf_journeys_ic2008 f INNER JOIN hcr_msssuf_grid g ON f.txyind=g.xyind ON CONFLICT DO NOTHING;
        -- Add subregion fields to the table. Note: arguably ST_Within could
        -- be used instead;
        ALTER TABLE hcr_msssuf_journeys ADD COLUMN a_region text;
        ALTER TABLE hcr_msssuf_journeys ADD COLUMN t_region text;
        -- Create an index to hcr_subregions to speed up queries.
        CREATE INDEX hcr_subregions_kokotun_idx ON hcr_subregions (kokotun);
        UPDATE hcr_msssuf_journeys SET a_region = r.kokotun FROM hcr_subregions r WHERE ST_Contains(r.geom, hcr_msssuf_journeys.a_geom) AND r.kunta IN ('049','091','092','235');
        UPDATE hcr_msssuf_journeys SET t_region = r.kokotun FROM hcr_subregions r WHERE ST_Contains(r.geom, hcr_msssuf_journeys.t_geom) AND r.kunta IN ('049','091','092','235');
        CREATE INDEX hcr_msssuf_journeys_a_region_idx ON hcr_msssuf_journeys (a_region);
        CREATE INDEX hcr_msssuf_journeys_a_region_idx ON hcr_msssuf_journeys (a_region);
        --
        ```
    6. Turn missing values of the all-journey table to real NULLs (note: this is slow!):
        ```sql
        UPDATE hcr_msssuf_journeys SET b_kaivos = NULL WHERE b_kaivos = -1
        UPDATE hcr_msssuf_journeys SET c_teoll = NULL WHERE c_teoll = -1
        UPDATE hcr_msssuf_journeys SET d_infra1 = NULL WHERE d_infra1 = -1
        UPDATE hcr_msssuf_journeys SET e_infra2 = NULL WHERE e_infra2 = -1
        UPDATE hcr_msssuf_journeys SET f_rakent = NULL WHERE f_rakent = -1
        UPDATE hcr_msssuf_journeys SET g_kauppa = NULL WHERE g_kauppa = -1
        UPDATE hcr_msssuf_journeys SET h_kulj = NULL WHERE h_kulj = -1
        UPDATE hcr_msssuf_journeys SET i_majrav = NULL WHERE i_majrav = -1
        UPDATE hcr_msssuf_journeys SET j_info = NULL WHERE j_info = -1
        UPDATE hcr_msssuf_journeys SET k_raha = NULL WHERE k_raha = -1
        UPDATE hcr_msssuf_journeys SET l_kiint = NULL WHERE l_kiint = -1
        UPDATE hcr_msssuf_journeys SET m_tekn = NULL WHERE m_tekn = -1
        UPDATE hcr_msssuf_journeys SET n_halpa = NULL WHERE n_halpa = -1
        UPDATE hcr_msssuf_journeys SET o_julk = NULL WHERE o_julk = -1
        UPDATE hcr_msssuf_journeys SET p_koul = NULL WHERE p_koul = -1
        UPDATE hcr_msssuf_journeys SET q_terv = NULL WHERE q_terv = -1
        UPDATE hcr_msssuf_journeys SET r_taide = NULL WHERE r_taide = -1
        UPDATE hcr_msssuf_journeys SET s_muupa = NULL WHERE s_muupa = -1
        UPDATE hcr_msssuf_journeys SET t_koti = NULL WHERE t_koti = -1
        UPDATE hcr_msssuf_journeys SET u_kvjarj = NULL WHERE u_kvjarj = -1
        UPDATE hcr_msssuf_journeys SET x_tuntem = NULL WHERE x_tuntem = -1
        ```
    7. Create the journey tables for aggregate.py:
        ```sql
        CREATE TABLE hcr_journeys_t_d_2013 AS (
          SELECT j.id, j.vuosi, j,sp, m.*
          FROM hcr_msssuf_journeys j
          INNER JOIN hcr_msssuf_grid gf ON j.axyind=gf.xyind
          INNER JOIN hcr_msssuf_grid gt ON j.txyind=gt.xyind
          INNER JOIN ttm2013 m ON gf.gid=m.from_id AND gt.gid=m.to_id
        );
        ALTER TABLE hcr_journeys_t_d_2013 DROP COLUMN j;
        ALTER TABLE hcr_journeys_t_d_2013 ADD PRIMARY KEY(id, vuosi);
        CREATE TABLE hcr_journeys_t_d_2015 AS (
          SELECT j.id, j.vuosi, j,sp, m.*
          FROM hcr_msssuf_journeys j
          INNER JOIN hcr_msssuf_grid gf ON j.axyind=gf.xyind
          INNER JOIN hcr_msssuf_grid gt ON j.txyind=gt.xyind
          INNER JOIN ttm2015 m ON gf.gid=m.from_id AND gt.gid=m.to_id
        );
        ALTER TABLE hcr_journeys_t_d_2015 DROP COLUMN j;
        ALTER TABLE hcr_journeys_t_d_2015 ADD PRIMARY KEY(id, vuosi);
        CREATE TABLE hcr_journeys_t_d_2018 AS (
          SELECT j.id, j.vuosi, j,sp, m.*
          FROM hcr_msssuf_journeys j
          INNER JOIN hcr_msssuf_grid gf ON j.axyind=gf.xyind
          INNER JOIN hcr_msssuf_grid gt ON j.txyind=gt.xyind
          INNER JOIN ttm2018 m ON gf.gid=m.from_id AND gt.gid=m.to_id
        );
        ALTER TABLE hcr_journeys_t_d_2018 DROP COLUMN j;
        ALTER TABLE hcr_journeys_t_d_2018 ADD PRIMARY KEY(id, vuosi);
        ```
