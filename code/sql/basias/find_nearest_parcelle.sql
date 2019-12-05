

DROP TABLE IF EXISTS etl.basias_localisation_intersected;

CREATE TABLE etl.basias_localisation_intersected (
  LIKE etl.basias_localisation_geog_merged INCLUDING indexes
);

INSERT INTO etl.basias_localisation_intersected (
  SELECT * FROM etl.basias_localisation_geog_merged
);

WITH new_geog as (
  SELECT geog FROM etl.basias_localisation_geog_merged
)

WITH new_geog as (
  SELECT geog
    FROM kelrisks.cadastre as c
    WHERE st_dwithin(basias.geog, c.geog, 0.0001)
    ORDER BY st_distance(basias.geog, c.geog)
    LIMIT 1) nearest
  FROM etl.basias_localisation_geog_merged basias
)

UPDATE etl.basias_localisation_intersected
  SET geog = new_geog.geog
FROM new_geog;
