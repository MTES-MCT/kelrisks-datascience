-- Ce script permet d'ajouter une colonne
-- geog à partir des coordonnées
-- x et y

DROP TABLE IF EXISTS etl.s3ic_idf_with_geom;

CREATE TABLE etl.s3ic_idf_with_geom (
  LIKE etl.s3ic_idf_source INCLUDING indexes,
  geog GEOMETRY(GEOMETRY, 4326)
);

INSERT INTO etl.s3ic_idf_with_geom
SELECT
  *,
  ST_Transform(
    ST_SetSrid(
      ST_MakePoint(x,y),
      2154
    ),
  4326)
FROM etl.s3ic_idf_source;