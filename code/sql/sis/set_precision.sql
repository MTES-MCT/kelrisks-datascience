-- Ce script est utilisé pour ajouter les champs geog_precision
-- et geog_source aux données SIS. Par défaut tous les
-- enregistrements ont une précision de type "parcel" et
-- l'origine de l'information provient de la colonne "geog"
-- des donnés sources

DROP TABLE IF EXISTS etl.sis_with_precision;

CREATE TABLE etl.sis_with_precision (
  LIKE etl.sis_geocoded INCLUDING indexes,
  geog_precision VARCHAR(255),
  geog_source VARCHAR(255)
);

INSERT INTO etl.sis_with_precision
  SELECT
    *,
    'parcel',
    'geog'
  FROM etl.sis_geocoded

