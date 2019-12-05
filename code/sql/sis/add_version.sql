-- Ce script permet d'ajouter un numéro
-- de version à la table s3ic pour des raisons
-- de compatibilité avec le framework Spring
-- La valeur de la colonne version est incrémenté
-- à 1 par défaut

DROP TABLE IF EXISTS etl.sis_with_version;

CREATE TABLE etl.sis_with_version (
  LIKE etl.sis_with_precision INCLUDING indexes,
  version INTEGER
);

INSERT INTO etl.sis_with_version
SELECT *, 1 FROM etl.sis_with_precision;