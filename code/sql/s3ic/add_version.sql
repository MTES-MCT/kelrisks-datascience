-- Ce script permet d'ajouter un numéro
-- de version à la table s3ic pour des raisons
-- de compatibilité avec le framework Spring
-- La valeur de la colonne version est incrémenté
-- à 0 par défaut

DROP TABLE IF EXISTS etl.s3ic_with_version;

CREATE TABLE etl.s3ic_with_version (
  LIKE etl.s3ic_with_commune,
  version INTEGER
);

INSERT INTO etl.s3ic_with_version
SELECT *, 1 FROM etl.s3ic_with_commune;