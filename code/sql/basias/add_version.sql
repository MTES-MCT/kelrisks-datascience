-- Ce script permet d'ajouter un numéro
-- de version à la table basias pour des raisons
-- de compatibilité avec le framework Spring
-- La valeur de la colonne version est incrémenté
-- à 1 par défaut

DROP TABLE IF EXISTS etl.basias_sites_with_version;

CREATE TABLE etl.basias_sites_with_version (
  LIKE etl.basias_sites_with_commune INCLUDING indexes,
  version INTEGER
);

INSERT INTO etl.basias_sites_with_version
SELECT *, 1 FROM etl.basias_sites_with_commune;