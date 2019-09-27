-- Permet de copier les données des tables
-- temporaires de département vers la table
-- cadastre principale

INSERT INTO {{ params.destination }} (
  version,
  code,
  commune,
  prefixe,
  section,
  numero,
  type,
  type_geom,
  geog
)
SELECT
  version,
  code,
  commune,
  prefixe,
  section,
  numero,
  type,
  type_geom,
  geog
FROM {{ params.source }}