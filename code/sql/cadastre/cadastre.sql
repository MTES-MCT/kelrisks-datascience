-- Schéma de la table cadastre principale
-- et des différentes tables temporaires

DROP TABLE IF EXISTS {{ params.table_name }};

CREATE TABLE {{ params.table_name }} (
  id BIGSERIAL PRIMARY KEY,
  version INTEGER,
  code VARCHAR(255),
  commune VARCHAR(255),
  prefixe VARCHAR(255),
  section VARCHAR(255),
  numero VARCHAR(255),
  type VARCHAR(255),
  type_geom VARCHAR(255),
  geog GEOMETRY(GEOMETRY, 4326)
);